// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type unsupportedWorkStore struct {
	Store
	delivery *Delivery
	turn     *TurnLease
	err      error
	leases   atomic.Int32
	writes   atomic.Int32
	snapshot *SessionSnapshot
	reads    atomic.Int32
}

func (s *unsupportedWorkStore) LeaseDelivery(context.Context, string, time.Duration) (*Delivery, error) {
	s.leases.Add(1)
	return s.delivery, s.err
}
func (s *unsupportedWorkStore) LeaseTurn(context.Context, ClaimOptions) (*TurnLease, error) {
	s.leases.Add(1)
	return s.turn, s.err
}
func (s *unsupportedWorkStore) RenewDelivery(_ context.Context, lease Lease, _ time.Duration) (Lease, error) {
	s.writes.Add(1)
	return lease, nil
}
func (s *unsupportedWorkStore) RenewTurn(_ context.Context, lease Lease, _ time.Duration) (Lease, error) {
	s.writes.Add(1)
	return lease, nil
}
func (s *unsupportedWorkStore) CompleteDelivery(context.Context, Lease, DeliveryResult) error {
	s.writes.Add(1)
	return nil
}
func (s *unsupportedWorkStore) SettleTurn(context.Context, Lease, TurnOutcome, []Delivery) error {
	s.writes.Add(1)
	return nil
}

func (s *unsupportedWorkStore) WaitTurn(context.Context, Lease, TurnWait) error {
	s.writes.Add(1)
	return nil
}
func (s *unsupportedWorkStore) GetSessionSnapshot(context.Context, string) (*SessionSnapshot, error) {
	return s.snapshot, s.err
}

func (s *unsupportedWorkStore) GetRemoteOperationContext(context.Context, string) (*TurnLease, error) {
	s.reads.Add(1)
	return s.turn, s.err
}

func (*unsupportedWorkStore) ListBindingVersions(context.Context) ([]BindingVersion, error) {
	return nil, nil
}
func (s *unsupportedWorkStore) ListTurns(context.Context, TurnQuery) ([]Turn, error) {
	if s.turn != nil {
		return []Turn{s.turn.Turn}, nil
	}
	return nil, nil
}
func (s *unsupportedWorkStore) ListDeliveries(context.Context, DeliveryQuery) ([]Delivery, error) {
	if s.delivery != nil {
		return []Delivery{*s.delivery}, nil
	}
	return nil, nil
}

type unsupportedWorkProvider struct{ calls atomic.Int32 }

func (*unsupportedWorkProvider) Start(context.Context, func(context.Context, Message) error) error {
	return nil
}
func (p *unsupportedWorkProvider) Deliver(context.Context, Delivery) (string, error) {
	p.calls.Add(1)
	return "remote", nil
}

type unsupportedWorkObserver struct{ blocked atomic.Int32 }

func (o *unsupportedWorkObserver) Observe(_ context.Context, item Observation) {
	if item.Status == "blocked" && item.ErrorKind == "unsupported_work" {
		o.blocked.Add(1)
	}
}

type unsupportedWorkMA struct{ MAClient }

func unsupportedRuntime(t *testing.T, store *unsupportedWorkStore) (*Runtime, *unsupportedWorkProvider, *unsupportedWorkObserver) {
	t.Helper()
	options, err := (RuntimeOptions{PollInterval: 10 * time.Millisecond}).Normalize()
	require.NoError(t, err)
	provider, observer := &unsupportedWorkProvider{}, &unsupportedWorkObserver{}
	options.Observer = observer
	return &Runtime{store: store, options: options, connections: map[string]Connection{"binding": {Binding: Binding{ID: "binding"}, Provider: provider, MAClient: &unsupportedWorkMA{}}}}, provider, observer
}

func TestRuntimeUnknownBodyCannotReachDefaultProvider(t *testing.T) {
	for _, old := range []bool{false, true} {
		t.Run(map[bool]string{false: "within budget", true: "expired budget"}[old], func(t *testing.T) {
			store := &unsupportedWorkStore{}
			runtime, provider, _ := unsupportedRuntime(t, store)
			delivery := Delivery{ID: "delivery", BindingID: "binding", Body: DeliveryBody{Type: "future"}, FirstAttemptAt: time.Now(), Attempt: 1}
			if old {
				delivery.FirstAttemptAt = time.Now().Add(-2 * time.Hour)
				delivery.Attempt = 100
			}
			err := runtime.processDelivery(context.Background(), delivery)
			require.Zero(t, provider.calls.Load())
			require.Zero(t, store.writes.Load(), "不能把未知记录标为failed/done")
			require.ErrorIs(t, err, ErrUnsupportedWork)
			require.ErrorIs(t, err, ErrUnknownBody)
		})
	}
}

func TestRuntimeUnsupportedWorkersBackOffWithoutWritingOrExiting(t *testing.T) {
	for _, kind := range []string{"delivery result", "delivery sentinel", "turn result", "turn sentinel"} {
		t.Run(kind, func(t *testing.T) {
			store := &unsupportedWorkStore{}
			runtime, provider, observer := unsupportedRuntime(t, store)
			switch kind {
			case "delivery result":
				store.delivery = &Delivery{ID: "d", BindingID: "binding", Body: DeliveryBody{Type: "future"}, FirstAttemptAt: time.Now()}
			case "turn result":
				store.turn = &TurnLease{Turn: Turn{ID: "t", BindingID: "binding", Phase: "future"}, Binding: BindingVersion{Content: Binding{ID: "binding"}}}
			default:
				store.err = ErrUnsupportedWork
			}
			ctx, cancel := context.WithTimeout(context.Background(), 35*time.Millisecond)
			defer cancel()
			var err error
			if kind[:4] == "turn" {
				err = runtime.turnWorker(ctx, "owner")
			} else {
				err = runtime.deliveryWorker(ctx, "owner")
			}
			require.NoError(t, err, "未知任务不能终止Runtime")
			require.Zero(t, provider.calls.Load())
			require.Zero(t, store.writes.Load(), "连租约续写也应在协议检查之后")
			require.Greater(t, store.leases.Load(), int32(1))
			require.LessOrEqual(t, store.leases.Load(), int32(5), "轮询应受PollInterval约束")
			require.Positive(t, observer.blocked.Load())
		})
	}
}

func TestRuntimeUnsupportedGuardPreservesInfrastructureFailure(t *testing.T) {
	failure := errors.New("database unavailable")
	for _, turn := range []bool{false, true} {
		store := &unsupportedWorkStore{err: failure}
		runtime, _, _ := unsupportedRuntime(t, store)
		var err error
		if turn {
			err = runtime.turnWorker(context.Background(), "owner")
		} else {
			err = runtime.deliveryWorker(context.Background(), "owner")
		}
		require.ErrorIs(t, err, failure)
	}
}

func TestRuntimeUnknownTurnAndSnapshotProtocolsCannotSettle(t *testing.T) {
	for _, protocol := range []string{"phase", "turn snapshot", "session snapshot"} {
		t.Run(protocol, func(t *testing.T) {
			store := &unsupportedWorkStore{}
			runtime, provider, _ := unsupportedRuntime(t, store)
			turn := Turn{ID: "turn", BindingID: "binding", ConversationID: "route", Phase: PhaseSend, Schedule: Schedule{DeadlineAt: time.Now().Add(time.Hour)}}
			switch protocol {
			case "phase":
				turn.Phase = "future"
			case "turn snapshot":
				turn.Snapshot = &TurnSnapshot{SchemaVersion: 99}
			case "session snapshot":
				turn.Session = TurnSession{SnapshotID: "snapshot", Generation: 1}
				store.snapshot = &SessionSnapshot{SchemaVersion: 99}
			}
			err := runtime.processTurn(context.Background(), &job{Turn: turn, binding: Binding{ID: "binding"}})
			require.ErrorIs(t, err, ErrUnsupportedWork)
			require.Zero(t, store.writes.Load())
			require.Zero(t, provider.calls.Load())
		})
	}
}

func TestRuntimeUnknownRemoteKindCannotEnterInputCleanup(t *testing.T) {
	store := &unsupportedWorkStore{}
	runtime, _, _ := unsupportedRuntime(t, store)
	err := runtime.reconcileRemote(context.Background(), store, RemoteOperation{ID: "op", Kind: "future"})
	require.ErrorIs(t, err, ErrUnsupportedWork)
	require.Zero(t, store.reads.Load(), "未知kind不能默认进入input查账或中断")
	require.Zero(t, store.writes.Load())
}

func TestIdentityRenewalUnknownSnapshotProtocolIsIsolated(t *testing.T) {
	store := &unsupportedWorkStore{snapshot: &SessionSnapshot{SchemaVersion: 99}}
	runtime, _, _ := unsupportedRuntime(t, store)
	err := runtime.maintainTurnIdentity(context.Background(), Turn{ID: "turn", BindingID: "binding", Session: TurnSession{SnapshotID: "future"}})
	require.ErrorIs(t, err, ErrUnsupportedWork)
	require.Zero(t, store.writes.Load())
}

func TestRuntimeStartupValidationDoesNotRejectOpaqueBindings(t *testing.T) {
	store := &unsupportedWorkStore{turn: &TurnLease{Turn: Turn{ID: "future-turn", BindingID: "future-binding", Phase: "future"}}, delivery: &Delivery{ID: "future-delivery", BindingID: "future-binding", Body: DeliveryBody{Type: "future"}}}
	runtime, _, observer := unsupportedRuntime(t, store)
	require.NoError(t, runtime.validatePersisted(context.Background()))
	require.Equal(t, int32(2), observer.blocked.Load())
}

type unsupportedCleanupResolver struct{ ConnectionResolver }

func (unsupportedCleanupResolver) ResolveForTurn(context.Context, Turn, *SessionSnapshot) (Connection, error) {
	return Connection{}, ErrChannelDisabled
}
func (unsupportedCleanupResolver) ResolveForCleanup(context.Context, Turn, *SessionSnapshot) (Connection, error) {
	return Connection{}, ErrUnsupportedWork
}

func TestRuntimeUnsupportedCleanupDoesNotRewriteTurn(t *testing.T) {
	store := &unsupportedWorkStore{}
	runtime, _, _ := unsupportedRuntime(t, store)
	runtime.options.Resolver = unsupportedCleanupResolver{}
	turn := Turn{ID: "turn", BindingID: "binding", Phase: PhaseObserve, Session: TurnSession{SessionID: "session"}, Schedule: Schedule{DeadlineAt: time.Now().Add(time.Hour)}}
	err := runtime.processTurn(context.Background(), &job{Turn: turn, binding: Binding{ID: "binding"}})
	require.ErrorIs(t, err, ErrUnsupportedWork)
	require.Zero(t, store.writes.Load())
}
