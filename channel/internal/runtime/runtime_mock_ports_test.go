// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// runtimePortStore 只实现脚本明确允许的方法；未声明的 port 调用会立即失败。
type runtimePortStore struct {
	Store
	remote   func(context.Context, RemoteOperationQuery) ([]RemoteOperation, error)
	list     func(context.Context, TurnQuery) ([]Turn, error)
	snapshot func(context.Context, string) (*SessionSnapshot, error)
	complete func(context.Context, Lease, DeliveryResult) error
}

func (s runtimePortStore) ListRemoteOperations(ctx context.Context, q RemoteOperationQuery) ([]RemoteOperation, error) {
	return s.remote(ctx, q)
}
func (s runtimePortStore) ListTurns(ctx context.Context, q TurnQuery) ([]Turn, error) {
	return s.list(ctx, q)
}
func (s runtimePortStore) GetSessionSnapshot(ctx context.Context, id string) (*SessionSnapshot, error) {
	return s.snapshot(ctx, id)
}
func (s runtimePortStore) CompleteDelivery(ctx context.Context, l Lease, r DeliveryResult) error {
	return s.complete(ctx, l, r)
}

type runtimePortResolver struct {
	ConnectionResolver
	turn     func(context.Context, Turn, *SessionSnapshot) (Connection, error)
	delivery func(context.Context, Delivery) (Connection, error)
}

func (r runtimePortResolver) ResolveForTurn(ctx context.Context, t Turn, s *SessionSnapshot) (Connection, error) {
	return r.turn(ctx, t, s)
}
func (r runtimePortResolver) ResolveForDelivery(ctx context.Context, d Delivery) (Connection, error) {
	return r.delivery(ctx, d)
}

type runtimePortProvider struct {
	Provider
	token   func(context.Context) (string, time.Time, error)
	deliver func(context.Context, Delivery) (string, error)
}

func (p runtimePortProvider) BotToken(ctx context.Context) (string, time.Time, error) {
	return p.token(ctx)
}
func (p runtimePortProvider) Deliver(ctx context.Context, d Delivery) (string, error) {
	return p.deliver(ctx, d)
}

type runtimePortMA struct {
	MAClient
	upsert func(context.Context, BotCredentialSpec, string) (string, error)
}

func (m runtimePortMA) UpsertEnvCredential(ctx context.Context, s BotCredentialSpec, token string) (string, error) {
	return m.upsert(ctx, s, token)
}

func TestDynamicIdentityMaintenanceScansOpenTurnsAcrossPages(t *testing.T) {
	now := time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC)
	b := Binding{ID: "binding", AppID: "app", Owner: "owner", Project: "project", ChannelID: "channel", BindingEpoch: 1, Identity: IdentityPolicy{P2P: IdentityBot}, VaultIDs: []string{"vault"}, BotCredential: BotCredentialSpec{VaultID: "vault", SecretName: "token", AllowedHosts: []string{"open.feishu.cn"}}}
	snapshot := NewSessionSnapshot("route", 1, CreateSessionSpec{Binding: b, CreationKey: "creation", LookupKey: "lookup", ChatType: "p2p"})
	turns := make([]Turn, MaxPageSize+1)
	for i := range turns {
		turns[i] = Turn{ID: fmt.Sprintf("turn-%04d", i), ConversationID: "route", Session: TurnSession{SnapshotID: snapshot.ID, Generation: 1}}
	}
	var queries []TurnQuery
	var mu sync.Mutex
	var snapshots, resolves, releases, tokens, writes int
	remoteCalls := 0
	store := runtimePortStore{
		remote: func(_ context.Context, q RemoteOperationQuery) ([]RemoteOperation, error) {
			require.Equal(t, RemoteOperationQuery{OpenOnly: true, Limit: MaxPageSize}, q)
			remoteCalls++
			return nil, nil
		},
		list: func(_ context.Context, q TurnQuery) ([]Turn, error) {
			queries = append(queries, q)
			switch len(queries) {
			case 1:
				return turns[:MaxPageSize], nil
			case 2:
				return turns[MaxPageSize:], nil
			default:
				t.Fatal("unexpected third page")
				return nil, nil
			}
		},
		snapshot: func(_ context.Context, id string) (*SessionSnapshot, error) {
			mu.Lock()
			defer mu.Unlock()
			assert.Equal(t, snapshot.ID, id)
			snapshots++
			return &snapshot, nil
		},
	}
	provider := runtimePortProvider{token: func(context.Context) (string, time.Time, error) {
		mu.Lock()
		defer mu.Unlock()
		tokens++
		return "token", now.Add(2 * time.Hour), nil
	}}
	ma := runtimePortMA{upsert: func(_ context.Context, s BotCredentialSpec, token string) (string, error) {
		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, b.BotCredential, s)
		assert.Equal(t, "token", token)
		writes++
		return "credential", nil
	}}
	resolver := runtimePortResolver{turn: func(_ context.Context, turn Turn, s *SessionSnapshot) (Connection, error) {
		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, snapshot, *s)
		assert.Equal(t, "route", string(turn.ConversationID))
		resolves++
		return Connection{Binding: b, Provider: provider, MAClient: ma, Release: func() { mu.Lock(); defer mu.Unlock(); releases++ }}, nil
	}}
	opts, err := (RuntimeOptions{Clock: func() time.Time { return now }, Resolver: resolver, IdentityRenewalConcurrency: 1}).Normalize()
	require.NoError(t, err)
	runtime := &Runtime{store: store, options: opts}
	require.NoError(t, runtime.maintainBotIdentities(context.Background()))
	require.Equal(t, []TurnQuery{{OpenOnly: true, Limit: MaxPageSize}, {OpenOnly: true, After: turns[MaxPageSize-1].ID, Limit: MaxPageSize}}, queries)
	require.Equal(t, MaxPageSize+1, snapshots)
	require.Equal(t, MaxPageSize+1, resolves)
	require.Equal(t, resolves, releases)
	require.Equal(t, 1, tokens, "duplicate credentials are reused across page boundaries")
	require.Equal(t, 1, writes)
	require.Equal(t, 1, remoteCalls)
}

func TestRuntimeMockPortsDeliveryPersistsResultAndReleasesConnection(t *testing.T) {
	now := time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC)
	delivery := Delivery{ID: "delivery", BindingID: "binding", Body: DeliveryBody{Type: BodyText}, FirstAttemptAt: now, Attempt: 1, Lease: Lease{ID: "delivery", Owner: "worker", Fence: 7}}
	var calls []string
	provider := runtimePortProvider{deliver: func(ctx context.Context, got Delivery) (string, error) {
		require.Equal(t, delivery, got)
		_, ok := ctx.Deadline()
		require.True(t, ok)
		calls = append(calls, "deliver")
		return "remote-message", nil
	}}
	store := runtimePortStore{complete: func(_ context.Context, l Lease, result DeliveryResult) error {
		require.Equal(t, delivery.Lease, l)
		require.Equal(t, DeliveryResult{Status: DeliveryDone, RemoteMessageID: "remote-message"}, result)
		calls = append(calls, "complete")
		return nil
	}}
	resolver := runtimePortResolver{delivery: func(_ context.Context, got Delivery) (Connection, error) {
		require.Equal(t, delivery, got)
		calls = append(calls, "resolve")
		return Connection{Binding: Binding{ID: "binding"}, Provider: provider, Release: func() { calls = append(calls, "release") }}, nil
	}}
	opts, err := (RuntimeOptions{Clock: func() time.Time { return now }, Resolver: resolver}).Normalize()
	require.NoError(t, err)
	runtime := &Runtime{store: store, options: opts}
	require.NoError(t, runtime.processDelivery(context.Background(), delivery))
	require.Equal(t, []string{"resolve", "deliver", "complete", "release"}, calls)
}

func TestRuntimeMockPortsDeliveryPreservesStoreFailureAndLeaseLost(t *testing.T) {
	cause := errors.New("store unavailable")
	for _, failure := range []error{NewStoreFailure(cause), ErrLeaseLost} {
		t.Run(failure.Error(), func(t *testing.T) {
			now := time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC)
			calls := 0
			resolver := runtimePortResolver{delivery: func(context.Context, Delivery) (Connection, error) { calls++; return Connection{}, failure }}
			opts, err := (RuntimeOptions{Clock: func() time.Time { return now }, Resolver: resolver}).Normalize()
			require.NoError(t, err)
			runtime := &Runtime{store: runtimePortStore{}, options: opts}
			err = runtime.processDelivery(context.Background(), Delivery{BindingID: "binding", Body: DeliveryBody{Type: BodyText}, FirstAttemptAt: now, Attempt: 1})
			require.ErrorIs(t, err, failure)
			require.Equal(t, 1, calls)
			if errors.Is(failure, ErrLeaseLost) {
				require.False(t, isStoreFailure(err))
			} else {
				require.True(t, isStoreFailure(err))
				require.ErrorIs(t, err, cause)
			}
		})
	}
}

func TestRuntimeMockPortsUnknownDeliveryDoesNotResolveOrWrite(t *testing.T) {
	runtime := &Runtime{store: runtimePortStore{}}
	err := runtime.processDelivery(context.Background(), Delivery{Body: DeliveryBody{Type: "future"}})
	require.ErrorIs(t, err, ErrUnsupportedWork)
	require.ErrorIs(t, err, ErrUnknownBody)
}

func TestRuntimeMockPortsCompletionFailureDoesNotResend(t *testing.T) {
	cause := errors.New("completion unavailable")
	for _, failure := range []error{NewStoreFailure(cause), ErrLeaseLost} {
		t.Run(failure.Error(), func(t *testing.T) {
			now := time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC)
			var calls []string
			provider := runtimePortProvider{deliver: func(context.Context, Delivery) (string, error) {
				calls = append(calls, "deliver")
				return "remote-message", nil
			}}
			store := runtimePortStore{complete: func(_ context.Context, _ Lease, result DeliveryResult) error {
				require.Equal(t, DeliveryResult{Status: DeliveryDone, RemoteMessageID: "remote-message"}, result)
				calls = append(calls, "complete")
				return failure
			}}
			resolver := runtimePortResolver{delivery: func(context.Context, Delivery) (Connection, error) {
				return Connection{Binding: Binding{ID: "binding"}, Provider: provider, Release: func() { calls = append(calls, "release") }}, nil
			}}
			options, err := (RuntimeOptions{Clock: func() time.Time { return now }, Resolver: resolver}).Normalize()
			require.NoError(t, err)
			runtime := &Runtime{store: store, options: options}
			err = runtime.processDelivery(context.Background(), Delivery{ID: "delivery", BindingID: "binding", Body: DeliveryBody{Type: BodyText}, FirstAttemptAt: now, Attempt: 1})
			require.ErrorIs(t, err, failure)
			require.Equal(t, []string{"deliver", "complete", "release"}, calls, "checkpoint failure must return without another provider call")
		})
	}
}

// unknownInputStore 只返回预设响应，不模拟持久化状态机。
type unknownInputStore struct {
	Store
	wait   func(context.Context, Lease, TurnWait) error
	update func(context.Context, Lease, TurnUpdate) (Turn, error)
	settle func(context.Context, Lease, TurnOutcome, []Delivery) error
}

func (s unknownInputStore) WaitTurn(ctx context.Context, l Lease, w TurnWait) error {
	return s.wait(ctx, l, w)
}
func (s unknownInputStore) UpdateTurn(ctx context.Context, l Lease, u TurnUpdate) (Turn, error) {
	return s.update(ctx, l, u)
}
func (s unknownInputStore) SettleTurn(ctx context.Context, l Lease, o TurnOutcome, d []Delivery) error {
	return s.settle(ctx, l, o, d)
}

type unknownInputMA struct {
	MAClient
	list      func(context.Context, string, EventQuery) (EventPage, error)
	send      func(context.Context, string, string, UserInput) (SentResponse, error)
	interrupt func(context.Context, string, string) (SentResponse, error)
}

func (m unknownInputMA) ListEvents(ctx context.Context, id string, q EventQuery) (EventPage, error) {
	return m.list(ctx, id, q)
}
func (m unknownInputMA) SendMessage(ctx context.Context, id, event string, input UserInput) (SentResponse, error) {
	return m.send(ctx, id, event, input)
}
func (m unknownInputMA) SendInterrupt(ctx context.Context, id, event string) (SentResponse, error) {
	return m.interrupt(ctx, id, event)
}

func TestRuntimeMockPortsUnknownInputNeverResends(t *testing.T) {
	for _, expired := range []bool{false, true} {
		t.Run(map[bool]string{false: "within_grace", true: "grace_expired"}[expired], func(t *testing.T) {
			now := time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC)
			submitted := now.Add(-time.Second)
			if expired {
				submitted = now.Add(-3 * time.Second)
			}
			turn := Turn{ID: "turn", BindingID: "binding", ConversationID: "route", Phase: PhaseSend, InputEventID: "input-event", Lease: Lease{ID: "turn", Owner: "worker", Fence: 9, ExpiresAt: now.Add(time.Minute)}, Session: TurnSession{SessionID: "sesn-unknown", Generation: 1}, Submit: Submission{WriteAttempts: 1, SubmittedAt: submitted}, Schedule: Schedule{DeadlineAt: now.Add(time.Hour)}}
			var calls []string
			sends := 0
			ma := unknownInputMA{
				list: func(_ context.Context, id string, q EventQuery) (EventPage, error) {
					require.Equal(t, "sesn-unknown", id)
					require.Empty(t, q.Page)
					require.Equal(t, 200, q.Limit)
					require.Contains(t, q.Types, "user.message")
					require.NotNil(t, q.CreatedAtGte)
					require.Equal(t, submitted.Add(-historySkew), *q.CreatedAtGte)
					calls = append(calls, "history")
					return EventPage{}, nil
				},
				send: func(context.Context, string, string, UserInput) (SentResponse, error) {
					sends++
					t.Error("unknown prior input write must never be sent again")
					return SentResponse{}, errors.New("unexpected resend")
				},
				interrupt: func(_ context.Context, id, event string) (SentResponse, error) {
					require.Equal(t, "sesn-unknown", id)
					require.NotEmpty(t, event)
					calls = append(calls, "interrupt")
					return SentResponse{}, nil
				},
			}
			store := unknownInputStore{
				wait: func(_ context.Context, l Lease, w TurnWait) error {
					require.False(t, expired, "expired unknown write must settle after cleanup")
					require.Equal(t, turn.Lease, l)
					require.Equal(t, TurnWait{Reason: "send_pending", Until: now.Add(10 * time.Millisecond), Submit: turn.Submit}, w)
					calls = append(calls, "wait:send_pending")
					return nil
				},
				update: func(_ context.Context, l Lease, u TurnUpdate) (Turn, error) {
					require.True(t, expired)
					require.Equal(t, turn.Lease, l)
					require.Equal(t, TurnUpdate{Phase: PhaseSend, Session: turn.Session, Submit: turn.Submit, RemoteKind: RemoteInput, RemoteCleanup: true}, u)
					calls = append(calls, "persist_cleanup")
					return turn, nil
				},
				settle: func(_ context.Context, l Lease, o TurnOutcome, deliveries []Delivery) error {
					require.True(t, expired)
					require.Equal(t, turn.Lease, l)
					require.Equal(t, TurnOutcome{Status: TurnFailed, ErrorKind: "send_unconfirmed"}, o)
					require.Len(t, deliveries, 1)
					require.Equal(t, BodyText, deliveries[0].Body.Type)
					require.Equal(t, replyUnconfirmed, deliveries[0].Body.Text)
					calls = append(calls, "settle:send_unconfirmed")
					return nil
				},
			}
			options, err := (RuntimeOptions{Clock: func() time.Time { return now }, UnknownWriteGrace: 2 * time.Second, PollInterval: 10 * time.Millisecond}).Normalize()
			require.NoError(t, err)
			connection := Connection{Binding: Binding{ID: "binding"}, MAClient: ma}
			runtime := &Runtime{store: store, options: options}
			j := &job{Turn: turn, msg: Message{Text: "original input", ChatID: "chat", ChatType: "p2p", MessageID: "source"}, binding: connection.Binding, connection: connection}
			require.NoError(t, runtime.sendAndObserve(context.Background(), j, connection))
			require.Zero(t, sends)
			require.Equal(t, turn.Submit, j.Submit, "unknown write attempt remains frozen")
			want := []string{"history", "wait:send_pending"}
			if expired {
				want = []string{"history", "persist_cleanup", "interrupt", "settle:send_unconfirmed"}
			}
			require.Equal(t, want, calls)
		})
	}
}
