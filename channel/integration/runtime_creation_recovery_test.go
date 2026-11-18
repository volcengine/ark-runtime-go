// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/ma"
)

// creationRecoveryMA 控制恢复时机；首次创建仍走真实HTTP，Store保留完整原始意图。
type creationRecoveryMA struct {
	core.MAClient
	enabled     atomic.Bool
	queries     atomic.Int32
	lookupError error
	mutate      func(*core.SessionPage)
}

func (m *creationRecoveryMA) ListSessions(ctx context.Context, key, page string) (core.SessionPage, error) {
	if !m.enabled.Load() {
		return core.SessionPage{}, nil
	}
	m.queries.Add(1)
	result, err := m.MAClient.ListSessions(ctx, key, page)
	if err == nil && m.mutate != nil {
		m.mutate(&result)
	}
	return result, err
}
func (m *creationRecoveryMA) GetSession(ctx context.Context, id string) (core.Session, error) {
	if m.enabled.Load() && id == "sesn-demo" && m.lookupError != nil {
		return core.Session{}, m.lookupError
	}
	return m.MAClient.GetSession(ctx, id)
}

// unknownCreationStore 仅在未知创建已持久化后通知测试推进逻辑时间。
type unknownCreationStore struct {
	core.Store
	unknown chan struct{}
	once    sync.Once
}

func (s *unknownCreationStore) WaitTurn(ctx context.Context, lease core.Lease, wait core.TurnWait) error {
	err := s.Store.WaitTurn(ctx, lease, wait)
	if err == nil && wait.Reason == "create_unknown" {
		s.once.Do(func() { close(s.unknown) })
	}
	return err
}

func TestUnknownCreationReconcilesClosedSessions(t *testing.T) {
	for _, tc := range []struct {
		name, status string
		lookupError  error
		mutate       func(*core.SessionPage)
		resolved     bool
		proof        core.RemoteProofKind
	}{
		{name: "active", status: "idle", resolved: true, proof: core.RemoteCreated},
		{name: "terminated", status: "terminated", resolved: true, proof: core.RemoteGone},
		{name: "failed", status: "failed", resolved: true, proof: core.RemoteGone},
		{name: "deleted_not_found", lookupError: &ma.APIError{Status: 404, Code: "InvalidSession.NotFound"}},
		{name: "ambiguous_404", lookupError: &ma.APIError{Status: 404, Code: "NotFound"}},
		{name: "unauthorized", lookupError: &ma.APIError{Status: 401, Code: "Unauthorized"}},
		{name: "empty_list", mutate: func(p *core.SessionPage) { p.Sessions = nil }},
		{name: "ambiguous_closed_candidates", status: "terminated", mutate: func(p *core.SessionPage) {
			if len(p.Sessions) > 0 {
				other := p.Sessions[0]
				other.ID = "sesn-other"
				p.Sessions = append(p.Sessions, other)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := &conformance.MAServer{CreatePersistThenError: true, CreateCode: 503, SessionStatus: map[string]string{"sesn-demo": tc.status, "sesn-other": tc.status}}
			clock := conformance.NewClock(time.Now())
			options := testRuntimeOptions()
			options.Clock = clock.Now
			options.CreationLimit = 1
			options.MessageDeadline = time.Minute
			options.TurnTimeout, options.LeaseTTL = 5*time.Second, 10*time.Minute
			options.UnknownWriteGrace = 20 * time.Millisecond
			binding := runtimeBinding()
			binding.MaxTurnDuration = options.TurnTimeout
			h := newHarnessWith(t, fixture, &runtimeProvider{}, binding, options)
			store := &unknownCreationStore{Store: h.store, unknown: make(chan struct{})}
			client := &creationRecoveryMA{MAClient: newMAForHarness(t, fixture), lookupError: tc.lookupError, mutate: tc.mutate}
			h.runtime = mustRuntime(t, store, binding, client, &runtimeProvider{}, options)
			message := runtimeMessage()
			message.Text = "/new"
			first := h.enqueue(message)
			h.run()
			select {
			case <-store.unknown:
			case <-time.After(5 * time.Second):
				t.Fatal("creation never reached its persisted unknown state")
			}
			clock.Advance(options.MessageDeadline)
			state := h.waitSettled(core.TurnFailed, core.DeliveryDone, 5*time.Second)
			require.Empty(t, state.turn.Session.SessionID, "unconfirmed session must not be adopted")
			ops, err := h.store.ListRemoteOperations(context.Background(), core.RemoteOperationQuery{TurnID: first.Turn.ID, OpenOnly: true})
			require.NoError(t, err)
			require.Len(t, ops, 1)
			client.enabled.Store(true)
			advanceReady := func(ops []core.RemoteOperation) {
				// 只推进重试调度，不让逻辑时间跳跃使正在查账的短租约失效。
				if len(ops) == 1 && ops[0].Lease.Owner == "" && !ops[0].Resolved {
					clock.Advance(options.PollInterval)
				}
			}
			if tc.resolved {
				require.Eventually(t, func() bool {
					ops, err := h.store.ListRemoteOperations(context.Background(), core.RemoteOperationQuery{TurnID: first.Turn.ID})
					if err == nil {
						advanceReady(ops)
					}
					return err == nil && len(ops) == 1 && ops[0].Resolved && !ops[0].Counted && ops[0].Proof != nil && ops[0].Proof.Kind == tc.proof
				}, 5*time.Second, 5*time.Millisecond)
				fixture.Lock()
				fixture.CreateCode = 0
				fixture.CreateSessionID = "sesn-next"
				fixture.Unlock()
				message.MessageID = "next"
				message.ProviderEventID = "next-event"
				h.enqueue(message)
				next := h.waitSettled(core.TurnDone, core.DeliveryDone, 5*time.Second)
				require.Equal(t, "sesn-next", next.turn.Session.SessionID, "CreationLimit=1 must allow next creation after proof")
			} else {
				require.Eventually(t, func() bool {
					ops, err := h.store.ListRemoteOperations(context.Background(), core.RemoteOperationQuery{TurnID: first.Turn.ID})
					if err == nil {
						advanceReady(ops)
					}
					return client.queries.Load() >= 2
				}, 5*time.Second, 5*time.Millisecond)
				ops, err := h.store.ListRemoteOperations(context.Background(), core.RemoteOperationQuery{TurnID: first.Turn.ID, OpenOnly: true})
				require.NoError(t, err)
				require.Len(t, ops, 1)
				require.True(t, ops[0].Counted)
				require.Nil(t, ops[0].Proof)
			}
			fixture.Lock()
			sends := fixture.SendCount
			fixture.Unlock()
			require.Zero(t, sends, "cleanup lookup must never send input")
		})
	}
}

// deadlineLookupSnapshotStore 仅在未知创建后的查账阶段耗尽调用者预算。
type deadlineLookupSnapshotStore struct {
	core.Store
	attempted     atomic.Bool
	lookupReady   atomic.Bool
	blocked       atomic.Bool
	unknown       chan struct{}
	lookupExpired chan struct{}
}

func (s *deadlineLookupSnapshotStore) GetSessionSnapshot(ctx context.Context, id string) (*core.SessionSnapshot, error) {
	if s.lookupReady.Load() && s.blocked.CompareAndSwap(false, true) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return s.Store.GetSessionSnapshot(ctx, id)
}

// GetConversation 是创建阶段在解析连接之后读取当前映射的边界；此前的快照读取属于连接解析。
func (s *deadlineLookupSnapshotStore) GetConversation(ctx context.Context, route core.RouteKey) (*core.Conversation, error) {
	if s.attempted.Load() {
		s.lookupReady.Store(true)
	}
	return s.Store.GetConversation(ctx, route)
}

func (s *deadlineLookupSnapshotStore) WaitTurn(ctx context.Context, lease core.Lease, wait core.TurnWait) error {
	if err := s.Store.WaitTurn(ctx, lease, wait); err != nil {
		return err
	}
	switch wait.Reason {
	case "create_unknown":
		close(s.unknown)
	case "create_lookup_error":
		close(s.lookupExpired)
	}
	return nil
}

// deadlineLookupMA 确保创建已产生未知结果，再让 Store 注入查账超时。
// HTTP 是否发出的分类由 MA client 契约覆盖，此处只验证 Runtime 的超时归属。
type deadlineLookupMA struct {
	core.MAClient
	store *deadlineLookupSnapshotStore
}

func (m deadlineLookupMA) CreateSession(context.Context, core.CreateSessionSpec) (core.Session, error) {
	m.store.attempted.Store(true)
	return core.Session{}, &ma.APIError{Status: 503}
}

func TestUnknownCreationLookupDeadlineIsNotStoreFailure(t *testing.T) {
	clock := conformance.NewClock(time.Now())
	options := deadlineRuntimeOptions()
	options.Clock = clock.Now
	h := newHarnessWith(t, &conformance.MAServer{}, &runtimeProvider{}, runtimeBinding(), options)
	store := &deadlineLookupSnapshotStore{Store: h.store, unknown: make(chan struct{}), lookupExpired: make(chan struct{})}
	h.runtime = mustRuntime(t, store, runtimeBinding(), deadlineLookupMA{MAClient: newMAForHarness(t, h.fixture), store: store}, &runtimeProvider{}, options)
	first := h.enqueue(runtimeMessage())
	h.run()
	select {
	case <-store.unknown:
	case <-time.After(5 * time.Second):
		t.Fatal("creation never reached unknown state")
	}
	// 用逻辑时钟只推进明确持久化的阶段，避免初始化或 HTTP 调度耗尽总期限。
	clock.Advance(options.PollInterval)
	select {
	case <-store.lookupExpired:
	case <-time.After(5 * time.Second):
		t.Fatal("lookup never exhausted its caller context")
	}
	clock.Advance(options.MessageDeadline)
	state := h.waitSettled(core.TurnFailed, core.DeliveryDone, 5*time.Second)
	require.Equal(t, "create_unconfirmed", state.turn.Outcome.ErrorKind)
	require.True(t, store.blocked.Load(), "the deadline must occur during unknown creation lookup")
	ops, err := h.store.ListRemoteOperations(context.Background(), core.RemoteOperationQuery{TurnID: first.Turn.ID, OpenOnly: true})
	require.NoError(t, err)
	require.Len(t, ops, 1)
	require.True(t, ops[0].Counted)
	require.Nil(t, ops[0].Proof)
}

func TestUnknownCreationBackendDeadlineStillStoreFailure(t *testing.T) {
	backing, err := newEmbeddedStore(t)
	require.NoError(t, err)
	store := &failedLookupSnapshotStore{Store: backing, failure: context.DeadlineExceeded}
	runtime := mustRuntime(t, store, runtimeBinding(), newMAForHarness(t, &conformance.MAServer{CreateCode: 503}), &runtimeProvider{}, testRuntimeOptions())
	mustEnqueue(t, runtime, "binding", runtimeMessage())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = runtime.Run(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	var failure core.StoreFailure
	require.ErrorAs(t, err, &failure, "backend timeout with a live caller is still infrastructure failure")
	require.NoError(t, ctx.Err(), "runtime must stop before caller timeout")
}
