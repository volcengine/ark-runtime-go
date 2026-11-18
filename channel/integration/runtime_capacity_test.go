// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	memory "github.com/volcengine/ark-runtime-go/channel/internal/testutil/memorystore"
)

// capacityMA 为每个冻结创建键保留独立 Session，可控制短调用和远端终态。
type capacityMA struct {
	core.MAClient
	mu                                           sync.Mutex
	sessions                                     map[string]core.Session
	events                                       map[string][]core.Event
	created, sent                                int
	ungatedCreates                               int
	createActive, sendActive, createMax, sendMax atomic.Int32
	createGate, sendGate                         <-chan struct{}
	observed                                     chan string
}

func newCapacityMA() *capacityMA {
	return &capacityMA{sessions: map[string]core.Session{}, events: map[string][]core.Event{}, observed: make(chan string, 2048)}
}
func trackCapacity(active, max *atomic.Int32) func() {
	n := active.Add(1)
	for old := max.Load(); n > old; old = max.Load() {
		if max.CompareAndSwap(old, n) {
			break
		}
	}
	return func() { active.Add(-1) }
}
func waitCapacityGate(ctx context.Context, gate <-chan struct{}) error {
	if gate == nil {
		return nil
	}
	select {
	case <-gate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (m *capacityMA) CreateSession(ctx context.Context, s core.CreateSessionSpec) (core.Session, error) {
	defer trackCapacity(&m.createActive, &m.createMax)()
	m.mu.Lock()
	ungated := m.created < m.ungatedCreates
	m.mu.Unlock()
	if !ungated {
		if err := waitCapacityGate(ctx, m.createGate); err != nil {
			return core.Session{}, err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.created++
	id := fmt.Sprintf("sesn-capacity-%d", m.created)
	session := core.Session{ID: id, Status: "idle", AgentID: s.Binding.AgentID, AgentVersion: s.Binding.AgentVersion, EnvironmentID: s.Binding.EnvironmentID, VaultIDs: s.Binding.VaultIDs, Tags: map[string]string{"channel_session_key": s.LookupKey}}
	m.sessions[id] = session
	return session, nil
}
func (m *capacityMA) GetSession(_ context.Context, id string) (core.Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sessions[id], nil
}
func (m *capacityMA) ListSessions(_ context.Context, key, _ string) (core.SessionPage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var sessions []core.Session
	for _, session := range m.sessions {
		if session.Tags["channel_session_key"] == key {
			sessions = append(sessions, session)
		}
	}
	return core.SessionPage{Sessions: sessions}, nil
}
func (m *capacityMA) SendMessage(ctx context.Context, id, eventID string, input core.UserInput) (core.SentResponse, error) {
	defer trackCapacity(&m.sendActive, &m.sendMax)()
	if err := waitCapacityGate(ctx, m.sendGate); err != nil {
		return core.SentResponse{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent++
	m.events[id] = append(m.events[id], core.Event{ID: eventID, Type: "user.message", Content: []core.TextBlock{{Type: "text", Text: input.ExpectedText()}}})
	return core.SentResponse{Events: []core.Event{{ID: eventID, Type: "user.message", Content: []core.TextBlock{{Type: "text", Text: input.ExpectedText()}}}}}, nil
}
func (m *capacityMA) ListEvents(_ context.Context, id string, _ core.EventQuery) (core.EventPage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return core.EventPage{Events: append([]core.Event(nil), m.events[id]...)}, nil
}
func (m *capacityMA) StreamEvents(ctx context.Context, id string) (core.EventStream, error) {
	select {
	case m.observed <- id:
	default:
	}
	<-ctx.Done()
	return nil, ctx.Err()
}
func (m *capacityMA) counts() (int, int) { m.mu.Lock(); defer m.mu.Unlock(); return m.created, m.sent }
func (m *capacityMA) finish() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id := range m.sessions {
		m.events[id] = append(m.events[id], core.Event{ID: "answer-" + id, Type: "agent.message", Content: []core.TextBlock{{Type: "text", Text: "done"}}}, core.Event{ID: "idle-" + id, Type: "session.status_idle", StopReason: "end_turn"})
	}
}
func capacityOptions() core.RuntimeOptions {
	return core.RuntimeOptions{CreateConcurrency: 2, SubmitConcurrency: 2, ObserveConcurrency: 1, PollInterval: 5 * time.Millisecond, TurnTimeout: 5 * time.Second, LeaseTTL: 6 * time.Second, DeliveryTimeout: 50 * time.Millisecond, IdleTimeout: time.Minute}
}
func capacityRuntime(t *testing.T, store core.Store, ma *capacityMA, options core.RuntimeOptions) *core.Runtime {
	t.Helper()
	binding := runtimeBinding()
	binding.Owner = "same-account"
	binding.MaxTurnDuration = 0
	r, err := core.NewRuntime(store, []core.Connection{{Binding: binding, MAClient: ma, Provider: &runtimeProvider{}}}, options)
	require.NoError(t, err)
	return r
}
func enqueueCapacity(t *testing.T, r *core.Runtime, first, n int) {
	t.Helper()
	for i := first; i < first+n; i++ {
		m := runtimeMessage()
		m.MessageID = fmt.Sprintf("message-%d", i)
		m.ProviderEventID = m.MessageID
		m.ChatID = fmt.Sprintf("chat-%d", i)
		_, err := r.Enqueue(t.Context(), "binding", m)
		require.NoError(t, err)
	}
}
func runCapacity(t *testing.T, r *core.Runtime) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); require.NoError(t, <-done) })
	return cancel, done
}

func TestStageRuntimeObservationSaturationDoesNotBlockSubmissionAndRestart(t *testing.T) {
	const waitTimeout = time.Minute
	const turnCount = 40
	store, err := memory.New(memory.Options{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	ma := newCapacityMA()
	options := capacityOptions()
	// 本用例验证容量隔离和重启恢复，不验证调用或空闲超时。所有时限覆盖断言窗口，
	// 避免 CI 的 race/coverage 开销先触发超时或续租失败，干扰恢复语义。
	options.TurnTimeout = 6 * waitTimeout
	options.LeaseTTL = 2 * options.TurnTimeout
	options.IdleTimeout = options.LeaseTTL
	options.DeliveryTimeout = 5 * time.Second
	observer := &recordingObserver{}
	options.Observer = observer
	r := capacityRuntime(t, store, ma, options)
	enqueueCapacity(t, r, 0, turnCount)
	cancel, done := runCapacity(t, r)
	defer func() { cancel() }()
	defer func() {
		if t.Failed() {
			created, sent := ma.counts()
			turns, err := store.ListTurns(t.Context(), core.TurnQuery{OpenOnly: true, Limit: 100})
			counts := map[string]int{}
			for _, turn := range turns {
				counts[string(turn.Phase)+"/"+turn.WaitReason]++
			}
			t.Logf("capacity diagnostic: creates=%d sends=%d open=%v store_err=%v", created, sent, counts, err)
		}
	}()

	require.Eventually(t, func() bool { _, sent := ma.counts(); return sent == turnCount }, waitTimeout, 5*time.Millisecond, "one observer must not cap same-account execution at 8 or 32")
	created, sent := ma.counts()
	require.Equal(t, turnCount, created)
	require.Equal(t, turnCount, sent)
	select {
	case <-ma.observed:
	case <-time.After(time.Second):
		t.Fatal("observation did not start")
	}
	operations, err := store.ListRemoteOperations(t.Context(), core.RemoteOperationQuery{OpenOnly: true, Limit: 100})
	require.NoError(t, err)
	require.Len(t, operations, turnCount, "all accepted remote inputs retain durable barriers")
	cancel()
	require.NoError(t, <-done)
	turns, err := store.ListTurns(t.Context(), core.TurnQuery{OpenOnly: true, Limit: 100})
	require.NoError(t, err)
	for _, turn := range turns {
		require.Empty(t, turn.Lease.Owner, "shutdown must release leases")
	}
	ma.finish()
	next := capacityRuntime(t, store, ma, options)
	cancelNext, doneNext := runCapacity(t, next)
	// 高频 ListTurns 会反复读取并 hydrate 全部记录，与 Runtime 争用唯一的 SQLite
	// 工作连接。先等已提交结算的事件，再一次性核验持久状态，不让断言本身制造饥饿。
	require.Eventually(t, func() bool {
		observer.mu.Lock()
		defer observer.mu.Unlock()
		settled := make(map[string]bool)
		for _, item := range observer.items {
			if item.Name == core.EventTurnSettle && item.Status == string(core.TurnDone) {
				settled[item.TurnID] = true
			}
		}
		return len(settled) == turnCount
	}, waitTimeout, 5*time.Millisecond)
	cancelNext()
	require.NoError(t, <-doneNext)
	turns, err = store.ListTurns(t.Context(), core.TurnQuery{Limit: 100})
	require.NoError(t, err)
	require.Len(t, turns, turnCount)
	for _, turn := range turns {
		require.Equal(t, core.TurnDone, turn.Status, "restart must persist successful outcomes")
		require.Empty(t, turn.Lease.Owner, "shutdown must release leases")
	}
	created, sent = ma.counts()
	require.Equal(t, turnCount, created)
	require.Equal(t, turnCount, sent, "restart never resubmits accepted input")
}

func TestStageRuntimeBoundsShortOperationsIndependently(t *testing.T) {
	const waitTimeout = 10 * time.Second
	store, err := memory.New(memory.Options{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	createGate, sendGate := make(chan struct{}), make(chan struct{})
	ma := newCapacityMA()
	ma.createGate = createGate
	ma.sendGate = sendGate
	options := capacityOptions()
	options.CreateConcurrency = 2
	options.SubmitConcurrency = 1
	// 本用例用 gate 验证并发隔离，不验证调用超时；调用与租约需覆盖全部断言窗口，
	// 否则 CI 调度延迟可能先取消阻塞提交，让它与创建吞吐的联合断言失去观测对象。
	options.TurnTimeout = 6 * waitTimeout
	options.LeaseTTL = 2 * options.TurnTimeout
	r := capacityRuntime(t, store, ma, options)
	enqueueCapacity(t, r, 0, 2)
	cancel, done := runCapacity(t, r)
	defer func() {
		if t.Failed() {
			created, sent := ma.counts()
			t.Logf("capacity diagnostic: creates=%d sends=%d create_active=%d send_active=%d create_max=%d send_max=%d",
				created, sent, ma.createActive.Load(), ma.sendActive.Load(), ma.createMax.Load(), ma.sendMax.Load())
		}
	}()
	require.Eventually(t, func() bool { return ma.createActive.Load() == 2 }, waitTimeout, time.Millisecond)
	// 额外排入一个创建候选，确保上限断言存在真实竞争。
	enqueueCapacity(t, r, 2, 1)
	require.Eventually(t, func() bool {
		turns, err := store.ListTurns(t.Context(), core.TurnQuery{OpenOnly: true, Limit: 10})
		if err != nil || len(turns) != 3 {
			return false
		}
		for _, turn := range turns {
			if turn.Phase != core.PhaseCreate {
				return false
			}
		}
		return true
	}, waitTimeout, time.Millisecond)
	require.Never(t, func() bool { return ma.createMax.Load() > 2 }, 30*time.Millisecond, time.Millisecond)
	close(createGate)
	require.Eventually(t, func() bool { return ma.sendActive.Load() == 1 }, waitTimeout, time.Millisecond)
	// 提交池已被占满后才加入新任务，避免测试依赖调度顺序提前完成准备阶段。
	enqueueCapacity(t, r, 3, 2)
	require.Eventually(t, func() bool { created, _ := ma.counts(); return created == 5 && ma.sendActive.Load() == 1 }, waitTimeout, time.Millisecond, "blocked submit must not hold creation capacity")
	require.Equal(t, int32(1), ma.sendMax.Load())
	close(sendGate)
	require.Eventually(t, func() bool { _, sent := ma.counts(); return sent == 5 }, waitTimeout, time.Millisecond)
	cancel()
	require.NoError(t, <-done)
}

func TestStageRuntimeBlockedCreationDoesNotBlockExistingSessionSubmission(t *testing.T) {
	const waitTimeout = 10 * time.Second
	store, err := memory.New(memory.Options{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	ma := newCapacityMA()
	ma.createGate = make(chan struct{})
	// 先建立一个可复用会话，再阻塞后续新会话的创建。
	ma.ungatedCreates = 1
	options := capacityOptions()
	options.SubmitConcurrency = 1
	options.TurnTimeout = 6 * waitTimeout
	options.LeaseTTL = 2 * options.TurnTimeout
	r := capacityRuntime(t, store, ma, options)
	enqueueCapacity(t, r, 0, 1)
	cancel, done := runCapacity(t, r)
	require.Eventually(t, func() bool { _, sent := ma.counts(); return sent == 1 }, waitTimeout, time.Millisecond)
	ma.finish()
	require.Eventually(t, func() bool {
		turns, err := store.ListTurns(t.Context(), core.TurnQuery{OpenOnly: true, Limit: 10})
		return err == nil && len(turns) == 0
	}, waitTimeout, time.Millisecond)

	enqueueCapacity(t, r, 1, 2)
	require.Eventually(t, func() bool { return ma.createActive.Load() == 2 }, waitTimeout, time.Millisecond)
	message := runtimeMessage()
	message.MessageID, message.ProviderEventID, message.ChatID = "follow-up", "follow-up", "chat-0"
	_, err = r.Enqueue(t.Context(), "binding", message)
	require.NoError(t, err)
	require.Eventually(t, func() bool { _, sent := ma.counts(); return sent == 2 }, waitTimeout, time.Millisecond, "blocked creation must not hold existing-session submission capacity")
	created, sent := ma.counts()
	require.Equal(t, 1, created)
	require.Equal(t, 2, sent)
	require.Equal(t, int32(2), ma.createMax.Load())
	require.Equal(t, int32(1), ma.sendMax.Load())
	cancel()
	require.NoError(t, <-done)
}
