// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/ma"
)

// delayedStopClient 模拟 HTTP 已发出但响应丢失，远端在调用者退出后才消费中断。
type delayedStopClient struct {
	core.MAClient
	release chan struct{}
	calls   atomic.Int32
	workers sync.WaitGroup
}

func (c *delayedStopClient) SendInterrupt(_ context.Context, sessionID, eventID string) (core.SentResponse, error) {
	c.calls.Add(1)
	c.workers.Add(1)
	go func() {
		defer c.workers.Done()
		<-c.release
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, _ = c.MAClient.SendInterrupt(ctx, sessionID, eventID)
	}()
	return core.SentResponse{}, errors.New("interrupt response lost")
}

func stopResumeRuntime(t *testing.T, r *core.Runtime) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			require.NoError(t, <-done)
		})
	}
	t.Cleanup(stop)
	return stop
}

func TestStopResumeWaitsForDelayedInterruptAcrossRestart(t *testing.T) {
	f := &conformance.MAServer{
		RecordInterrupt: true, PageSize: 1,
		InterruptEvents: []map[string]any{{"id": "stop-idle", "type": "session.status_idle", "stop_reason": map[string]string{"type": "end_turn"}}},
	}
	options := deadlineRuntimeOptions()
	options.MessageDeadline = 10 * time.Second
	h := newHarnessWith(t, f, &runtimeProvider{}, runtimeBinding(), options)
	client, err := newTestMA(h.server.URL, "fixture", ma.ClientOptions{})
	require.NoError(t, err)
	delayed := &delayedStopClient{MAClient: client, release: make(chan struct{})}
	var release sync.Once
	allowInterrupt := func() { release.Do(func() { close(delayed.release) }) }
	t.Cleanup(func() { allowInterrupt(); delayed.workers.Wait() })
	connections := []core.Connection{{Binding: runtimeBinding(), MAClient: delayed, Provider: &runtimeProvider{}}}
	h.runtime, err = core.NewRuntime(h.store, connections, options)
	require.NoError(t, err)
	first := h.enqueue(runtimeMessage())
	stop := stopResumeRuntime(t, h.runtime)
	h.phaseEventually(core.PhaseObserve)
	h.enqueue(stopMessage("delayed-stop"))
	require.Eventually(t, func() bool { return h.turnByID(first.Turn.ID).Status == core.TurnStopped }, time.Second, 5*time.Millisecond)
	stop()

	// 旧轮自然 idle 不能证明尚在网络中的中断已经执行。
	f.Lock()
	f.Events = append(f.Events, map[string]any{"id": "natural-idle", "type": "session.status_idle", "stop_reason": map[string]string{"type": "end_turn"}})
	f.Unlock()
	h.runtime, err = core.NewRuntime(h.store, connections, options)
	require.NoError(t, err)
	follow := runtimeMessage()
	follow.ProviderEventID, follow.MessageID, follow.Text = "follow-event", "follow-message", "继续"
	next := h.enqueue(follow)
	stopResumeRuntime(t, h.runtime)
	require.Eventually(t, func() bool { return h.turnByID(next.Turn.ID).WaitReason == "remote_budget" }, time.Second, 5*time.Millisecond)
	require.Never(t, func() bool { f.Lock(); defer f.Unlock(); return f.SendCount > 1 }, 100*time.Millisecond, 5*time.Millisecond)
	require.EqualValues(t, 1, delayed.calls.Load(), "restart must not dispatch the old interrupt again")

	allowInterrupt()
	f.AppendAfterInputCount(t, 2, conformance.AnswerEvents("continued")...)
	state := h.waitAllSettled(core.DeliveryDone, 3*time.Second)
	require.Equal(t, core.TurnDone, h.turnByID(next.Turn.ID).Status)
	for _, turn := range state.turns {
		require.Equal(t, "sesn-demo", turn.Session.SessionID)
	}
	f.Lock()
	require.Equal(t, 1, f.InterruptCount)
	require.Equal(t, 2, f.SendCount)
	require.Equal(t, 1, f.CreateCount)
	f.Unlock()
	require.EqualValues(t, 1, delayed.calls.Load())
}

type stopStatusClient struct {
	core.MAClient
	running atomic.Bool
}

func TestStopResumeReconcilesExistingCleanupAfterRestart(t *testing.T) {
	f := &conformance.MAServer{}
	h := newDeadlineHarness(t, f, &runtimeProvider{})
	first := h.enqueue(runtimeMessage())
	stop := stopResumeRuntime(t, h.runtime)
	h.phaseEventually(core.PhaseObserve)
	h.enqueue(stopMessage("existing"))
	require.Eventually(t, func() bool { return h.turnByID(first.Turn.ID).Status == core.TurnStopped }, time.Second, 5*time.Millisecond)
	stop()
	f.Lock()
	f.Events = append(f.Events,
		map[string]any{"id": f.InterruptIDs[0], "type": "user.interrupt"},
		map[string]any{"id": "old-idle", "type": "session.status_idle", "stop_reason": map[string]string{"type": "end_turn"}})
	f.Unlock()
	client, err := newTestMA(h.server.URL, "fixture", ma.ClientOptions{})
	require.NoError(t, err)
	r, err := core.NewRuntime(h.store, []core.Connection{{Binding: runtimeBinding(), MAClient: client, Provider: &runtimeProvider{}}}, deadlineRuntimeOptions())
	require.NoError(t, err)
	stopResumeRuntime(t, r)
	require.Eventually(t, func() bool {
		ops, readErr := h.store.ListRemoteOperations(context.Background(), core.RemoteOperationQuery{TurnID: first.Turn.ID, OpenOnly: true})
		return readErr == nil && len(ops) == 0
	}, time.Second, 5*time.Millisecond)
}

func (c *stopStatusClient) GetSession(ctx context.Context, id string) (core.Session, error) {
	session, err := c.MAClient.GetSession(ctx, id)
	if c.running.Load() {
		session.Status = "running"
	}
	return session, err
}

func TestStopResumeRequiresCurrentSessionIdle(t *testing.T) {
	f := &conformance.MAServer{}
	options := testRuntimeOptions()
	options.MessageDeadline = 5 * time.Second
	h := newHarnessWith(t, f, &runtimeProvider{}, runtimeBinding(), options)
	client, err := newTestMA(h.server.URL, "fixture", ma.ClientOptions{})
	require.NoError(t, err)
	status := &stopStatusClient{MAClient: client}
	h.runtime, err = core.NewRuntime(h.store, []core.Connection{{Binding: runtimeBinding(), MAClient: status, Provider: &runtimeProvider{}}}, options)
	require.NoError(t, err)
	first := h.enqueue(runtimeMessage())
	h.run()
	h.phaseEventually(core.PhaseObserve)
	h.enqueue(stopMessage("status-check"))
	require.Eventually(t, func() bool { return h.turnByID(first.Turn.ID).Status == core.TurnStopped }, time.Second, 5*time.Millisecond)
	status.running.Store(true)
	f.Lock()
	f.Events = append(f.Events,
		map[string]any{"id": f.InterruptIDs[0], "type": "user.interrupt"},
		map[string]any{"id": "stop-idle", "type": "session.status_idle", "stop_reason": map[string]string{"type": "end_turn"}})
	f.Unlock()
	require.Never(t, func() bool {
		ops, readErr := h.store.ListRemoteOperations(context.Background(), core.RemoteOperationQuery{TurnID: first.Turn.ID, OpenOnly: true})
		return readErr == nil && len(ops) == 0
	}, 100*time.Millisecond, 5*time.Millisecond, "historic idle cannot override current running")
	status.running.Store(false)
	require.Eventually(t, func() bool {
		ops, readErr := h.store.ListRemoteOperations(context.Background(), core.RemoteOperationQuery{TurnID: first.Turn.ID, OpenOnly: true})
		return readErr == nil && len(ops) == 0
	}, time.Second, 5*time.Millisecond)
}
