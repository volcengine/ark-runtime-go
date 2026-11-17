// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/ma"
)

// idleClock 让小时级无进展场景按事件屏障推进，不等待真实时间。
type idleClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *idleClock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *idleClock) advance(d time.Duration) { c.mu.Lock(); defer c.mu.Unlock(); c.now = c.now.Add(d) }

func TestRuntimeProgressExtendsDeadlineAcrossOriginalHourAndRestart(t *testing.T) {
	clock := &idleClock{now: time.Now().UTC()}
	options := testRuntimeOptions()
	options.Clock, options.MessageDeadline, options.LeaseTTL = clock.Now, time.Hour, time.Hour
	fixture := &conformance.MAServer{}
	h := newHarnessWith(t, fixture, &runtimeProvider{}, runtimeBinding(), options)
	ingested := h.enqueue(runtimeMessage())
	stop := stopResumeRuntime(t, h.runtime)
	h.phaseEventually(core.PhaseObserve)
	first := h.turnByID(ingested.Turn.ID).Schedule.DeadlineAt
	clock.advance(40 * time.Minute)
	fixture.AppendAfterInput(t, map[string]any{"id": "tool-progress", "type": "agent.tool_use", "name": "bash", "processed_at": clock.Now().Format(time.RFC3339Nano)})
	require.Eventually(t, func() bool { return h.turnByID(ingested.Turn.ID).Schedule.DeadlineAt.After(first) }, time.Second, 5*time.Millisecond, "new tool activity must renew the idle deadline")
	renewed := h.turnByID(ingested.Turn.ID).Schedule.DeadlineAt
	stop()
	clock.advance(30 * time.Minute)
	second := mustRuntime(t, h.store, runtimeBinding(), newMAForHarness(t, fixture), &runtimeProvider{}, options)
	stopSecond := stopResumeRuntime(t, second)
	require.Never(t, func() bool { return h.turnByID(ingested.Turn.ID).Status.Terminal() }, 100*time.Millisecond, 5*time.Millisecond)
	require.Equal(t, renewed, h.turnByID(ingested.Turn.ID).Schedule.DeadlineAt, "replayed history and restart must not renew the deadline")
	stopSecond()
	clock.advance(31 * time.Minute)
	third := mustRuntime(t, h.store, runtimeBinding(), newMAForHarness(t, fixture), &runtimeProvider{}, options)
	stopResumeRuntime(t, third)
	h.waitSettled(core.TurnFailed, core.DeliveryDone, 2*time.Second)
	fixture.Lock()
	defer fixture.Unlock()
	require.Equal(t, 1, fixture.SendCount)
	require.Equal(t, 1, fixture.InterruptCount, "61 quiet minutes after the last progress must interrupt once")
}

func TestRuntimeReconcilesRecentProgressBeforeExpiringOnTakeover(t *testing.T) {
	clock := &idleClock{now: time.Now().UTC()}
	options := testRuntimeOptions()
	options.Clock, options.MessageDeadline, options.LeaseTTL = clock.Now, time.Hour, time.Hour
	fixture := &conformance.MAServer{}
	h := newHarnessWith(t, fixture, &runtimeProvider{}, runtimeBinding(), options)
	ingested := h.enqueue(runtimeMessage())
	stop := stopResumeRuntime(t, h.runtime)
	h.phaseEventually(core.PhaseObserve)
	stop()
	clock.advance(70 * time.Minute)
	fixture.AppendAfterInput(t, map[string]any{"id": "recent-tool", "type": "agent.tool_result", "tool_use_id": "task", "processed_at": clock.Now().Add(-time.Minute).Format(time.RFC3339Nano)})
	second := mustRuntime(t, h.store, runtimeBinding(), newMAForHarness(t, fixture), &runtimeProvider{}, options)
	stopResumeRuntime(t, second)
	require.Eventually(t, func() bool { return h.turnByID(ingested.Turn.ID).Schedule.DeadlineAt.After(clock.Now()) }, time.Second, 5*time.Millisecond)
	fixture.AppendAfterInput(t, conformance.AnswerEvents("long task finished")...)
	h.waitSettled(core.TurnDone, core.DeliveryDone, 2*time.Second)
	fixture.Lock()
	defer fixture.Unlock()
	require.Zero(t, fixture.InterruptCount)
}

func TestRuntimeDefaultIdleWindowIsOneHour(t *testing.T) {
	options, err := (core.RuntimeOptions{}).Normalize()
	require.NoError(t, err)
	require.Equal(t, time.Hour, options.MessageDeadline)
}

type idleFailingResolver struct{ fixtureResolver }

func (f idleFailingResolver) ResolveForTurn(context.Context, core.Turn, *core.SessionSnapshot) (core.Connection, error) {
	return core.Connection{}, errors.New("normal resolver unavailable")
}
func (f idleFailingResolver) ResolveForCleanup(ctx context.Context, t core.Turn, s *core.SessionSnapshot) (core.Connection, error) {
	return f.fixtureResolver.ResolveForTurn(ctx, t, s)
}

func TestIdleExpiryStillInterruptsWhenNormalResolverFails(t *testing.T) {
	clock := &idleClock{now: time.Now().UTC()}
	options := testRuntimeOptions()
	options.Clock, options.MessageDeadline, options.LeaseTTL = clock.Now, time.Hour, time.Hour
	fixture := &conformance.MAServer{}
	h := newHarnessWith(t, fixture, &runtimeProvider{}, runtimeBinding(), options)
	h.enqueue(runtimeMessage())
	stop := stopResumeRuntime(t, h.runtime)
	h.phaseEventually(core.PhaseObserve)
	stop()
	clock.advance(61 * time.Minute)
	options.Resolver = idleFailingResolver{fixtureResolver{connections: map[string]core.Connection{"binding": {Binding: runtimeBinding(), MAClient: newMAForHarness(t, fixture), Provider: &runtimeProvider{}}}}}
	second, err := core.NewRuntime(h.store, nil, options)
	require.NoError(t, err)
	stopResumeRuntime(t, second)
	state := h.waitSettled(core.TurnFailed, core.DeliveryDone, 2*time.Second)
	require.Equal(t, "idle_timeout", state.turn.Outcome.ErrorKind)
	fixture.Lock()
	defer fixture.Unlock()
	require.Equal(t, 1, fixture.InterruptCount)
}

func TestIdleTakeoverDeliversOldTerminalBeforeTimeout(t *testing.T) {
	clock := &idleClock{now: time.Now().UTC()}
	options := testRuntimeOptions()
	options.Clock, options.MessageDeadline, options.LeaseTTL = clock.Now, time.Hour, time.Hour
	fixture := &conformance.MAServer{}
	h := newHarnessWith(t, fixture, &runtimeProvider{}, runtimeBinding(), options)
	h.enqueue(runtimeMessage())
	stop := stopResumeRuntime(t, h.runtime)
	h.phaseEventually(core.PhaseObserve)
	stop()
	old := clock.Now().Format(time.RFC3339Nano)
	fixture.AppendAfterInput(t, map[string]any{"id": "answer", "type": "agent.message", "processed_at": old, "content": []map[string]string{{"type": "text", "text": "finished before disconnect"}}}, map[string]any{"id": "idle", "type": "session.status_idle", "processed_at": old, "stop_reason": map[string]string{"type": "end_turn"}})
	clock.advance(61 * time.Minute)
	second := mustRuntime(t, h.store, runtimeBinding(), newMAForHarness(t, fixture), &runtimeProvider{}, options)
	stopResumeRuntime(t, second)
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 2*time.Second)
	require.Equal(t, "finished before disconnect", state.export.Deliveries[0].Body.Text)
	fixture.Lock()
	defer fixture.Unlock()
	require.Zero(t, fixture.InterruptCount)
}

func TestIdleLiveSSETailWithoutStartContinuesAcrossWorkerRestart(t *testing.T) {
	clock := &idleClock{now: time.Now().UTC()}
	fixture := &conformance.MAServer{}
	frames := make(chan string, 8)
	var opened atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if !strings.Contains(req.URL.Path, "/threads/") {
			fixture.ServeHTTP(w, req)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.(http.Flusher).Flush()
		opened.Add(1)
		for {
			select {
			case <-req.Context().Done():
				return
			case frame := <-frames:
				_, _ = io.WriteString(w, "data: "+frame+"\n\n")
				w.(http.Flusher).Flush()
			}
		}
	}))
	t.Cleanup(server.Close)
	client, err := newTestMA(server.URL, "fixture", ma.ClientOptions{StreamTimeout: time.Minute})
	require.NoError(t, err)
	store, err := newEmbeddedStore(t, clock.Now)
	require.NoError(t, err)
	opts := testRuntimeOptions()
	opts.Clock, opts.MessageDeadline, opts.LeaseTTL = clock.Now, time.Hour, time.Hour
	first := mustRuntime(t, store, runtimeBinding(), client, &runtimeProvider{}, opts)
	accepted := mustEnqueue(t, first, "binding", runtimeMessage())
	stop := stopResumeRuntime(t, first)
	require.Eventually(t, func() bool {
		turn, e := store.GetTurn(t.Context(), accepted.Turn.ID)
		return e == nil && turn.Activity != nil && turn.Activity.InputEventID == turn.InputEventID
	}, 15*time.Second, time.Millisecond)
	delta := `{"type":"event_delta","event_id":"still-thinking","delta":{"type":"content_delta","content":{"type":"text","text":"新的思考片段"}}}`
	advance := func(d time.Duration) {
		clock.advance(d)
		require.Eventually(t, func() bool {
			select {
			case frames <- delta:
			default:
			}
			turn, e := store.GetTurn(t.Context(), accepted.Turn.ID)
			return e == nil && turn.Activity != nil && turn.Activity.LastProgressAt.Equal(clock.Now())
		}, time.Second, 5*time.Millisecond)
	}
	advance(40 * time.Minute)
	stop()
	second := mustRuntime(t, store, runtimeBinding(), client, &runtimeProvider{}, opts)
	stopResumeRuntime(t, second)
	require.Eventually(t, func() bool { return opened.Load() >= 2 }, time.Second, time.Millisecond)
	advance(30 * time.Minute)
	fixture.AppendAfterInput(t, conformance.AnswerEvents("live thinking finished")...)
	state := waitLatest(t, store, core.TurnDone, core.DeliveryDone, 2*time.Second)
	require.Equal(t, "live thinking finished", state.export.Deliveries[0].Body.Text)
	fixture.Lock()
	defer fixture.Unlock()
	require.Zero(t, fixture.InterruptCount)
}

type idleLostSendResponse struct {
	core.MAClient
	stop context.CancelFunc
}

func (m idleLostSendResponse) SendMessage(ctx context.Context, id, eventID string, input core.UserInput) (core.SentResponse, error) {
	_, err := m.MAClient.SendMessage(ctx, id, eventID, input)
	if err != nil {
		return core.SentResponse{}, err
	}
	m.stop()
	return core.SentResponse{}, errors.New("response lost after remote acceptance")
}

func TestIdleUnknownSendAdoptsRecentProgressBeforeOldDeadline(t *testing.T) {
	clock := &idleClock{now: time.Now().UTC()}
	opts := testRuntimeOptions()
	opts.Clock, opts.MessageDeadline, opts.LeaseTTL = clock.Now, time.Hour, time.Hour
	fixture := &conformance.MAServer{}
	store, err := newEmbeddedStore(t, clock.Now)
	require.NoError(t, err)
	client := newMAForHarness(t, fixture)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	first := mustRuntime(t, store, runtimeBinding(), idleLostSendResponse{MAClient: client, stop: cancel}, &runtimeProvider{}, opts)
	accepted := mustEnqueue(t, first, "binding", runtimeMessage())
	require.NoError(t, first.Run(ctx))
	saved, err := store.GetTurn(t.Context(), accepted.Turn.ID)
	require.NoError(t, err)
	require.Equal(t, core.PhaseSend, saved.Phase)
	clock.advance(70 * time.Minute)
	fixture.AppendAfterInput(t, map[string]any{"id": "accepted-work", "type": "agent.tool_use", "processed_at": clock.Now().Add(-time.Minute).Format(time.RFC3339Nano)})
	second := mustRuntime(t, store, runtimeBinding(), client, &runtimeProvider{}, opts)
	stopResumeRuntime(t, second)
	require.Eventually(t, func() bool {
		turn, e := store.GetTurn(t.Context(), accepted.Turn.ID)
		return e == nil && turn.Phase == core.PhaseObserve && turn.Schedule.DeadlineAt.After(clock.Now())
	}, time.Second, 5*time.Millisecond)
	fixture.AppendAfterInput(t, conformance.AnswerEvents("adopted without resending")...)
	waitLatest(t, store, core.TurnDone, core.DeliveryDone, 2*time.Second)
	fixture.Lock()
	defer fixture.Unlock()
	require.Equal(t, 1, fixture.SendCount)
	require.Zero(t, fixture.InterruptCount)
}
