// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	memory "github.com/volcengine/ark-runtime-go/channel/internal/testutil/memorystore"
	"github.com/volcengine/ark-runtime-go/channel/ma"
)

func TestQuietSSEReconcilesPersistedFinalBeforeTurnTimeout(t *testing.T) {
	fixture, provider := &conformance.MAServer{}, &runtimeProvider{}
	store, err := newEmbeddedStore(t)
	require.NoError(t, err)
	var activeStreams atomic.Int32
	streamOpened := make(chan struct{}, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet || req.URL.Path != "/api/v3/sessions/sesn-demo/threads/sthr-demo/stream" {
			fixture.ServeHTTP(w, req)
			return
		}
		activeStreams.Add(1)
		defer activeStreams.Add(-1)
		fixture.Lock()
		if fixture.InputID != "" && len(fixture.Events) == 1 {
			fixture.Events = append(fixture.Events, conformance.AnswerEvents("answer text")...)
		}
		fixture.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(": keepalive\n\n"))
		w.(http.Flusher).Flush()
		select {
		case streamOpened <- struct{}{}:
		default:
		}
		<-req.Context().Done()
	}))
	defer server.Close()
	client, err := newTestMA(server.URL, "fixture-key", ma.ClientOptions{RequestTimeout: time.Second, StreamTimeout: time.Second})
	require.NoError(t, err)
	options := deadlineRuntimeOptions()
	options.Workers, options.LeaseTTL, options.DeliveryTimeout = 1, time.Second, 100*time.Millisecond
	runtime, err := core.NewRuntime(store, []core.Connection{{Binding: runtimeBinding(), MAClient: client, Provider: provider}}, options)
	require.NoError(t, err)
	mustEnqueue(t, runtime, "binding", runtimeMessage())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	stopped := false
	defer func() {
		if !stopped {
			cancel()
			require.NoError(t, <-done)
		}
	}()
	select {
	case <-streamOpened:
	case <-time.After(time.Second):
		t.Fatal("stream never opened")
	}
	state := waitLatest(t, store, core.TurnDone, core.DeliveryDone, 300*time.Millisecond)
	require.Equal(t, "answer text", state.export.Deliveries[0].Body.Text)
	require.Equal(t, 1, provider.count())
	fixture.Lock()
	require.Equal(t, 1, fixture.CreateCount)
	require.Equal(t, 1, fixture.SendCount)
	fixture.Unlock()
	cancel()
	require.NoError(t, <-done)
	stopped = true
	require.Eventually(t, func() bool { return activeStreams.Load() == 0 }, time.Second, 10*time.Millisecond)
}

// TestRuntimeApprovalSurvivesRestart 验证审批交互是持久的：卡片在第一个进程中展示，进程重启后
// 用户点击仍然生效，且新进程不会重复展示同一张卡片。
func TestRuntimeApprovalSurvivesRestart(t *testing.T) {
	f := &conformance.MAServer{ConfirmationEvents: map[string][]map[string]any{
		"allow": append([]map[string]any{{"id": "running", "type": "session.status_running"}}, conformance.AnswerEvents("approved after restart")...),
	}}
	store, err := newEmbeddedStore(t)
	require.NoError(t, err)
	client := newMAForHarness(t, f)
	binding := runtimeBinding()
	binding.Approvals = true
	options := testRuntimeOptions()
	options.MessageDeadline = 5 * time.Second
	firstProvider := &approvalProvider{}
	first := mustRuntime(t, store, binding, client, firstProvider, options)
	ingested := mustEnqueue(t, first, "binding", runtimeMessage())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- first.Run(ctx) }()
	f.AppendAfterInput(t, askEvents()...)
	require.Eventually(t, func() bool { return len(firstProvider.prompted()) == 1 }, 2*time.Second, 5*time.Millisecond)
	cancel()
	require.NoError(t, <-done)
	interaction, err := store.GetInteraction(context.Background(), core.InteractionID(ingested.Turn.ID, "tool-ask"))
	require.NoError(t, err)
	require.True(t, interaction.Prompted)
	require.Equal(t, core.InteractionPending, interaction.Status)

	secondProvider := &approvalProvider{}
	second := mustRuntime(t, store, binding, client, secondProvider, options)
	runUntilCleanup(t, second)
	require.NoError(t, second.SubmitApproval(context.Background(), core.ApprovalDecision{ChatID: "chat", TurnID: ingested.Turn.ID, ToolUseID: "tool-ask", Allow: true, DeciderID: "user"}), "a card from before the restart is still valid")
	state := waitLatest(t, store, core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Equal(t, "approved after restart", state.export.Deliveries[0].Body.Text)
	require.Empty(t, secondProvider.prompted(), "a prompted card is not shown again after restart")
	decided, err := store.GetInteraction(context.Background(), interaction.ID)
	require.NoError(t, err)
	require.Equal(t, core.InteractionAllowed, decided.Status)
}

// confirmFailMA 让第一次工具确认写回失败，模拟决定已持久但 MA 未收到。
type confirmFailMA struct {
	core.MAClient
	failures atomic.Int32
}

func (m *confirmFailMA) SendToolConfirmation(ctx context.Context, sessionID string, confirmation core.ToolConfirmation) (core.SentResponse, error) {
	if m.failures.Add(1) == 1 {
		return core.SentResponse{}, errUnknownDelivery
	}
	return m.MAClient.SendToolConfirmation(ctx, sessionID, confirmation)
}

func TestRuntimeResendsPersistedDecisionWhenConfirmationFailed(t *testing.T) {
	f := &conformance.MAServer{ConfirmationEvents: map[string][]map[string]any{
		"deny": append([]map[string]any{{"id": "running", "type": "session.status_running"}}, conformance.AnswerEvents("没有执行")...),
	}}
	store, err := newEmbeddedStore(t)
	require.NoError(t, err)
	client := &confirmFailMA{MAClient: newMAForHarness(t, f)}
	binding := runtimeBinding()
	binding.Approvals = true
	options := testRuntimeOptions()
	options.MessageDeadline = 5 * time.Second
	p := &approvalProvider{}
	runtime := mustRuntime(t, store, binding, client, p, options)
	ingested := mustEnqueue(t, runtime, "binding", runtimeMessage())
	runUntilCleanup(t, runtime)
	f.AppendAfterInput(t, askEvents()...)
	require.Eventually(t, func() bool { return len(p.prompted()) == 1 }, 2*time.Second, 5*time.Millisecond)
	require.NoError(t, runtime.SubmitApproval(context.Background(), core.ApprovalDecision{ChatID: "chat", TurnID: ingested.Turn.ID, ToolUseID: "tool-ask", Allow: false, DeciderID: "user", DenyReason: "不要"}), "the decision is recorded even if MA is briefly unavailable")
	state := waitLatest(t, store, core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Equal(t, "没有执行", state.export.Deliveries[0].Body.Text)
	require.GreaterOrEqual(t, client.failures.Load(), int32(2), "the worker re-sent the persisted decision")
	f.Lock()
	require.Equal(t, "deny", f.Confirmations[0]["result"])
	f.Unlock()
}

type expiringApprovalStore struct {
	core.Store
	expiresAt time.Time
}

func (s expiringApprovalStore) PutInteraction(ctx context.Context, lease core.Lease, interaction core.Interaction) (core.Interaction, error) {
	interaction.ExpiresAt = s.expiresAt
	return s.Store.PutInteraction(ctx, lease, interaction)
}

func TestRuntimeExpiredApprovalIsDeniedWithoutAnotherClick(t *testing.T) {
	fixture := &conformance.MAServer{ConfirmationEvents: map[string][]map[string]any{
		"deny": append([]map[string]any{{"id": "running", "type": "session.status_running"}}, conformance.AnswerEvents("审批已超时，操作未执行")...),
	}}
	store, err := newEmbeddedStore(t)
	require.NoError(t, err)
	binding := runtimeBinding()
	binding.Approvals = true
	provider := &approvalProvider{}
	options := deadlineRuntimeOptions()
	options.MessageDeadline = 3 * time.Second
	wrapped := expiringApprovalStore{Store: store, expiresAt: time.Now().Add(300 * time.Millisecond)}
	runtime := mustRuntime(t, wrapped, binding, newMAForHarness(t, fixture), provider, options)
	ingested := mustEnqueue(t, runtime, "binding", runtimeMessage())
	runUntilCleanup(t, runtime)
	fixture.AppendAfterInput(t, askEvents()...)
	state := waitLatest(t, store, core.TurnDone, core.DeliveryDone, 5*time.Second)
	require.Contains(t, state.export.Deliveries[0].Body.Text, "操作未执行")
	interaction, err := store.GetInteraction(context.Background(), core.InteractionID(ingested.Turn.ID, "tool-ask"))
	require.NoError(t, err)
	require.Equal(t, core.InteractionExpired, interaction.Status)
	fixture.Lock()
	defer fixture.Unlock()
	require.NotEmpty(t, fixture.Confirmations)
	require.Equal(t, "deny", fixture.Confirmations[0]["result"])
	require.NotEmpty(t, fixture.Confirmations[0]["deny_message"])
}

func TestRuntimeApprovalWaitStartsWhenFirstRequested(t *testing.T) {
	base := time.Date(2026, 10, 20, 1, 0, 0, 0, time.UTC)
	var nanos atomic.Int64
	nanos.Store(base.UnixNano())
	clock := func() time.Time { return time.Unix(0, nanos.Load()) }
	store, err := memory.New(memory.Options{Clock: clock})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	fixture := &conformance.MAServer{}
	provider := &approvalProvider{}
	binding := runtimeBinding()
	binding.Approvals = true
	options := testRuntimeOptions()
	options.Clock = clock
	options.MessageDeadline = time.Hour
	runtime := mustRuntime(t, store, binding, newMAForHarness(t, fixture), provider, options)
	mustEnqueue(t, runtime, "binding", runtimeMessage())
	nanos.Store(base.Add(11 * time.Minute).UnixNano())
	runUntilCleanup(t, runtime)
	fixture.AppendAfterInput(t, askEvents()...)
	require.Eventually(t, func() bool { return len(provider.prompted()) == 1 }, 2*time.Second, 5*time.Millisecond)
	require.True(t, base.Add(21*time.Minute).Equal(provider.prompted()[0].ExpiresAt))
}
