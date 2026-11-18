// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/ma"
	"github.com/volcengine/ark-runtime-go/channel/store/embedded"
)

func newEmbeddedStore(t *testing.T, clock ...func() time.Time) (testStore, error) {
	t.Helper()
	options := embedded.Options{NoSync: true}
	if len(clock) > 0 {
		options.Clock = clock[0]
	}
	store, err := openEmbeddedFixture(filepath.Join(t.TempDir(), "state.db"), options)
	if err == nil {
		t.Cleanup(func() { require.NoError(t, store.Close()) })
	}
	return store, err
}

// runUntilCleanup 在后台运行 Runtime，测试结束时取消并等待退出（先于 Store 关闭）。
func runUntilCleanup(t *testing.T, runtime *core.Runtime) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-done)
	})
}

func newRuntimeWithStore(t *testing.T, store testStore, f *conformance.MAServer, p core.Provider, extra ...func(*core.RuntimeOptions)) *core.Runtime {
	t.Helper()
	server := httptest.NewServer(httpHandler(f))
	t.Cleanup(server.Close)
	client, err := newTestMA(server.URL, "fixture-key", ma.ClientOptions{RequestTimeout: time.Second, StreamTimeout: time.Second})
	require.NoError(t, err)
	options := testRuntimeOptions()
	for _, mutate := range extra {
		mutate(&options)
	}
	runtime, err := core.NewRuntime(store, []core.Connection{{Binding: runtimeBinding(), MAClient: client, Provider: p}}, options)
	require.NoError(t, err)
	return runtime
}

func harnessWithStore(t *testing.T, store testStore, f *conformance.MAServer, p core.Provider, extra ...func(*core.RuntimeOptions)) *rtHarness {
	t.Helper()
	server := httptest.NewServer(httpHandler(f))
	t.Cleanup(server.Close)
	client, err := newTestMA(server.URL, "fixture-key", ma.ClientOptions{RequestTimeout: time.Second, StreamTimeout: time.Second})
	require.NoError(t, err)
	options := testRuntimeOptions()
	for _, mutate := range extra {
		mutate(&options)
	}
	runtime, err := core.NewRuntime(store, []core.Connection{{Binding: runtimeBinding(), MAClient: client, Provider: p}}, options)
	require.NoError(t, err)
	return &rtHarness{t: t, runtime: runtime, store: store, fixture: f, server: server}
}

// waitSettledExternal 直接通过 Store 等待最新 Turn 终态，供多 Runtime 重启测试使用。
func waitSettledExternal(t *testing.T, store testStore, wantTurn core.TurnStatus, wantDelivery core.DeliveryStatus, within time.Duration) settleState {
	t.Helper()
	return waitLatest(t, store, wantTurn, wantDelivery, within)
}

func (h *rtHarness) waitSettledWithinExternal(t *testing.T, store testStore, wantTurn core.TurnStatus, wantDelivery core.DeliveryStatus, within time.Duration) settleState {
	return waitLatest(t, store, wantTurn, wantDelivery, within)
}

func TestRuntimeNewReusesPersistedSessionAfterCrash(t *testing.T) {
	f, p := &conformance.MAServer{}, &runtimeProvider{}
	store, err := newEmbeddedStore(t)
	require.NoError(t, err)
	server := httptest.NewServer(httpHandler(f))
	t.Cleanup(server.Close)
	client, err := newTestMA(server.URL, "fixture-key", ma.ClientOptions{})
	require.NoError(t, err)
	broken := &failSettleStore{Store: store, fail: true}
	options := deadlineRuntimeOptions()
	// 崩溃后需等待旧租约过期才能接管，总时限需覆盖这段时间。
	options.Workers, options.MessageDeadline = 1, 5*time.Second
	first, err := core.NewRuntime(broken, []core.Connection{{Binding: runtimeBinding(), MAClient: client, Provider: p}}, options)
	require.NoError(t, err)
	message := runtimeMessage()
	message.Text = "/new"
	ingested := mustEnqueue(t, first, "binding", message)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err = first.Run(ctx)
	cancel()
	require.ErrorContains(t, err, "simulated crash")
	// 会话绑定已在崩溃前写入。
	conversation, err := store.GetConversation(context.Background(), ingested.Turn.ConversationID)
	require.NoError(t, err)
	require.Equal(t, core.SessionActive, conversation.Session.State)
	sessionID := conversation.Session.SessionID
	second := newRuntimeWithStore(t, store, f, p, func(o *core.RuntimeOptions) {
		o.Workers, o.MessageDeadline = 1, 5*time.Second
	})
	runUntilCleanup(t, second)
	state := waitSettledExternal(t, store, core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Equal(t, "已创建新会话。", state.export.Deliveries[0].Body.Text)
	conversation, err = store.GetConversation(context.Background(), ingested.Turn.ConversationID)
	require.NoError(t, err)
	require.Equal(t, sessionID, conversation.Session.SessionID)
	f.Lock()
	require.Equal(t, 1, f.CreateCount)
	f.Unlock()
}

func TestRuntimeOutboxFailureNeverReexecutesAgent(t *testing.T) {
	f, p := &conformance.MAServer{CreateSessionID: "sesn-x"}, &runtimeProvider{err: errUnknownDelivery}
	h := newHarness(t, f, p)
	h.enqueue(runtimeMessage())
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("answer text")...)
	state := h.waitSettled(core.TurnDone, core.DeliveryFailed, 6*time.Second)
	require.Equal(t, "delivery_unconfirmed", state.export.Deliveries[0].ErrorKind)
	f.Lock()
	require.Equal(t, 1, f.SendCount)
	f.Unlock()
	require.Equal(t, 3, p.count())
	for _, delivered := range p.delivered() {
		require.Equal(t, state.export.Deliveries[0].UUID, delivered.UUID)
		require.Equal(t, state.export.Deliveries[0].Body.Text, delivered.Body.Text)
	}
}

func TestRuntimeLaterRejectionDoesNotEraseEarlierUnknownDelivery(t *testing.T) {
	f, p := &conformance.MAServer{}, &runtimeProvider{errs: []error{errUnknownDelivery, runtimeDeliveryError{kind: "rejected"}}}
	h := newHarness(t, f, p)
	h.enqueue(runtimeMessage())
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("answer text")...)
	state := h.waitSettled(core.TurnDone, core.DeliveryFailed, 6*time.Second)
	require.Equal(t, "delivery_unconfirmed", state.export.Deliveries[0].ErrorKind)
	require.Equal(t, 2, p.count())
}

func TestRuntimeStopsUnknownDeliveryAfterDedupeWindow(t *testing.T) {
	clockNow := time.Now()
	var mu sync.Mutex
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return clockNow }
	store, err := newEmbeddedStore(t, clock)
	require.NoError(t, err)
	ctx := context.Background()
	binding, message := runtimeBinding(), runtimeMessage()
	ingested, err := store.Ingest(ctx, core.NewBindingVersion(binding), message)
	require.NoError(t, err)
	lease, err := store.LeaseTurn(ctx, conformance.Claim("setup"))
	require.NoError(t, err)
	delivery := core.NewDelivery(core.DeliverySource{Kind: core.SourceTurn, ID: ingested.Turn.ID}, 0, "answer", "binding", core.ReplyTargetFor(message, binding), core.TextBody("answer"))
	require.NoError(t, store.SettleTurn(ctx, lease.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, []core.Delivery{delivery}))
	first, err := store.LeaseDelivery(ctx, "crashed", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, first)
	mu.Lock()
	clockNow = clockNow.Add(2 * time.Hour)
	mu.Unlock()
	client, err := newTestMA("https://invalid.local", "fixture-key", ma.ClientOptions{})
	require.NoError(t, err)
	p := &runtimeProvider{}
	options := testRuntimeOptions()
	options.Workers, options.Clock = 1, clock
	runtime, err := core.NewRuntime(store, []core.Connection{{Binding: binding, MAClient: client, Provider: p}}, options)
	require.NoError(t, err)
	runUntilCleanup(t, runtime)
	require.Eventually(t, func() bool {
		export, err := store.Export(context.Background())
		return err == nil && len(export.Deliveries) == 1 && export.Deliveries[0].Status == core.DeliveryFailed
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, 0, p.count())
}

func TestRuntimeNewCreateRejectionPreservesOldSession(t *testing.T) {
	f, p := &conformance.MAServer{CreateCode: 400}, &runtimeProvider{}
	h := newHarness(t, f, p)
	old := seedConversation(t, h.store, "sesn-old")
	message := runtimeMessage()
	message.Text = "/new"
	h.enqueue(message)
	h.run()
	state := h.waitSettled(core.TurnFailed, core.DeliveryDone, 6*time.Second)
	require.Equal(t, "create_rejected", state.turn.Outcome.ErrorKind)
	conversation, err := h.store.GetConversation(context.Background(), old.ID)
	require.NoError(t, err)
	require.Equal(t, old.Session, conversation.Session)
}

func TestRuntimeRefusesUnregisteredOutstandingOutbox(t *testing.T) {
	store, err := newEmbeddedStore(t)
	require.NoError(t, err)
	ctx := context.Background()
	binding, message := runtimeBinding(), runtimeMessage()
	ingested, err := store.Ingest(ctx, core.NewBindingVersion(binding), message)
	require.NoError(t, err)
	lease, err := store.LeaseTurn(ctx, conformance.Claim("setup"))
	require.NoError(t, err)
	delivery := core.NewDelivery(core.DeliverySource{Kind: core.SourceTurn, ID: ingested.Turn.ID}, 0, "answer", "binding", core.ReplyTargetFor(message, binding), core.TextBody("answer"))
	require.NoError(t, store.SettleTurn(ctx, lease.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, []core.Delivery{delivery}))
	wrong := binding
	wrong.AppID = "different-app"
	client, err := newTestMA("https://invalid.local", "fixture-key", ma.ClientOptions{})
	require.NoError(t, err)
	_, err = core.NewRuntime(store, []core.Connection{{Binding: wrong, MAClient: client, Provider: &runtimeProvider{}}}, core.RuntimeOptions{})
	require.ErrorIs(t, err, core.ErrRuntimeConfig)
}

func TestRuntimeRefusesAppChangeForPersistedConversation(t *testing.T) {
	store, err := newEmbeddedStore(t)
	require.NoError(t, err)
	seedConversation(t, store, "sesn-old")
	wrong := runtimeBinding()
	wrong.AppID = "other-app"
	client, err := newTestMA("https://invalid.local", "fixture-key", ma.ClientOptions{})
	require.NoError(t, err)
	_, err = core.NewRuntime(store, []core.Connection{{Binding: wrong, MAClient: client, Provider: &runtimeProvider{}}}, core.RuntimeOptions{})
	require.ErrorIs(t, err, core.ErrRuntimeConfig)
}

func TestRuntimeDifferentConversationsProgressConcurrently(t *testing.T) {
	var mu sync.Mutex
	sends := 0
	creates := 0
	bothSent := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v3/sessions":
			mu.Lock()
			creates++
			id := fmt.Sprintf("sesn-%d", creates)
			mu.Unlock()
			_, _ = fmt.Fprintf(w, `{"id":%q,"type":"session","status":"idle","created_at":"2026-10-13T00:00:00Z","updated_at":"2026-10-13T00:00:00Z","agent":{"id":"agent","version":1},"environment_id":"env"}`, id)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/events"):
			mu.Lock()
			sends++
			if sends == 2 {
				close(bothSent)
			}
			mu.Unlock()
			select {
			case <-bothSent:
				_, _ = w.Write([]byte(`{"data":[]}`))
			case <-r.Context().Done():
			}
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/events"):
			_, _ = w.Write([]byte(`{"data":[]}`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/events/stream"):
			w.Header().Set("Content-Type", "text/event-stream")
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v3/sessions/"):
			_, _ = w.Write([]byte(`{"id":"sesn-demo","type":"session","status":"idle","created_at":"2026-10-13T00:00:00Z","updated_at":"2026-10-13T00:00:00Z","agent":{"id":"agent","version":1},"environment_id":"env"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	store, err := newEmbeddedStore(t)
	require.NoError(t, err)
	client, err := newTestMA(server.URL, "fixture-key", ma.ClientOptions{})
	require.NoError(t, err)
	runtime, err := core.NewRuntime(store, []core.Connection{{Binding: runtimeBinding(), MAClient: client, Provider: &runtimeProvider{}}}, core.RuntimeOptions{Workers: 2, PollInterval: 10 * time.Millisecond, TurnTimeout: 10 * time.Second, LeaseTTL: 15 * time.Second, DeliveryTimeout: 200 * time.Millisecond})
	require.NoError(t, err)
	first := runtimeMessage()
	second := runtimeMessage()
	second.ChatID, second.MessageID, second.ProviderEventID = "other-chat", "other-message", "other-event"
	mustEnqueue(t, runtime, "binding", first)
	mustEnqueue(t, runtime, "binding", second)
	h := &rtHarness{t: t, runtime: runtime, store: store}
	h.run()
	// 等待窗口须短于首个发送的单轮期限：期限到期后首个发送退出，第二个顺序发送会被误判为并发。
	select {
	case <-bothSent:
	case <-time.After(8 * time.Second):
		t.Fatal("second conversation did not send while the first was in flight")
	}
}

func TestRuntimeRejectsUnsupportedGroupReplyMode(t *testing.T) {
	store, err := newEmbeddedStore(t)
	require.NoError(t, err)
	binding := runtimeBinding()
	binding.GroupReplyMode = "wrong"
	client, err := newTestMA("https://invalid.local", "fixture-key", ma.ClientOptions{})
	require.NoError(t, err)
	_, err = core.NewRuntime(store, []core.Connection{{Binding: binding, MAClient: client, Provider: &runtimeProvider{}}}, core.RuntimeOptions{})
	require.ErrorIs(t, err, core.ErrRuntimeConfig)
}
