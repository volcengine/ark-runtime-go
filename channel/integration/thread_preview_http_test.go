// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/ma"
)

func TestCOTPrimaryThreadPreviewKeepsHistoryAuthoritative(t *testing.T) {
	f, p := &conformance.MAServer{}, &nativeProgressProvider{}
	ready := make(chan struct{})
	finishPreview := make(chan struct{})
	var streams atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/threads/") {
			f.ServeHTTP(w, r)
			return
		}
		require.Equal(t, "/api/v3/sessions/sesn-demo/threads/sthr-demo/stream", r.URL.Path)
		require.Equal(t, []string{"agent.message", "agent.thinking"}, r.URL.Query()["event_deltas"])
		w.Header().Set("Content-Type", "text/event-stream")
		w.(http.Flusher).Flush()
		if streams.Add(1) != 1 {
			<-r.Context().Done()
			return
		}
		select {
		case <-ready:
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, "data: {\"type\":\"event_start\",\"event\":{\"id\":\"preview-only\",\"type\":\"agent.message\"}}\n\n")
		_, _ = io.WriteString(w, "data: {\"type\":\"event_delta\",\"event_id\":\"preview-only\",\"delta\":{\"type\":\"content_delta\",\"content\":{\"type\":\"text\",\"text\":\"过程预览\"}}}\n\n")
		_, _ = io.WriteString(w, "data: {\"id\":\"stream-end\",\"type\":\"session.status_idle\",\"stop_reason\":{\"type\":\"end_turn\"}}\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-finishPreview:
		case <-r.Context().Done():
		}
		// 主线程流结束后由 Runtime 重连；stream 的终态帧与 EOF 都不能替代 history。
	}))
	t.Cleanup(server.Close)
	client, err := newTestMA(server.URL, "test", ma.ClientOptions{})
	require.NoError(t, err)
	store, err := newEmbeddedStore(t)
	require.NoError(t, err)
	b := runtimeBinding()
	b.ProgressProtocol, b.StreamProgress, b.MaxTurnDuration = "native_cot", true, 5*time.Second
	opts := testRuntimeOptions()
	opts.TurnTimeout, opts.MessageDeadline, opts.ProgressInterval = 5*time.Second, 10*time.Second, time.Millisecond
	opts.LeaseTTL = 6 * time.Second
	runtime, err := core.NewRuntime(store, []core.Connection{{Binding: b, Provider: p, MAClient: client}}, opts)
	require.NoError(t, err)
	accepted := mustEnqueue(t, runtime, b.ID, runtimeMessage())
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	t.Cleanup(func() { cancel(); require.NoError(t, <-done) })
	require.Eventually(t, func() bool { n, _ := p.counts(); return n == 1 }, time.Second, time.Millisecond)
	close(ready)
	require.Eventually(t, func() bool {
		p.progressMu.Lock()
		defer p.progressMu.Unlock()
		return p.lastText == "过程预览"
	}, time.Second, time.Millisecond)
	close(finishPreview)
	require.Eventually(t, func() bool { return streams.Load() >= 2 }, time.Second, time.Millisecond)
	turn, err := store.GetTurn(t.Context(), accepted.Turn.ID)
	require.NoError(t, err)
	require.False(t, turn.Status.Terminal())
	require.Zero(t, p.count(), "neither a preview nor SSE EOF can produce a final reply")
	f.AppendAfterInput(t, conformance.AnswerEvents("history 最终答案")...)
	waitLatest(t, store, core.TurnDone, core.DeliveryDone, 3*time.Second)
	require.Len(t, p.delivered(), 1)
	require.Equal(t, "history 最终答案", p.delivered()[0].Body.Text)
}
