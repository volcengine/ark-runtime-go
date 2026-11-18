// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/ma"
)

func TestRuntimeSSEWakeReadsTailAndVerifiesLateInputBeforeCompletion(t *testing.T) {
	fixture, provider := &conformance.MAServer{PageSize: 1}, &runtimeProvider{}
	reads, wake, ready := make(chan string, 1000), make(chan struct{}, 4), make(chan struct{})
	var once sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if strings.HasSuffix(req.URL.Path, "/threads/sthr-demo/stream") {
			w.Header().Set("Content-Type", "text/event-stream")
			w.(http.Flusher).Flush()
			once.Do(func() { close(ready) })
			for {
				select {
				case <-req.Context().Done():
					return
				case <-wake:
					_, _ = fmt.Fprint(w, "data: {\"id\":\"wake\",\"type\":\"session.status_running\"}\n\n")
					w.(http.Flusher).Flush()
				}
			}
		}
		if req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/events") {
			reads <- req.URL.Query().Get("page")
		}
		fixture.ServeHTTP(w, req)
	}))
	t.Cleanup(server.Close)
	client, err := newTestMA(server.URL, "fixture-key", ma.ClientOptions{RequestTimeout: time.Second, StreamTimeout: time.Minute})
	require.NoError(t, err)
	store, err := newEmbeddedStore(t)
	require.NoError(t, err)
	options := testRuntimeOptions()
	options.PollInterval, options.TurnTimeout, options.LeaseTTL, options.MessageDeadline = time.Second, 10*time.Second, 20*time.Second, time.Minute
	binding := runtimeBinding()
	binding.MaxTurnDuration = options.TurnTimeout
	runtime, err := core.NewRuntime(store, []core.Connection{{Binding: binding, MAClient: client, Provider: provider}}, options)
	require.NoError(t, err)
	mustEnqueue(t, runtime, "binding", runtimeMessage())
	runUntilCleanup(t, runtime)
	fixture.AppendAfterInput(t,
		map[string]any{"id": "tool", "type": "agent.tool_use", "name": "read"},
		map[string]any{"id": "result", "type": "agent.tool_result", "tool_use_id": "tool"},
	)
	select {
	case <-ready:
	case <-time.After(3 * time.Second):
		t.Fatal("SSE not ready")
	}
	waitPage := func(want string) []string {
		t.Helper()
		var observed []string
		deadline := time.NewTimer(3 * time.Second)
		defer deadline.Stop()
		for {
			select {
			case page := <-reads:
				observed = append(observed, page)
				if page == want {
					return observed
				}
			case <-deadline.C:
				t.Fatalf("page %q not read; observed %q", want, observed)
				return nil
			}
		}
	}
	waitPage("2")
	wake <- struct{}{}
	require.Equal(t, []string{"2"}, waitPage("2"), "SSE 唤醒不能重读已缓存前缀")
	fixture.Lock()
	fixture.Events = append(fixture.Events[:1], append([]map[string]any{{"id": "late-input", "type": "user.message"}}, fixture.Events[1:]...)...)
	fixture.Events = append(fixture.Events, conformance.AnswerEvents("foreign answer")...)
	fixture.Unlock()
	wake <- struct{}{}
	state := waitLatest(t, store, core.TurnFailed, core.DeliveryDone, 4*time.Second)
	require.Equal(t, "unsupported_contract", state.turn.Outcome.ErrorKind)
	for _, delivery := range provider.delivered() {
		require.NotContains(t, delivery.Body.Text, "foreign answer")
	}
}
