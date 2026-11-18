// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/provider/feishu"
	"github.com/volcengine/ark-runtime-go/channel/secret"
	"github.com/volcengine/ark-runtime-go/channel/store/embedded"
)

func TestFinalStreamHTTPRestartKeepsOneResultMessage(t *testing.T) {
	var mu sync.Mutex
	creates, sends, closes := 0, 0, 0
	var final string
	fs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal") {
			_, _ = io.WriteString(w, `{"code":0,"tenant_access_token":"fixture-token","expire":7200}`)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.URL.Path == "/open-apis/cardkit/v1/cards":
			creates++
			_, _ = io.WriteString(w, `{"code":0,"data":{"card_id":"7355439197428236291"}}`)
		case strings.HasSuffix(r.URL.Path, "/reply"):
			var body struct {
				MsgType string `json:"msg_type"`
				Content string
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Equal(t, "interactive", body.MsgType)
			require.Contains(t, body.Content, "7355439197428236291")
			sends++
			_, _ = io.WriteString(w, `{"code":0,"data":{"message_id":"one-result"}}`)
		case strings.HasSuffix(r.URL.Path, "/content"):
			var body struct{ Content string }
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			final = body.Content
			_, _ = io.WriteString(w, `{"code":0}`)
		case strings.HasSuffix(r.URL.Path, "/settings"):
			var body struct{ Settings string }
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			if strings.Contains(body.Settings, `"streaming_mode":false`) {
				closes++
			}
			_, _ = io.WriteString(w, `{"code":0}`)
		case r.URL.Path == "/open-apis/contact/v3/users/user":
			_, _ = io.WriteString(w, `{"code":0,"data":{"user":{"name":"测试用户"}}}`)
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	t.Cleanup(fs.Close)
	f := &conformance.MAServer{}
	client := newMAForHarness(t, f)
	secrets := secret.NewMemoryResolver()
	require.NoError(t, secrets.Set("memory:stream", "fixture-secret"))
	b := runtimeBinding()
	b.StreamFinalAnswer = true
	b.MaxTurnDuration = 3 * time.Second
	opts := testRuntimeOptions()
	opts.TurnTimeout, opts.LeaseTTL, opts.MessageDeadline, opts.DeliveryTimeout = 3*time.Second, 6*time.Second, 10*time.Second, 2*time.Second
	var reports atomic.Int32
	opts.Hooks.AfterDelivery = func(context.Context, core.DeliveryReport) { reports.Add(1) }
	path := filepath.Join(t.TempDir(), "stream.db")
	s, err := openEmbeddedFixture(path, embedded.Options{})
	require.NoError(t, err)
	var done chan error
	cancel := func() {}
	t.Cleanup(func() {
		cancel()
		if done != nil {
			require.NoError(t, <-done)
		}
		require.NoError(t, s.Close())
		require.EqualValues(t, 1, reports.Load())
	})
	start := func(enqueue bool) {
		p, e := feishu.NewProvider(feishu.ProviderConfig{Binding: b, AppSecretRef: "memory:stream", SecretResolver: secrets, OpenBaseURL: fs.URL})
		require.NoError(t, e)
		r, e := core.NewRuntime(s, []core.Connection{{Binding: b, MAClient: client, Provider: &outboundFeishu{p}}}, opts)
		require.NoError(t, e)
		if enqueue {
			mustEnqueue(t, r, b.ID, runtimeMessage())
		}
		var ctx context.Context
		ctx, cancel = context.WithCancel(t.Context())
		done = make(chan error, 1)
		target := done
		go func() { target <- r.Run(ctx) }()
	}
	start(true)
	f.AppendAfterInput(t, map[string]any{"id": "intermediate", "type": "agent.message", "content": []map[string]string{{"type": "text", "text": "我查一下"}}})
	mu.Lock()
	require.Zero(t, creates)
	mu.Unlock()
	answer := strings.Repeat("确定的最终答案。", 20)
	f.AppendAfterInput(t, conformance.AnswerEvents(answer)...)
	require.Eventually(t, func() bool {
		d, e := s.ListDeliveries(t.Context(), core.DeliveryQuery{})
		return e == nil && len(d) == 1 && d[0].Status == core.DeliveryPending && d[0].TextStream != nil && d[0].TextStream.Position == utf8.RuneCountInString(answer) && !d[0].TextStream.Done
	}, 3*time.Second, 5*time.Millisecond)
	cancel()
	require.NoError(t, <-done)
	done = nil
	require.NoError(t, s.Close())
	s, err = openEmbeddedFixture(path, embedded.Options{})
	require.NoError(t, err)
	start(false)
	// 停机可能发生在 checkpoint 已落盘、投递租约尚未释放时；恢复必须等待原租约过期。
	state := waitLatest(t, s, core.TurnDone, core.DeliveryDone, opts.LeaseTTL+opts.DeliveryTimeout+time.Second)
	require.Equal(t, "one-result", state.export.Deliveries[0].RemoteMessageID)
	require.Eventually(t, func() bool { return reports.Load() == 1 }, time.Second, time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, creates)
	require.Equal(t, 1, sends)
	require.Equal(t, 1, closes)
	require.Equal(t, answer, final)
}
