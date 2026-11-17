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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/ma"
	"github.com/volcengine/ark-runtime-go/channel/provider/feishu"
	"github.com/volcengine/ark-runtime-go/channel/secret"
	"github.com/volcengine/ark-runtime-go/channel/store/embedded"
)

type outboundFeishu struct{ *feishu.Provider }

func (*outboundFeishu) Start(ctx context.Context, _ func(context.Context, core.Message) error) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestCOTRestartsWithBothIDs(t *testing.T) {
	var mu sync.Mutex
	creates, completes := 0, 0
	var preview, final string
	fs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal"):
			_, _ = io.WriteString(w, `{"code":0,"tenant_access_token":"fixture-cot-token","expire":7200}`)
		case r.Method == http.MethodPost && r.URL.Path == "/open-apis/im/v1/message_cot":
			var body map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Equal(t, true, body["cot_hidden"])
			require.Equal(t, "chat", body["receive_id"])
			require.Equal(t, "source-message", body["origin_message_id"])
			mu.Lock()
			creates++
			mu.Unlock()
			_, _ = io.WriteString(w, `{"cot_id":"cot-original","message_id":"process-original"}`)
		case r.Method == http.MethodPut && r.URL.Path == "/open-apis/im/v1/message_cot":
			var body struct {
				CotID     string `json:"cot_id"`
				MessageID string `json:"message_id"`
				Events    []struct {
					Kind    string `json:"event_type"`
					Content string `json:"content"`
				} `json:"events"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Equal(t, "cot-original", body.CotID)
			require.Equal(t, "process-original", body.MessageID)
			for _, event := range body.Events {
				if event.Kind == "TEXT_MESSAGE_CONTENT" {
					var content struct {
						Delta string `json:"delta"`
					}
					require.NoError(t, json.Unmarshal([]byte(event.Content), &content))
					mu.Lock()
					preview += content.Delta
					mu.Unlock()
				}
			}
			_, _ = io.WriteString(w, `{"code":0}`)
		case strings.Contains(r.URL.Path, "/message_cot/complete/"):
			require.Equal(t, "done", r.URL.Query().Get("reason"))
			require.Equal(t, "process-original", r.URL.Query().Get("message_id"))
			mu.Lock()
			completes++
			mu.Unlock()
			_, _ = io.WriteString(w, `{"code":0}`)
		case r.URL.Path == "/open-apis/im/v1/messages/source-message/reply":
			var body struct {
				Content string `json:"content"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			var content struct {
				Text string `json:"text"`
			}
			require.NoError(t, json.Unmarshal([]byte(body.Content), &content))
			mu.Lock()
			final = content.Text
			mu.Unlock()
			_, _ = io.WriteString(w, `{"code":0,"data":{"message_id":"final-original"}}`)
		case r.URL.Path == "/open-apis/contact/v3/users/user":
			_, _ = io.WriteString(w, `{"code":0,"data":{"user":{"name":"测试用户"}}}`)
		default:
			t.Errorf("unexpected Feishu request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	t.Cleanup(fs.Close)
	f := &conformance.MAServer{}
	ms := httptest.NewServer(http.HandlerFunc(f.ServeHTTP))
	t.Cleanup(ms.Close)
	client, err := newTestMA(ms.URL, "fixture-key", ma.ClientOptions{StreamTimeout: time.Second})
	require.NoError(t, err)
	secrets := secret.NewMemoryResolver()
	require.NoError(t, secrets.Set("memory:cot", "fixture-secret"))
	binding := runtimeBinding()
	binding.ProgressProtocol, binding.StreamProgress = "native_cot", true
	// DeliveryTimeout 同时限定过程卡片创建调用（core 上限 5s）与收尾投递的等待。未确认的创建会把本轮过程
	// 标记为失败且不再重放，避免重复卡片；慢 runner 上首次创建须在该期限内完成，因此取上限。
	opts := testRuntimeOptions()
	opts.MessageDeadline, opts.DeliveryTimeout, opts.ProgressInterval = 30*time.Second, 5*time.Second, time.Millisecond
	path := filepath.Join(t.TempDir(), "native.db")
	store, err := openEmbeddedFixture(path, embedded.Options{})
	require.NoError(t, err)
	var done chan error
	cancel := func() {}
	t.Cleanup(func() {
		cancel()
		if done != nil {
			require.NoError(t, <-done)
		}
		require.NoError(t, store.Close())
	})
	build := func() *core.Runtime {
		p, err := feishu.NewProvider(feishu.ProviderConfig{Binding: binding, AppSecretRef: "memory:cot", SecretResolver: secrets, OpenBaseURL: fs.URL})
		require.NoError(t, err)
		r, err := core.NewRuntime(store, []core.Connection{{Binding: binding, MAClient: client, Provider: &outboundFeishu{p}}}, opts)
		require.NoError(t, err)
		return r
	}
	start := func(r *core.Runtime) {
		var ctx context.Context
		ctx, cancel = context.WithCancel(context.Background())
		done = make(chan error, 1)
		current := done
		go func() { current <- r.Run(ctx) }()
	}
	r := build()
	accepted, err := r.Enqueue(context.Background(), binding.ID, runtimeMessage())
	require.NoError(t, err)
	start(r)
	if !assert.Eventually(t, func() bool {
		turn, err := store.GetTurn(context.Background(), accepted.Turn.ID)
		return err == nil && turn.Progress != nil && turn.Progress.Handle != "" && !turn.Progress.Attempted
	}, 15*time.Second, 10*time.Millisecond) {
		turn, err := store.GetTurn(context.Background(), accepted.Turn.ID)
		mu.Lock()
		t.Fatalf("DIAG cot handle never confirmed: creates=%d err=%v status=%s phase=%s progress=%+v", creates, err, turn.Status, turn.Phase, turn.Progress)
	}
	cancel()
	require.NoError(t, <-done)
	done = nil
	require.NoError(t, store.Close())
	store, err = openEmbeddedFixture(path, embedded.Options{})
	require.NoError(t, err)
	f.AppendAfterInput(t, map[string]any{"id": "progress-after-restart", "type": "agent.message", "content": []map[string]string{{"type": "text", "text": "重启后处理进度"}}})
	start(build())
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return preview == "重启后处理进度"
	}, 15*time.Second, 10*time.Millisecond)
	f.AppendAfterInput(t, conformance.AnswerEvents("最终答案")...)
	waitLatest(t, store, core.TurnDone, core.DeliveryDone, 15*time.Second)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, creates)
	require.Equal(t, 1, completes)
	require.Equal(t, "最终答案", final)
}
