// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/ma"
)

func TestRuntimeDeliversAnswerAfterImageEchoBecomesText(t *testing.T) {
	f := &conformance.MAServer{}
	// 模拟 Event 服务先把图片转为 URL 文本，再把同一事件回显并写入历史。
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/events") {
			var body struct {
				Events []map[string]any `json:"events"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			_ = r.Body.Close()
			for _, event := range body.Events {
				content, _ := event["content"].([]any)
				for i, value := range content {
					block := value.(map[string]any)
					if block["type"] == "image" {
						content[i] = map[string]any{"type": "text", "text": "https://files.example.test/image.png"}
					}
				}
			}
			data, err := json.Marshal(body)
			if err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(data))
			r.ContentLength = int64(len(data))
		}
		f.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	client, err := newTestMA(server.URL, "fixture-key", ma.ClientOptions{RequestTimeout: time.Second, StreamTimeout: time.Second, FilePollInterval: time.Millisecond})
	require.NoError(t, err)
	store, err := newEmbeddedStore(t)
	require.NoError(t, err)
	p := &attachmentProvider{files: map[string]string{"image": "png"}}
	runtime := mustRuntime(t, store, mountBinding(), client, p, testRuntimeOptions())
	h := &rtHarness{t: t, runtime: runtime, store: store, fixture: f, server: server}
	message := runtimeMessage()
	message.Text = "这张图里写了啥"
	message.Attachments = []core.Attachment{{Kind: "image", Key: "image"}}
	h.enqueue(message)
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("图中是应用信息")...)
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 3*time.Second)
	require.Empty(t, state.turn.Outcome.ErrorKind)
	require.Len(t, state.export.Deliveries, 1)
	require.Equal(t, "图中是应用信息", state.export.Deliveries[0].Body.Text)
	f.Lock()
	defer f.Unlock()
	require.Equal(t, 1, f.SendCount)
	require.Zero(t, f.InterruptCount, "附件降级回显不能中断已受理的请求")
	require.Len(t, f.LastContent, 3, "正文、降级 URL 与挂载说明都保留")
}
