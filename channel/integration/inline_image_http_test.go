// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/provider/feishu"
	"github.com/volcengine/ark-runtime-go/channel/secret"
)

func TestFinalAnswerInlineImagesHTTP(t *testing.T) {
	for _, tc := range []struct {
		name       string
		failSecond bool
		limit      int
	}{
		{"all inline", false, 5},
		{"one upload fails", true, 5},
		{"count limit", false, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			uploads := map[string]int{}
			cards, messages, attachments := 0, 0, 0
			var final string
			fs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if strings.HasSuffix(request.URL.Path, "/tenant_access_token/internal") {
					_, _ = io.WriteString(w, `{"code":0,"tenant_access_token":"fixture-token","expire":7200}`)
					return
				}
				mu.Lock()
				defer mu.Unlock()
				switch {
				case request.URL.Path == "/open-apis/im/v1/images":
					require.NoError(t, request.ParseMultipartForm(1024))
					file, _, err := request.FormFile("image")
					require.NoError(t, err)
					data, err := io.ReadAll(file)
					require.NoError(t, err)
					require.NoError(t, file.Close())
					name := string(data)
					uploads[name]++
					if tc.failSecond && name == "two" && uploads[name] == 1 {
						_, _ = io.WriteString(w, `{"code":999,"msg":"upload failed"}`)
						return
					}
					_, _ = io.WriteString(w, `{"code":0,"data":{"image_key":"img_`+name+`"}}`)
				case strings.HasSuffix(request.URL.Path, "/cards"):
					cards++
					_, _ = io.WriteString(w, `{"code":0,"data":{"card_id":"7355439197428236291"}}`)
				case strings.HasSuffix(request.URL.Path, "/reply"):
					var body struct {
						MsgType string `json:"msg_type"`
					}
					require.NoError(t, json.NewDecoder(request.Body).Decode(&body))
					if body.MsgType == "interactive" {
						messages++
					} else {
						require.Equal(t, "image", body.MsgType)
						attachments++
					}
					_, _ = io.WriteString(w, `{"code":0,"data":{"message_id":"result"}}`)
				case strings.HasSuffix(request.URL.Path, "/content"):
					var body struct{ Content string }
					require.NoError(t, json.NewDecoder(request.Body).Decode(&body))
					final = body.Content
					_, _ = io.WriteString(w, `{"code":0}`)
				case strings.HasSuffix(request.URL.Path, "/settings"):
					_, _ = io.WriteString(w, `{"code":0}`)
				case request.URL.Path == "/open-apis/contact/v3/users/user":
					_, _ = io.WriteString(w, `{"code":0,"data":{"user":{"name":"测试用户"}}}`)
				default:
					t.Errorf("unexpected request %s", request.URL.Path)
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			t.Cleanup(fs.Close)
			f := &conformance.MAServer{}
			client := outputMA{MAClient: newMAForHarness(t, f), objects: map[string]string{
				"https://tos.example/outputs/one.png?sig=fixture":   "one",
				"https://tos.example/outputs/two.png?sig=fixture":   "two",
				"https://tos.example/outputs/three.png?sig=fixture": "three",
			}}
			secrets := secret.NewMemoryResolver()
			require.NoError(t, secrets.Set("memory:inline", "fixture-secret"))
			binding := runtimeBinding()
			binding.DeliverArtifacts, binding.StreamFinalAnswer = true, true
			provider, err := feishu.NewProvider(feishu.ProviderConfig{Binding: binding, AppSecretRef: "memory:inline", SecretResolver: secrets, OpenBaseURL: fs.URL})
			require.NoError(t, err)
			store, err := newEmbeddedStore(t)
			require.NoError(t, err)
			options := testRuntimeOptions()
			options.Artifacts.MaxCount = tc.limit
			runtime, err := core.NewRuntime(store, []core.Connection{{Binding: binding, MAClient: client, Provider: &outboundFeishu{provider}, Outputs: &outputResolver{}}}, options)
			require.NoError(t, err)
			h := &rtHarness{t: t, runtime: runtime, store: store, fixture: f}
			h.enqueue(runtimeMessage())
			h.run()
			answer := "已准备三种风格。\n1. ![一](/mnt/session/outputs/one.png)\n2. ![二](/mnt/session/outputs/two.png)\n3. ![三](/mnt/session/outputs/three.png)\n再次查看：![一](/mnt/session/outputs/one.png)\n回复 1、2 或 3。"
			f.AppendAfterInput(t, conformance.AnswerEvents(answer)...)
			state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
			mu.Lock()
			defer mu.Unlock()
			require.Equal(t, 1, cards)
			require.Equal(t, 1, messages)
			require.Equal(t, 1, uploads["one"], "重复引用不重新上传")
			require.Contains(t, final, "1. ![一](img_one)")
			require.Contains(t, final, "再次查看：![一](img_one)")
			require.Contains(t, final, "回复 1、2 或 3。", "图片失败也不能截断结尾")
			var storedText string
			for _, delivery := range state.export.Deliveries {
				if delivery.Body.IsTextStream() {
					storedText = delivery.Body.Text
				}
			}
			require.Equal(t, final, storedText, "转换后的正文必须通过 Store 持久化")
			if tc.failSecond {
				// 上传失败只降级第二张，原有附件投递仍可重试成功。
				require.Contains(t, final, "`![二](/mnt/session/outputs/two.png)`")
				require.Contains(t, final, "![三](img_three)")
				require.Equal(t, 1, attachments)
				require.Equal(t, 2, uploads["two"])
			} else {
				require.Zero(t, attachments, "成功内嵌不重复发送附件")
			}
			if tc.limit == 2 {
				require.Zero(t, uploads["three"], "内嵌不能绕过单轮产物数量限制")
				require.Contains(t, final, "`![三](/mnt/session/outputs/three.png)`")
				require.Contains(t, final, "另有 1 个文件超出单次发送数量")
			}
		})
	}
}
