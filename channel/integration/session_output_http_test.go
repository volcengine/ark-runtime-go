// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"bytes"
	"encoding/json"
	"image"
	"image/png"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/ma"
	"github.com/volcengine/ark-runtime-go/channel/provider/feishu"
	"github.com/volcengine/ark-runtime-go/channel/secret"
)

func TestSessionFilesFinalReplyHTTPWithoutHostedResolver(t *testing.T) {
	var pngData bytes.Buffer
	require.NoError(t, png.Encode(&pngData, image.NewRGBA(image.Rect(0, 0, 1, 1))))
	for _, tc := range []struct{ name, mime string }{{"image and video", "image/png"}, {"HTML error page", "text/html; charset=UTF-8"}} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var messages []string
			var objectRequests []string
			maFixture := &conformance.MAServer{}
			var maURL string
			maHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasPrefix(r.URL.Path, "/object/") {
					require.Empty(t, r.Header.Get("Authorization"))
					mu.Lock()
					objectRequests = append(objectRequests, r.URL.Path)
					mu.Unlock()
					if strings.HasSuffix(r.URL.Path, "one") {
						_, _ = w.Write(pngData.Bytes())
					} else {
						_, _ = io.WriteString(w, "\x00\x00\x00\x18ftypmp42")
					}
					return
				}
				if r.URL.Path == "/api/v3/files" {
					require.Equal(t, "sesn-demo", r.URL.Query().Get("scope_id"))
					var files []map[string]any
					for _, item := range []struct{ id, name, mime string }{{"file-one", "a.png", tc.mime}, {"file-video", "v.mp4", "video/mp4"}, {"file-unreferenced", "old.png", "image/png"}} {
						files = append(files, map[string]any{"id": item.id, "description": "/mnt/session/outputs/cats/" + item.name, "filename": item.name, "mime_type": item.mime, "status": "active", "scope": map[string]string{"id": "sesn-demo", "type": "session"}})
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"data": files, "has_more": false})
					return
				}
				if strings.HasPrefix(r.URL.Path, "/api/v3/files/file-") {
					id := strings.TrimPrefix(r.URL.Path, "/api/v3/files/")
					mime, size := "video/mp4", 12
					if id == "file-one" {
						mime, size = tc.mime, pngData.Len()
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"id": id, "status": "active", "bytes": size, "mime_type": mime, "download_url": maURL + "/object/" + strings.TrimPrefix(id, "file-"), "scope": map[string]string{"id": "sesn-demo", "type": "session"}})
					return
				}
				maFixture.ServeHTTP(w, r)
			}))
			t.Cleanup(maHTTP.Close)
			maURL = maHTTP.URL
			fs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.URL.Path == "/open-apis/contact/v3/users/user":
					_, _ = io.WriteString(w, `{"code":0,"data":{"user":{"name":"测试用户"}}}`)
				case strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal"):
					_, _ = io.WriteString(w, `{"code":0,"tenant_access_token":"token","expire":7200}`)
				case r.URL.Path == "/open-apis/im/v1/images":
					_, _ = io.WriteString(w, `{"code":0,"data":{"image_key":"img_one"}}`)
				case r.URL.Path == "/open-apis/im/v1/files":
					_, _ = io.WriteString(w, `{"code":0,"data":{"file_key":"file_video"}}`)
				case strings.HasSuffix(r.URL.Path, "/reply"):
					var body struct {
						MsgType string `json:"msg_type"`
						Content string `json:"content"`
					}
					require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
					require.Equal(t, "post", body.MsgType)
					mu.Lock()
					messages = append(messages, body.Content)
					mu.Unlock()
					_, _ = io.WriteString(w, `{"code":0,"data":{"message_id":"om_result"}}`)
				default:
					t.Errorf("unexpected Feishu request %s", r.URL.Path)
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			t.Cleanup(fs.Close)
			secrets := secret.NewMemoryResolver()
			require.NoError(t, secrets.Set("memory:media", "secret"))
			binding := runtimeBinding()
			binding.DeliverArtifacts, binding.StreamFinalAnswer = true, true
			provider, err := feishu.NewProvider(feishu.ProviderConfig{Binding: binding, AppSecretRef: "memory:media", SecretResolver: secrets, OpenBaseURL: fs.URL})
			require.NoError(t, err)
			client, err := newTestMA(maHTTP.URL, "key", ma.ClientOptions{})
			require.NoError(t, err)
			store, err := newEmbeddedStore(t)
			require.NoError(t, err)
			runtime, err := core.NewRuntime(store, []core.Connection{{Binding: binding, MAClient: client, Provider: &outboundFeishu{provider}}}, testRuntimeOptions())
			require.NoError(t, err)
			h := &rtHarness{t: t, store: store, fixture: maFixture, runtime: runtime}
			h.run()
			h.enqueue(runtimeMessage())
			maFixture.AppendAfterInput(t, conformance.AnswerEvents("目录 `/mnt/session/outputs/cats/`\n\n图片 `a.png`\n\n视频 [预览](v.mp4)\n\n结束")...)
			deliveries := waitDeliveries(t, store, 1)
			require.Equal(t, core.DeliveryDone, deliveries[0].Status)
			require.Equal(t, core.BodyRichText, deliveries[0].Body.Type)
			mu.Lock()
			defer mu.Unlock()
			require.Len(t, messages, 1)
			require.Contains(t, messages[0], `"tag":"media"`)
			require.Contains(t, messages[0], "结束")
			require.NotContains(t, strings.Join(objectRequests, ","), "unreferenced")
			if strings.HasPrefix(tc.mime, "text/") {
				require.Contains(t, messages[0], "实际类型与图片/视频不符")
				require.NotContains(t, strings.Join(objectRequests, ","), "/object/one")
			} else {
				require.Contains(t, messages[0], `"tag":"img"`)
			}
		})
	}
}
