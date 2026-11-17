// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestUploadInlineImageReturnsResourceWithoutSendingMessage(t *testing.T) {
	uploads := 0
	p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal") {
			tokenResponse(w)
			return
		}
		require.Equal(t, "/open-apis/im/v1/images", r.URL.Path)
		require.NoError(t, r.ParseMultipartForm(1024))
		require.Equal(t, "message", r.FormValue("image_type"))
		file, _, err := r.FormFile("image")
		require.NoError(t, err)
		defer func() { _ = file.Close() }()
		data, err := io.ReadAll(file)
		require.NoError(t, err)
		require.Equal(t, "png", string(data))
		uploads++
		_, _ = io.WriteString(w, `{"code":0,"data":{"image_key":"img_fixture"}}`)
	})
	key, err := p.UploadInlineImage(t.Context(), core.FileContent{Body: io.NopCloser(strings.NewReader("png")), Size: 3})
	require.NoError(t, err)
	require.Equal(t, "img_fixture", key)
	_, err = p.UploadInlineImage(t.Context(), core.FileContent{Body: io.NopCloser(strings.NewReader("png")), Size: maxImageBytes + 1})
	require.Error(t, err)
	require.Equal(t, 1, uploads)
}

func TestTextStreamKeepsImageAndInlineCodeReferencesWhole(t *testing.T) {
	for _, reference := range []string{"![图](img_fixture)", "`![图](/mnt/session/outputs/preview.png)`", "``![图 `一`](bad)``"} {
		t.Run(reference, func(t *testing.T) {
			text := reference + "\n全部正文与结尾。"
			var contents []string
			p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal") {
					tokenResponse(w)
					return
				}
				switch {
				case strings.HasSuffix(r.URL.Path, "/cards"):
					var request struct{ Data string }
					require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
					var card struct {
						Body struct{ Elements []struct{ Content string } }
					}
					require.NoError(t, json.Unmarshal([]byte(request.Data), &card))
					contents = append(contents, card.Body.Elements[0].Content)
					_, _ = io.WriteString(w, `{"code":0,"data":{"card_id":"7355439197428236291"}}`)
				case strings.HasSuffix(r.URL.Path, "/reply"):
					_, _ = io.WriteString(w, `{"code":0,"data":{"message_id":"original"}}`)
				case strings.HasSuffix(r.URL.Path, "/content"):
					var body struct{ Content string }
					require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
					contents = append(contents, body.Content)
					_, _ = io.WriteString(w, `{"code":0}`)
				default:
					_, _ = io.WriteString(w, `{"code":0}`)
				}
			})
			d := streamDelivery(text)
			_, err := p.DeliverTextStream(t.Context(), d, func(_ context.Context, next core.TextStreamState) error { *d.TextStream = next; return nil })
			require.NoError(t, err)
			require.NotEmpty(t, contents)
			for _, content := range contents {
				require.True(t, strings.HasPrefix(content, reference), "首次创建和每次更新都必须包含完整引用：%q", content)
			}
			require.Equal(t, text, contents[len(contents)-1])
		})
	}
}

func TestTextStreamQuotesRejectedImageOnOriginalCard(t *testing.T) {
	text := "开头\n![图](img_invalid)\n后续正文与最终选择。"
	state := core.TextStreamState{Handle: "7355439197428236291", MessageID: "original", FirstSendAt: time.Now(), Position: 1, PendingPosition: 1, LastContentAt: time.Now()}
	rejected, lastSequence := false, uint64(0)
	var final string
	p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal") {
			tokenResponse(w)
			return
		}
		require.NotEqual(t, http.MethodPost, r.Method, "降级必须复用原卡片和消息")
		var body struct {
			Content  string
			Sequence uint64
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		require.Greater(t, body.Sequence, lastSequence)
		lastSequence = body.Sequence
		if strings.HasSuffix(r.URL.Path, "/content") {
			if strings.Contains(body.Content, "![图](img_invalid)") && !strings.Contains(body.Content, "`![图](img_invalid)`") {
				rejected = true
				_, _ = io.WriteString(w, `{"code":200570,"msg":"card contains invalid image keys"}`)
				return
			}
			if rejected {
				raw, err := json.Marshal(state)
				require.NoError(t, err)
				require.Contains(t, string(raw), `"images_as_code":true`, "先持久化降级模式再更新远端")
			}
			final = body.Content
		}
		_, _ = io.WriteString(w, `{"code":0}`)
	})
	d := streamDelivery(text)
	d.Body.ImageFallbackText = "开头\n`![图](/mnt/session/outputs/original.png)`\n后续正文与最终选择。"
	d.TextStream = &state
	save := func(_ context.Context, next core.TextStreamState) error {
		require.NoError(t, state.ValidateUpdate(next))
		state = next
		return nil
	}
	_, err := p.DeliverTextStream(t.Context(), d, save)
	require.NoError(t, err)
	require.True(t, rejected)
	require.Equal(t, "开头\n`![图](/mnt/session/outputs/original.png)`\n后续正文与最终选择。", final)
	// 用 JSON 恢复持久状态，模拟等待打印期间的进程重启。
	raw, err := json.Marshal(state)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &state))
	state.LastContentAt = time.Now().Add(-time.Minute)
	result, err := p.DeliverTextStream(t.Context(), d, save)
	require.NoError(t, err)
	require.Equal(t, "original", result.MessageID)
	require.True(t, state.Done)
}

func TestImageFallbackWaitsForActualRenderedLength(t *testing.T) {
	w := textStreamWriter{
		text: []rune("short"), state: core.TextStreamState{ImagesAsCode: true},
		delivery: core.Delivery{Body: core.DeliveryBody{Text: "short", ImageFallbackText: strings.Repeat("字", 100)}},
	}
	require.Equal(t, 5100*time.Millisecond, w.printWindow(), "代码路径变长后必须等实际全文打印完成")
}
