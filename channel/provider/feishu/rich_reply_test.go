// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"bytes"
	"encoding/json"
	"image"
	"image/png"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestRichReplyUploadsThenSendsOnePostInOriginalOrder(t *testing.T) {
	var sent []map[string]any
	var uploads []string
	p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal"):
			tokenResponse(w)
		case r.URL.Path == "/open-apis/im/v1/images":
			uploads = append(uploads, "image")
			_, _ = io.WriteString(w, `{"code":0,"data":{"image_key":"img_uploaded"}}`)
		case r.URL.Path == "/open-apis/im/v1/files":
			require.NoError(t, r.ParseMultipartForm(1024))
			require.Equal(t, "mp4", r.FormValue("file_type"))
			uploads = append(uploads, "video")
			_, _ = io.WriteString(w, `{"code":0,"data":{"file_key":"file_uploaded"}}`)
		case strings.HasSuffix(r.URL.Path, "/reply"):
			var body map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			sent = append(sent, body)
			_, _ = io.WriteString(w, `{"code":0,"data":{"message_id":"om_post"}}`)
		default:
			t.Errorf("unexpected %s", r.URL.Path)
		}
	})
	var imageBytes bytes.Buffer
	require.NoError(t, png.Encode(&imageBytes, image.NewRGBA(image.Rect(0, 0, 1, 1))))
	img, err := p.UploadInlineMedia(t.Context(), core.Artifact{Kind: "image", FileName: "a.png"}, core.FileContent{Body: io.NopCloser(bytes.NewReader(imageBytes.Bytes())), Size: int64(imageBytes.Len()), MimeType: "image/png"})
	require.NoError(t, err)
	video, err := p.UploadInlineMedia(t.Context(), core.Artifact{Kind: "video", FileName: "v.mp4"}, core.FileContent{Body: io.NopCloser(strings.NewReader("\x00\x00\x00\x18ftypmp42")), Size: 12, MimeType: "video/mp4"})
	require.NoError(t, err)
	d := core.Delivery{UUID: "fixture-uuid", ReplyTarget: core.ReplyTarget{Mode: "reply", MessageID: "om_source"}, Body: core.RichTextBody([]core.RichTextPart{{Type: "text", Text: "**图片**\n"}, img, {Type: "text", Text: "\n视频\n"}, video, {Type: "text", Text: "\n结束"}})}
	id, err := p.DeliverRichText(t.Context(), d)
	require.NoError(t, err)
	require.Equal(t, "om_post", id)
	require.Equal(t, []string{"image", "video"}, uploads)
	require.Len(t, sent, 1)
	require.Equal(t, "post", sent[0]["msg_type"])
	require.Equal(t, "fixture-uuid", sent[0]["uuid"])
	var content map[string]struct {
		Content [][]map[string]string `json:"content"`
	}
	require.NoError(t, json.Unmarshal([]byte(sent[0]["content"].(string)), &content))
	require.Equal(t, [][]map[string]string{{{"tag": "md", "text": "**图片**\n"}}, {{"tag": "img", "image_key": "img_uploaded"}}, {{"tag": "md", "text": "\n视频\n"}}, {{"tag": "media", "file_key": "file_uploaded"}}, {{"tag": "md", "text": "\n结束"}}}, content["zh_cn"].Content)
}

func TestRichReplyRejectsHTMLDisguisedAsImageBeforeUpload(t *testing.T) {
	calls := 0
	p := newFixtureProvider(t, func(http.ResponseWriter, *http.Request) { calls++ })
	_, err := p.UploadInlineMedia(t.Context(), core.Artifact{Kind: "image", FileName: "a.jpg"}, core.FileContent{Body: io.NopCloser(strings.NewReader("<!DOCTYPE html><html>Just a moment...</html>")), Size: 44, MimeType: "image/jpeg"})
	require.Error(t, err)
	require.Zero(t, calls)
}
