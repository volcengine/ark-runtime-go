// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestDeliverArtifactUploadsThenSendsByType(t *testing.T) {
	var mu sync.Mutex
	var calls []string
	var sent []string
	p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal"):
			tokenResponse(w)
		case r.URL.Path == "/open-apis/im/v1/images":
			calls = append(calls, "image")
			_, _ = io.WriteString(w, `{"code":0,"data":{"image_key":"img_k"}}`)
		case r.URL.Path == "/open-apis/im/v1/files":
			require.NoError(t, r.ParseMultipartForm(1<<20))
			calls = append(calls, "file:"+r.FormValue("file_type")+":"+r.FormValue("file_name"))
			_, _ = io.WriteString(w, `{"code":0,"data":{"file_key":"file_k"}}`)
		case strings.HasSuffix(r.URL.Path, "/reply"):
			body, _ := io.ReadAll(r.Body)
			sent = append(sent, string(body))
			_, _ = io.WriteString(w, `{"code":0,"data":{"message_id":"om_sent"}}`)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
	})
	delivery := core.Delivery{UUID: "11111111-2222-3333-4444-555555555555", ReplyTarget: core.ReplyTarget{Mode: "reply", MessageID: "om_src"}, Body: core.ArtifactBody(core.Artifact{Kind: "image", FileID: "file-1"})}
	id, err := p.DeliverArtifact(context.Background(), delivery, core.FileContent{Body: io.NopCloser(strings.NewReader("png")), Size: 3})
	require.NoError(t, err)
	require.Equal(t, "om_sent", id)
	delivery.Body = core.ArtifactBody(core.Artifact{Kind: "document", FileID: "file-2"})
	_, err = p.DeliverArtifact(context.Background(), delivery, core.FileContent{Body: io.NopCloser(strings.NewReader("pdf")), FileName: "dir/../report.pdf", Size: 3})
	require.NoError(t, err)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"image", "file:pdf:report.pdf"}, calls)
	require.Len(t, sent, 2)
	require.Contains(t, sent[0], `"msg_type":"image"`)
	require.Contains(t, sent[0], `image_key`)
	require.Contains(t, sent[1], `"msg_type":"file"`)
	require.Contains(t, sent[1], "11111111-2222-3333-4444-555555555555", "uuid dedupes retried sends")
}

func TestDeliverOutputArtifactsByKind(t *testing.T) {
	var mu sync.Mutex
	var calls, sent []string
	p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal"):
			tokenResponse(w)
		case r.URL.Path == "/open-apis/im/v1/images":
			calls = append(calls, "image")
			_, _ = io.WriteString(w, `{"code":0,"data":{"image_key":"img_k"}}`)
		case r.URL.Path == "/open-apis/im/v1/files":
			require.NoError(t, r.ParseMultipartForm(1<<20))
			calls = append(calls, "file:"+r.FormValue("file_type")+":"+r.FormValue("file_name"))
			_, _ = io.WriteString(w, `{"code":0,"data":{"file_key":"file_k"}}`)
		case strings.HasSuffix(r.URL.Path, "/reply"):
			body, _ := io.ReadAll(r.Body)
			sent = append(sent, string(body))
			_, _ = io.WriteString(w, `{"code":0,"data":{"message_id":"om_sent"}}`)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
	})
	target := core.ReplyTarget{Mode: "reply", MessageID: "om_src"}
	for _, tc := range []struct {
		name    string
		path    string
		size    int64
		call    string
		msgType string
	}{
		{"video sends playable media", "/mnt/session/outputs/clip.mp4", 3, "file:mp4:clip.mp4", `"msg_type":"media"`},
		{"image with known size sends image", "/mnt/session/outputs/chart.png", 3, "image", `"msg_type":"image"`},
		{"image without size falls back to file", "/mnt/session/outputs/raw.png", 0, "file:stream:raw.png", `"msg_type":"file"`},
		{"markdown sends file", "/mnt/session/outputs/markdown-示例.md", 3, "file:stream:markdown-示例.md", `"msg_type":"file"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mu.Lock()
			calls, sent = nil, nil
			mu.Unlock()
			artifact := core.Artifact{Kind: core.ArtifactKindByName(tc.path), Path: tc.path, FileName: tc.path[strings.LastIndex(tc.path, "/")+1:]}
			delivery := core.Delivery{UUID: "11111111-2222-3333-4444-555555555555", ReplyTarget: target, Body: core.OutputFileBody(artifact)}
			_, err := p.DeliverArtifact(context.Background(), delivery, core.FileContent{Body: io.NopCloser(strings.NewReader("abc")), Size: tc.size})
			require.NoError(t, err)
			mu.Lock()
			defer mu.Unlock()
			require.Equal(t, []string{tc.call}, calls)
			require.Len(t, sent, 1)
			require.Contains(t, sent[0], tc.msgType)
		})
	}
}

func TestDeliverArtifactLinkSendsTextWithUUID(t *testing.T) {
	var sent []string
	p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal"):
			tokenResponse(w)
		case strings.HasSuffix(r.URL.Path, "/reply"):
			body, _ := io.ReadAll(r.Body)
			sent = append(sent, string(body))
			_, _ = io.WriteString(w, `{"code":0,"data":{"message_id":"om_link"}}`)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
	})
	delivery := core.Delivery{UUID: "11111111-2222-3333-4444-555555555555", ReplyTarget: core.ReplyTarget{Mode: "reply", MessageID: "om_src"}, Body: core.OutputFileBody(core.Artifact{Kind: "video", Path: "/mnt/session/outputs/big.mp4", FileName: "big.mp4"})}
	id, err := p.DeliverArtifactLink(context.Background(), delivery, "big.mp4", "https://tos.example/big.mp4?sig=x")
	require.NoError(t, err)
	require.Equal(t, "om_link", id)
	require.Len(t, sent, 1)
	require.Contains(t, sent[0], `"msg_type":"text"`)
	require.Contains(t, sent[0], "big.mp4")
	require.Contains(t, sent[0], "https://tos.example/big.mp4?sig=x")
	require.Contains(t, sent[0], "11111111-2222-3333-4444-555555555555")

	_, err = p.DeliverArtifactLink(context.Background(), delivery, "big.mp4", "javascript:alert(1)")
	require.Error(t, err, "only http(s) links are sent")
}
