// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestFetchAttachmentStreamsResource(t *testing.T) {
	p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal") {
			tokenResponse(w)
			return
		}
		require.Equal(t, "/open-apis/im/v1/messages/om-1/resources/file_1", r.URL.Path)
		require.Equal(t, "file", r.URL.Query().Get("type"))
		require.Equal(t, "Bearer fixture-token", r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/pdf")
		w.Header().Set("Content-Disposition", `attachment; filename="server.pdf"`)
		_, _ = io.WriteString(w, "%PDF-1.4")
	})
	got, err := p.FetchAttachment(context.Background(), core.Message{MessageID: "om-1"}, core.Attachment{Kind: "file", Key: "file_1"})
	require.NoError(t, err)
	defer func() { _ = got.Body.Close() }()
	data, err := io.ReadAll(got.Body)
	require.NoError(t, err)
	require.Equal(t, "%PDF-1.4", string(data))
	require.Equal(t, "server.pdf", got.FileName)
	require.Equal(t, "application/pdf", got.MimeType)
}

func TestVideoDownloadsThroughFileResource(t *testing.T) {
	p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal") {
			tokenResponse(w)
			return
		}
		require.Equal(t, "/open-apis/im/v1/messages/om-1/resources/video_1", r.URL.Path)
		require.Equal(t, "file", r.URL.Query().Get("type"))
		w.Header().Set("Content-Type", "video/mp4")
		_, _ = io.WriteString(w, "video-content")
	})
	m, accepted, err := normalize(inbound("user", "p2p", "media", `{"file_key":"video_1","file_name":"clip.mp4","image_key":"cover"}`), core.Binding{ID: "binding-1", AppID: "app-1"}, "ou-bot")
	require.NoError(t, err)
	require.True(t, accepted)
	require.Len(t, m.Attachments, 1)
	download, err := p.FetchAttachment(context.Background(), m, m.Attachments[0])
	require.NoError(t, err)
	defer func() { require.NoError(t, download.Body.Close()) }()
	content, err := io.ReadAll(download.Body)
	require.NoError(t, err)
	require.Equal(t, "video-content", string(content))
	require.Equal(t, "clip.mp4", download.FileName)
	require.Equal(t, "video/mp4", download.MimeType)
}

func TestFetchAttachmentRejectsErrorsAndBadKeys(t *testing.T) {
	p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal") {
			tokenResponse(w)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"code":234003,"msg":"no permission"}`)
	})
	_, err := p.FetchAttachment(context.Background(), core.Message{MessageID: "om-1"}, core.Attachment{Kind: "image", Key: "img"})
	require.ErrorContains(t, err, "unavailable", "json bodies are business errors, not file content")
	_, err = p.FetchAttachment(context.Background(), core.Message{MessageID: "om-1"}, core.Attachment{Kind: "image", Key: "../x"})
	require.Error(t, err)
	_, err = p.FetchAttachment(context.Background(), core.Message{MessageID: "om-1"}, core.Attachment{Kind: "audio", Key: "a"})
	require.Error(t, err)
}

func TestBotTokenReturnsExpiry(t *testing.T) {
	p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
		tokenResponse(w)
	})
	token, expiresAt, err := p.BotToken(context.Background())
	require.NoError(t, err)
	require.Equal(t, "fixture-token", token)
	require.WithinDuration(t, time.Now().Add(2*time.Hour), expiresAt, time.Minute)
}

func TestFetchAttachmentPreservesJSONFiles(t *testing.T) {
	for _, tc := range []struct {
		name        string
		disposition string
		content     string
	}{
		{"attachment", `attachment; filename="sessions.json"`, `{"sessions":[{"id":"sesn-1"}]}`},
		{"error-shaped attachment", `attachment; filename="response.json"`, `{"code":234003,"msg":"example data"}`},
		{"without disposition", "", `{"sessions":[1,2,3]}`},
		{"large JSON", "", `{"text":"` + strings.Repeat("x", 100000) + `"}`},
		{"JSON array", "", `[{"code":234003,"msg":"example data"}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal") {
					tokenResponse(w)
					return
				}
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				if tc.disposition != "" {
					w.Header().Set("Content-Disposition", tc.disposition)
				}
				_, _ = io.WriteString(w, tc.content)
			})
			got, err := p.FetchAttachment(context.Background(), core.Message{MessageID: "om-1"}, core.Attachment{Kind: "file", Key: "file_1", FileName: "sessions.json"})
			require.NoError(t, err, "a JSON MIME type does not make an attachment an API error")
			defer func() { _ = got.Body.Close() }()
			data, err := io.ReadAll(got.Body)
			require.NoError(t, err)
			require.Equal(t, tc.content, string(data), "error detection must not consume the file prefix")
		})
	}
}
