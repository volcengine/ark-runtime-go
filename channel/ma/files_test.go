// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package ma_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/ma"
)

func TestResolveFileURLUsesAuthenticatedMetadataOnly(t *testing.T) {
	for _, tc := range []struct {
		name   string
		url    string
		status string
		ok     bool
	}{
		{"ready", "https://storage.example/file?signature=fixture", "active", true},
		{"missing URL", "", "active", false},
		{"credentials in URL", "https://user:password@storage.example/file", "active", false},
		{"wrong scheme", "file:///tmp/file", "active", false},
		{"processing", "https://storage.example/file", "processing", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "/api/v3/files/file-1", r.URL.Path)
				require.Equal(t, "Bearer fixture-key", r.Header.Get("Authorization"))
				require.Equal(t, "86400", r.Header.Get("X-Ark-PreSignedURL-ExpiresAfter"))
				_ = json.NewEncoder(w).Encode(map[string]any{"id": "file-1", "status": tc.status, "download_url": tc.url})
			}))
			t.Cleanup(server.Close)
			client, err := ma.NewClient(server.URL, "fixture-key", ma.ClientOptions{})
			require.NoError(t, err)
			resolver, ok := any(client).(interface {
				ResolveFileURL(context.Context, string) (string, error)
			})
			require.True(t, ok, "the MA client must expose URL resolution without downloading the object")
			got, err := resolver.ResolveFileURL(context.Background(), "file-1")
			if tc.ok {
				require.NoError(t, err)
				require.Equal(t, tc.url, got)
			} else {
				require.Error(t, err)
				require.Empty(t, got)
				require.NotContains(t, err.Error(), "signature=")
				require.NotContains(t, err.Error(), "password")
			}
		})
	}
}

func TestDownloadURLStreamsWithoutAPIKeyAndReportsOversize(t *testing.T) {
	object := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Empty(t, r.Header.Get("Authorization"), "presigned downloads never carry the API key")
		w.Header().Set("Content-Type", "image/png")
		switch r.URL.Path {
		case "/small.png":
			_, _ = io.WriteString(w, "PNG")
		case "/big.mp4":
			_, _ = io.WriteString(w, strings.Repeat("x", 64))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(object.Close)
	client, err := ma.NewClient("https://ma.example", "fixture-key", ma.ClientOptions{})
	require.NoError(t, err)

	content, err := client.DownloadURL(context.Background(), object.URL+"/small.png", 16)
	require.NoError(t, err)
	data, err := io.ReadAll(content.Body)
	require.NoError(t, err)
	require.NoError(t, content.Body.Close())
	require.Equal(t, "PNG", string(data))
	require.EqualValues(t, 3, content.Size, "size comes from Content-Length so images can be sent as images")
	require.Equal(t, "image/png", content.MimeType)

	_, err = client.DownloadURL(context.Background(), object.URL+"/big.mp4", 16)
	require.ErrorIs(t, err, core.ErrFileTooLarge)

	_, err = client.DownloadURL(context.Background(), object.URL+"/missing", 16)
	require.Error(t, err)
	require.NotErrorIs(t, err, core.ErrFileTooLarge)

	_, err = client.DownloadURL(context.Background(), "file:///etc/passwd", 16)
	require.Error(t, err)
}

func TestDownloadFileReportsOversizeFromMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"id": "file-1", "status": "active", "bytes": 100, "download_url": "https://storage.example/file"})
	}))
	t.Cleanup(server.Close)
	client, err := ma.NewClient(server.URL, "fixture-key", ma.ClientOptions{})
	require.NoError(t, err)
	_, err = client.DownloadFile(context.Background(), "file-1", 10)
	require.ErrorIs(t, err, core.ErrFileTooLarge)
}
