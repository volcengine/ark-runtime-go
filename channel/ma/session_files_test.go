// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package ma_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/ma"
)

func TestSessionOutputFilesPagesAndChecksSessionOwnership(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v3/files", r.URL.Path)
		require.Equal(t, "sesn-demo", r.URL.Query().Get("scope_id"))
		require.Equal(t, "agent", r.URL.Query().Get("purpose"))
		require.Equal(t, "100", r.URL.Query().Get("limit"))
		require.Equal(t, "Bearer session-key", r.Header.Get("Authorization"))
		if r.URL.Query().Get("after") == "" {
			_, _ = io.WriteString(w, `{"data":[{"id":"file-one","filename":"one.png","description":"/mnt/session/outputs/cats/one.png","mime_type":"image/png","bytes":10,"status":"active","scope":{"id":"sesn-demo","type":"session","status":"idle","environment_id":"env","agent":{"id":"agent","version":1},"created_at":"2026-10-13T00:00:00Z","updated_at":"2026-10-13T00:00:00Z"},"object":"file","purpose":"user_data","created_at":0,"expire_at":-1}],"has_more":true,"last_id":"file-one"}`)
			return
		}
		require.Equal(t, "file-one", r.URL.Query().Get("after"))
		_, _ = io.WriteString(w, `{"data":[{"id":"file-foreign","filename":"secret.png","description":"/mnt/session/outputs/secret.png","status":"active","scope":{"id":"sesn-other","type":"session","status":"idle","environment_id":"env","agent":{"id":"agent","version":1},"created_at":"2026-10-13T00:00:00Z","updated_at":"2026-10-13T00:00:00Z"},"object":"file","purpose":"user_data","created_at":0,"expire_at":-1},{"id":"file-two","filename":"two.mp4","description":"/mnt/session/outputs/two.mp4","mime_type":"video/mp4","status":"active","scope":{"id":"sesn-demo","type":"session","status":"idle","environment_id":"env","agent":{"id":"agent","version":1},"created_at":"2026-10-13T00:00:00Z","updated_at":"2026-10-13T00:00:00Z"},"object":"file","purpose":"user_data","created_at":0,"expire_at":-1},{"id":"file-pending","filename":"wait.png","description":"/mnt/session/outputs/wait.png","status":"processing","scope":{"id":"sesn-demo","type":"session","status":"idle","environment_id":"env","agent":{"id":"agent","version":1},"created_at":"2026-10-13T00:00:00Z","updated_at":"2026-10-13T00:00:00Z"},"object":"file","purpose":"user_data","created_at":0,"expire_at":-1}],"has_more":false}`)
	}))
	t.Cleanup(server.Close)
	client, err := ma.NewClient(server.URL, "session-key", ma.ClientOptions{})
	require.NoError(t, err)
	files, err := client.ListSessionOutputFiles(t.Context(), "sesn-demo")
	require.NoError(t, err)
	require.Len(t, files, 2)
	require.Equal(t, "file-one", files[0].FileID)
	require.Equal(t, "/mnt/session/outputs/cats/one.png", files[0].Path)
	require.Equal(t, "video/mp4", files[1].MimeType)
}

func TestSessionOutputFilesRejectsRepeatedCursor(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":[],"has_more":true,"last_id":"file-same"}`)
	}))
	t.Cleanup(server.Close)
	client, err := ma.NewClient(server.URL, "key", ma.ClientOptions{})
	require.NoError(t, err)
	_, err = client.ListSessionOutputFiles(t.Context(), "sesn-demo")
	require.Error(t, err)
}

func TestDownloadSessionFileChecksOwnershipAndUsesUncredentialedObjectRequest(t *testing.T) {
	object := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Empty(t, r.Header.Get("Authorization"))
		_, _ = io.WriteString(w, "image")
	}))
	t.Cleanup(object.Close)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v3/files/file-one", r.URL.Path)
		require.Empty(t, r.URL.RawQuery)
		require.Equal(t, "Bearer session-key", r.Header.Get("Authorization"))
		_ = json.NewEncoder(w).Encode(map[string]any{"id": "file-one", "status": "active", "filename": "one.png", "download_url": object.URL, "scope": map[string]string{"id": "sesn-demo", "type": "session"}})
	}))
	t.Cleanup(server.Close)
	client, err := ma.NewClient(server.URL, "session-key", ma.ClientOptions{})
	require.NoError(t, err)
	content, err := client.DownloadSessionFile(t.Context(), "sesn-demo", "file-one", 100)
	require.NoError(t, err)
	defer func() { _ = content.Body.Close() }()
	b, err := io.ReadAll(content.Body)
	require.NoError(t, err)
	require.Equal(t, "image", string(b))
}
