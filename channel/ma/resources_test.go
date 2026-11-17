// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package ma

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestCreateAndListSessionResources(t *testing.T) {
	var created map[string]string
	var gotPath, gotMethod string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		switch r.Method {
		case http.MethodPost:
			require.NoError(t, json.NewDecoder(r.Body).Decode(&created))
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "rsrc-1", "type": "file", "file_id": created["file_id"], "mount_path": created["mount_path"]})
		default:
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{{"id": "rsrc-1", "type": "file", "file_id": "file-1", "mount_path": "a.csv"}}})
		}
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "k", ClientOptions{})
	require.NoError(t, err)
	resource, err := client.CreateSessionResource(context.Background(), "sesn-1", "file-1", "a.csv")
	require.NoError(t, err)
	require.Equal(t, http.MethodPost, gotMethod)
	require.Equal(t, "/api/v3/sessions/sesn-1/resources", gotPath)
	require.Equal(t, "file", created["type"])
	require.Equal(t, core.SessionResource{ID: "rsrc-1", FileID: "file-1", MountPath: "a.csv", ActualPath: core.MountedActualPath("a.csv")}, resource)
	list, err := client.ListSessionResources(context.Background(), "sesn-1")
	require.NoError(t, err)
	require.Len(t, list, 1)
	require.Equal(t, "a.csv", list[0].MountPath)
}

func TestCreateSessionResourceRejectsInvalidSession(t *testing.T) {
	client, err := NewClient("https://example.invalid", "k", ClientOptions{})
	require.NoError(t, err)
	_, err = client.CreateSessionResource(context.Background(), "../x", "f", "a")
	require.Error(t, err)
}

func TestSessionResourceUsesReturnedActualPath(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resource := map[string]any{"id": "rsrc-1", "type": "file", "file_id": "registered-file", "mount_path": "file:///mnt/session/uploads/a.csv"}
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{resource}})
			return
		}
		_ = json.NewEncoder(w).Encode(resource)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "k", ClientOptions{})
	require.NoError(t, err)
	created, err := client.CreateSessionResource(context.Background(), "sesn-1", "original-file", "a.csv")
	require.NoError(t, err)
	listed, err := client.ListSessionResources(context.Background(), "sesn-1")
	require.NoError(t, err)
	require.Equal(t, core.SessionResource{ID: "rsrc-1", FileID: "registered-file", MountPath: "a.csv", ActualPath: "file:///mnt/session/uploads/a.csv"}, created)
	require.Equal(t, []core.SessionResource{created}, listed)
}

func TestSessionResourceOwningConsolePaths(t *testing.T) {
	// 字面值来自 Console resource_service.go 的 resourceMountPathPrefix；converter 原样返回。
	for _, actual := range []string{"/mnt/session/uploads/a.csv", "file:///mnt/session/uploads/a.csv"} {
		t.Run(actual, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				resource := map[string]any{"id": "resource", "type": "file", "file_id": "registered", "mount_path": actual}
				if r.Method == http.MethodGet {
					_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{map[string]any{"id": "unrelated", "type": "file", "file_id": "existing", "mount_path": "/mnt/session/uploads/data/input.csv"}, resource}})
					return
				}
				_ = json.NewEncoder(w).Encode(resource)
			}))
			defer server.Close()
			client, err := NewClient(server.URL, "k", ClientOptions{})
			require.NoError(t, err)
			created, err := client.CreateSessionResource(context.Background(), "sesn-1", "original", "a.csv")
			require.NoError(t, err)
			require.Equal(t, actual, created.ActualPath)
			require.Equal(t, "a.csv", created.MountPath)
			listed, err := client.ListSessionResources(context.Background(), "sesn-1")
			require.NoError(t, err, "unrelated legal nested paths must not prevent reconciliation")
			require.Len(t, listed, 2)
			require.Equal(t, "data/input.csv", listed[0].MountPath)
			require.Equal(t, created, listed[1])
		})
	}
}

func TestCreateSessionResourceRequiresFlatInput(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls++ }))
	defer server.Close()
	client, err := NewClient(server.URL, "k", ClientOptions{})
	require.NoError(t, err)
	for _, name := range []string{"data/input.csv", "../a.csv", "/mnt/session/uploads/a.csv", "a\\b.csv"} {
		_, err = client.CreateSessionResource(context.Background(), "sesn-1", "file", name)
		require.Error(t, err)
	}
	require.Zero(t, calls)
}

func TestGetSessionProjectsFrozenEnvironmentType(t *testing.T) {
	// ToSessionResponse.environment 来源为 ark_sessions.environment，不是可变 Environment 主表。
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"id":"sesn-legacy","environment_id":"env","environment":{"id":"env","config":{"type":"self_hosted"}},"type":"session","status":"idle","agent":{"id":"agent","version":1},"created_at":"2026-10-13T00:00:00Z","updated_at":"2026-10-13T00:00:00Z"}`))
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "k", ClientOptions{})
	require.NoError(t, err)
	session, err := client.GetSession(context.Background(), "sesn-legacy")
	require.NoError(t, err)
	wire, err := json.Marshal(session)
	require.NoError(t, err)
	require.Contains(t, string(wire), `"environment_type":"self_hosted"`)
}
