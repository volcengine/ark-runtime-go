// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package ma_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/ma"
)

func TestModelHeadersOnlyEventsAndRefresh(t *testing.T) {
	var calls atomic.Int32
	var value atomic.Value
	value.Store("Bearer first")
	received := make(chan http.Header, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received <- r.Header.Clone()
		if strings.HasSuffix(r.URL.Path, "/events") {
			_, _ = w.Write([]byte(`{"data":[]}`))
			return
		}
		_, _ = w.Write([]byte(`{"id":"s1","status":"idle","type":"session","environment_id":"env","agent":{"id":"agent","version":1},"created_at":"2026-10-13T00:00:00Z","updated_at":"2026-10-13T00:00:00Z"}`))
	}))
	defer server.Close()
	source := map[string]string{"Authorization": "Bearer first"}
	client, err := ma.NewClient(server.URL, "platform-key", ma.ClientOptions{ModelHeaderSource: func(_ context.Context, scope ma.RequestScope) (map[string]string, error) {
		calls.Add(1)
		require.Equal(t, ma.ScopeSession, scope.Kind)
		require.Equal(t, "s1", scope.SessionID)
		return map[string]string{"Authorization": value.Load().(string), "X-Api-Key": "model-api-key"}, nil
	}})
	require.NoError(t, err)
	_, err = client.GetSession(t.Context(), "s1")
	require.NoError(t, err)
	require.Empty(t, (<-received).Get("X-User-Model-Header-Authorization"))
	require.Zero(t, calls.Load())
	for _, key := range []string{"Bearer first", "Bearer rotated"} {
		value.Store(key)
		_, err = client.SendMessage(t.Context(), "s1", "e1", core.UserInput{Text: "hello"})
		require.NoError(t, err)
		h := <-received
		require.Equal(t, "Bearer platform-key", h.Get("Authorization"))
		require.Equal(t, key, h.Get("X-User-Model-Header-Authorization"))
		require.Equal(t, "model-api-key", h.Get("X-User-Model-Header-X-Api-Key"))
	}
	_, err = client.SendToolConfirmation(t.Context(), "s1", core.ToolConfirmation{EventID: "approval", ToolUseID: "tool", Result: "allow"})
	require.NoError(t, err)
	require.NotEmpty(t, (<-received).Get("X-User-Model-Header-Authorization"))
	_, err = client.SendInterrupt(t.Context(), "s1", "interrupt")
	require.NoError(t, err)
	require.NotEmpty(t, (<-received).Get("X-User-Model-Header-Authorization"))
	require.Equal(t, int32(4), calls.Load())
	// provider 的原 map 不允许被添加前缀或修改。
	client, err = ma.NewClient(server.URL, "platform-key", ma.ClientOptions{ModelHeaderSource: func(context.Context, ma.RequestScope) (map[string]string, error) { return source, nil }})
	require.NoError(t, err)
	_, err = client.SendInterrupt(t.Context(), "s1", "e2")
	require.NoError(t, err)
	<-received
	require.Equal(t, map[string]string{"Authorization": "Bearer first"}, source)
}

func TestModelHeadersRejectBeforeSending(t *testing.T) {
	oversized := map[string]string{}
	for i := 0; i < 33; i++ {
		oversized[fmt.Sprintf("X-Key-%d", i)] = "value"
	}
	cases := []struct {
		name    string
		headers map[string]string
		err     error
	}{
		{"provider failure", nil, errors.New("private-model-key")},
		{"duplicate", map[string]string{"Authorization": "a", "authorization": "b"}, nil},
		{"prefixed", map[string]string{"X-User-Model-Header-Authorization": "a"}, nil},
		{"forbidden", map[string]string{"X-Ark-Env": "a"}, nil},
		{"cookie", map[string]string{"Cookie": "a"}, nil},
		{"invalid name", map[string]string{"Bad Name": "a"}, nil},
		{"empty value", map[string]string{"Authorization": ""}, nil},
		{"control", map[string]string{"Authorization": "private-model-key\r\nInjected: a"}, nil},
		{"long name", map[string]string{strings.Repeat("x", 65): "a"}, nil},
		{"long value", map[string]string{"Authorization": strings.Repeat("a", 4097)}, nil},
		{"too many", oversized, nil},
		{"total bytes", map[string]string{"A": strings.Repeat("a", 4096), "B": strings.Repeat("a", 4096), "C": strings.Repeat("a", 4096), "D": strings.Repeat("a", 4096)}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var sent atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { sent.Add(1) }))
			defer server.Close()
			client, err := ma.NewClient(server.URL, "platform", ma.ClientOptions{ModelHeaderSource: func(context.Context, ma.RequestScope) (map[string]string, error) { return tc.headers, tc.err }})
			require.NoError(t, err)
			_, err = client.SendMessage(t.Context(), "s1", "e1", core.UserInput{Text: "hello"})
			require.Error(t, err)
			require.NotContains(t, err.Error(), "private-model-key")
			var unsent interface{ RequestNotSent() bool }
			require.ErrorAs(t, err, &unsent)
			require.True(t, unsent.RequestNotSent())
			require.Zero(t, sent.Load())
		})
	}
}

func TestModelHeadersShareRequestBudget(t *testing.T) {
	client, err := ma.NewClient("http://fixture.invalid", "platform", ma.ClientOptions{RequestTimeout: 10 * time.Millisecond, ModelHeaderSource: func(ctx context.Context, _ ma.RequestScope) (map[string]string, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}})
	require.NoError(t, err)
	_, err = client.SendInterrupt(t.Context(), "s1", "e1")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	var unsent interface{ RequestNotSent() bool }
	require.ErrorAs(t, err, &unsent)
}

func TestModelHeadersConcurrentSessionIsolation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.Split(r.URL.Path, "/")[4]
		require.Equal(t, "key-"+id, r.Header.Get("X-User-Model-Header-X-Api-Key"))
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	defer server.Close()
	client, err := ma.NewClient(server.URL, "platform", ma.ClientOptions{ModelHeaderSource: func(_ context.Context, s ma.RequestScope) (map[string]string, error) {
		return map[string]string{"X-Api-Key": "key-" + s.SessionID}, nil
	}})
	require.NoError(t, err)
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := client.SendInterrupt(t.Context(), fmt.Sprintf("s%d", i), "e1")
			require.NoError(t, err)
		}()
	}
	wg.Wait()
}

func TestModelHeadersNeverReachFilesStreamsOrCreation(t *testing.T) {
	var calls atomic.Int32
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Empty(t, r.Header.Get("X-User-Model-Header-Authorization"))
		switch r.URL.Path {
		case "/api/v3/files/file-1":
			_, _ = fmt.Fprintf(w, `{"id":"file-1","status":"active","download_url":%q}`, server.URL+"/object")
		case "/object":
			require.Empty(t, r.Header.Get("Authorization"))
			_, _ = w.Write([]byte("file"))
		case "/api/v3/sessions":
			_, _ = w.Write([]byte(`{"id":"s1","status":"idle","type":"session","environment_id":"env","agent":{"id":"agent","version":1},"created_at":"2026-10-13T00:00:00Z","updated_at":"2026-10-13T00:00:00Z"}`))
		default:
			_, _ = w.Write([]byte(`{"data":[]}`))
		}
	}))
	defer server.Close()
	client, err := ma.NewClient(server.URL, "platform", ma.ClientOptions{ModelHeaderSource: func(context.Context, ma.RequestScope) (map[string]string, error) {
		calls.Add(1)
		return nil, errors.New("must not resolve")
	}})
	require.NoError(t, err)
	_, err = client.CreateSession(t.Context(), core.CreateSessionSpec{Binding: core.Binding{AgentID: "agent-1", AgentVersion: "1", EnvironmentID: "env-1"}, CreationKey: "create-1"})
	require.NoError(t, err)
	_, err = client.ListEvents(t.Context(), "s1", core.EventQuery{})
	require.NoError(t, err)
	stream, err := client.StreamEvents(t.Context(), "s1")
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	file, err := client.DownloadFile(t.Context(), "file-1", 1024)
	require.NoError(t, err)
	require.NoError(t, file.Body.Close())
	require.Zero(t, calls.Load())
}
