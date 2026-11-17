// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package ma_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/ma"
)

func TestStreamPreviewOptsInAgentMessage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v3/sessions/sesn-main/threads/sthr-main/stream", r.URL.Path)
		require.Equal(t, []string{"agent.message", "agent.thinking"}, r.URL.Query()["event_deltas"])
		require.Equal(t, "Bearer test", r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"type\":\"event_start\",\"event\":{\"id\":\"message\",\"type\":\"agent.message\"}}\n\n")
		_, _ = io.WriteString(w, "data: {\"type\":\"event_delta\",\"event_id\":\"message\",\"delta\":{\"type\":\"content_delta\",\"index\":1,\"content\":{\"type\":\"text\",\"text\":\"增量\"}}}\n\n")
	}))
	defer server.Close()
	client, err := ma.NewClient(server.URL, "test", ma.ClientOptions{})
	require.NoError(t, err)
	preview, ok := any(client).(interface {
		StreamEventPreviews(context.Context, string) (core.EventStream, error)
	})
	require.True(t, ok, "preview streaming must be an optional MA client port")
	stream, err := preview.StreamEventPreviews(context.Background(), "sesn-main")
	require.NoError(t, err)
	defer func() { require.NoError(t, stream.Close()) }()
	for _, kind := range []string{"event_start", "event_delta"} {
		event, err := stream.Recv(context.Background())
		require.NoError(t, err)
		require.Equal(t, kind, event.Type)
		require.Equal(t, "sthr-main", event.SessionThreadID)
		require.True(t, event.Preview.Live, "the MA thread stream is live pubsub, including deltas without start")
		encoded, err := json.Marshal(event)
		require.NoError(t, err)
		require.Contains(t, string(encoded), "\"preview\"")
		require.Contains(t, string(encoded), "\"event_id\":\"message\"")
	}
	_, err = stream.Recv(context.Background())
	require.ErrorIs(t, err, io.EOF)
}

func TestStreamPreviewRejectsConflictingThread(t *testing.T) {
	for _, frame := range []string{
		`{"type":"event_start","session_thread_id":"sthr-child","event":{"id":"message","type":"agent.message"}}`,
		`{"type":"event_delta","session_thread_id":"sthr-child","event_id":"message","delta":{"type":"content_delta","content":{"type":"text","text":"private"}}}`,
	} {
		t.Run(frame, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: "+frame+"\n\n")
			}))
			defer server.Close()
			client, err := ma.NewClient(server.URL, "test", ma.ClientOptions{})
			require.NoError(t, err)
			stream, err := client.StreamEventPreviews(t.Context(), "sesn-main")
			require.NoError(t, err)
			defer func() { require.NoError(t, stream.Close()) }()
			_, err = stream.Recv(t.Context())
			require.Error(t, err, "explicit conflicting thread cannot be overwritten by subscription scope")
		})
	}
}

func TestStreamPreviewRejectsUnknownPrimaryBeforeHTTP(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
	}))
	defer server.Close()
	client, err := ma.NewClient(server.URL, "test", ma.ClientOptions{})
	require.NoError(t, err)
	for _, id := range []string{"", "session-legacy", "sesn-", "sesn-main/child", "sesn-main\\child"} {
		stream, err := client.StreamEventPreviews(t.Context(), id)
		if stream != nil {
			require.NoError(t, stream.Close())
		}
		require.Error(t, err, id)
	}
	require.Zero(t, calls.Load())
}

func TestSessionStreamDoesNotInferPreviewThread(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v3/sessions/sesn-main/events/stream", r.URL.Path)
		require.Empty(t, r.URL.Query()["event_deltas"])
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"type\":\"event_start\",\"event\":{\"id\":\"message\",\"type\":\"agent.message\"}}\n\n")
	}))
	defer server.Close()
	client, err := ma.NewClient(server.URL, "test", ma.ClientOptions{})
	require.NoError(t, err)
	stream, err := client.StreamEvents(t.Context(), "sesn-main")
	require.NoError(t, err)
	defer func() { require.NoError(t, stream.Close()) }()
	event, err := stream.Recv(t.Context())
	require.NoError(t, err)
	require.Empty(t, event.SessionThreadID, "session scope does not prove a preview belongs to the primary thread")
}
