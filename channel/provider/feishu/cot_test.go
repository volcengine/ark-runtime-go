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

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestCreateCOTSetsHiddenAndFrozenReplyTarget(t *testing.T) {
	var creates int
	var events []string
	var completed string
	p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal") {
			tokenResponse(w)
			return
		}
		require.NotEmpty(t, r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodPost:
			if strings.Contains(r.URL.Path, "/complete/") {
				require.Equal(t, "/open-apis/im/v1/message_cot/complete/cot-one", r.URL.Path)
				require.Equal(t, "message-one", r.URL.Query().Get("message_id"))
				completed = r.URL.Query().Get("reason")
				_, _ = io.WriteString(w, `{"code":0,"data":{}}`)
				return
			}
			require.Equal(t, "/open-apis/im/v1/message_cot", r.URL.Path)
			require.Equal(t, "chat_id", r.URL.Query().Get("receive_id_type"))
			var body map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Equal(t, true, body["cot_hidden"])
			require.Equal(t, "chat-one", body["receive_id"])
			require.Equal(t, "original-one", body["origin_message_id"])
			require.Equal(t, true, body["reply_in_thread"])
			require.NotContains(t, body, "uuid", "COT does not document ordinary IM UUID deduplication")
			creates++
			_, _ = io.WriteString(w, `{"cot_id":"cot-one","message_id":"message-one"}`)
		case http.MethodPut:
			var body struct {
				CotID     string `json:"cot_id"`
				MessageID string `json:"message_id"`
				Events    []struct {
					Type    string `json:"event_type"`
					Content string `json:"content"`
				} `json:"events"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Equal(t, "cot-one", body.CotID)
			require.Equal(t, "message-one", body.MessageID)
			require.LessOrEqual(t, len(body.Events), 50)
			for _, event := range body.Events {
				require.True(t, json.Valid([]byte(event.Content)))
				require.LessOrEqual(t, len(event.Content), 4096)
				require.NotContains(t, event.Content, "private arguments")
				events = append(events, event.Type)
			}
			_, _ = io.WriteString(w, `{"code":0}`)
		default:
			t.Errorf("unexpected COT method %s", r.Method)
		}
	})
	port, ok := any(p).(core.DurableProgressReporter)
	require.True(t, ok, "native COT must expose a durable lifecycle port")
	update := core.ProgressUpdate{TurnID: "turn-one", ReplyTarget: core.ReplyTarget{ChatID: "chat-one", MessageID: "original-one", Mode: "thread"}, State: core.ProgressRunning, Steps: []core.ProgressStep{{EventID: "tool-one", Name: "read"}}}
	checkpoint, err := port.ReportDurableProgress(context.Background(), core.ProgressCheckpoint{}, update)
	require.NoError(t, err)
	require.NotEmpty(t, checkpoint.Handle)
	checkpoint = core.ProgressCheckpoint{Handle: checkpoint.Handle, Cursor: checkpoint.Cursor}
	update.State = core.ProgressDone
	_, err = port.ReportDurableProgress(context.Background(), checkpoint, update)
	require.NoError(t, err)
	require.Equal(t, 1, creates)
	require.Equal(t, "done", completed)
	require.Contains(t, events, "RUN_STARTED")
	require.Contains(t, events, "RUN_FINISHED")
}

func TestCOTMapsRunLifecycleAndNeverUsesExternalTimeout(t *testing.T) {
	for _, tc := range []struct {
		state                core.ProgressState
		kind, status, reason string
	}{
		{core.ProgressDone, "RUN_FINISHED", "done", "done"},
		{core.ProgressFailed, "RUN_ERROR", "failed", "error"},
		{core.ProgressStopped, "RUN_FINISHED", "interrupted", "error"},
	} {
		t.Run(string(tc.state), func(t *testing.T) {
			var calls []string
			var terminalType string
			var terminalContent map[string]any
			p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal") {
					tokenResponse(w)
					return
				}
				calls = append(calls, r.Method)
				if r.Method == http.MethodPut {
					var body struct {
						Events []cotEvent `json:"events"`
					}
					require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
					require.NotEmpty(t, body.Events)
					terminal := body.Events[len(body.Events)-1]
					terminalType = terminal.Type
					require.NoError(t, json.Unmarshal([]byte(terminal.Content), &terminalContent))
				} else if strings.Contains(r.URL.Path, "/complete/") {
					require.Equal(t, tc.reason, r.URL.Query().Get("reason"))
					require.NotEqual(t, "timeout", r.URL.Query().Get("reason"))
				}
				_, _ = io.WriteString(w, `{"code":0}`)
			})
			port, ok := any(p).(core.DurableProgressReporter)
			require.True(t, ok)
			_, err := port.ReportDurableProgress(context.Background(), core.ProgressCheckpoint{Handle: `{"cot_id":"cot","message_id":"message"}`}, core.ProgressUpdate{TurnID: "turn", SessionID: "session", State: tc.state, Summary: "summary"})
			require.NoError(t, err)
			require.Equal(t, []string{http.MethodPut, http.MethodPost}, calls)
			require.Equal(t, tc.kind, terminalType)
			require.Equal(t, tc.status, terminalContent["status"])
			require.Equal(t, "turn", terminalContent["runId"])
			require.Equal(t, "session", terminalContent["threadId"])
			if tc.state == core.ProgressFailed {
				require.Equal(t, map[string]any{"message": "summary"}, terminalContent["error"])
			} else {
				require.NotContains(t, terminalContent, "error")
			}
		})
	}
}

func TestCOTReconnectWaitsForConfirmedPrefix(t *testing.T) {
	cursor := cotCursor{Tools: map[string]string{}}
	u := core.ProgressUpdate{TurnID: "turn", State: core.ProgressRunning, TextID: "same-event", Text: "hello world"}
	_, cursor = buildCOTEvents(cursor, u)
	confirmed := cursor
	u.Text = "hello"
	events, cursor := buildCOTEvents(cursor, u)
	require.Empty(t, events, "reconnection can temporarily rebuild a shorter snapshot of the same event")
	require.Equal(t, confirmed, cursor)
	u.Text = "hello world"
	events, cursor = buildCOTEvents(cursor, u)
	require.Empty(t, events, "catching up must not replay confirmed text")
	u.Text += "!"
	events, _ = buildCOTEvents(cursor, u)
	require.Len(t, events, 1)
	require.Equal(t, "TEXT_MESSAGE_CONTENT", events[0].Type)
	require.Contains(t, events[0].Content, `"delta":"!"`)
}
