// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package ma

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/arkruntime/model/session"
)

func TestTypedSDKEventsPreserveChannelFields(t *testing.T) {
	var message session.ManagedAgentsAgentMessageEvent
	require.NoError(t, json.Unmarshal([]byte(`{"id":"message-1","type":"agent.message","session_thread_id":"sthr-1","processed_at":"2026-11-13T00:00:00Z","content":[{"type":"text","text":"answer"}]}`), &message))
	event, err := projectSDKEvent(&message)
	require.NoError(t, err)
	require.Equal(t, "message-1", event.ID)
	require.Equal(t, "sthr-1", event.SessionThreadID)
	require.Equal(t, "answer", event.Content[0].Text)
	require.NotNil(t, event.ProcessedAt)
	var tool session.ManagedAgentsAgentToolUseEvent
	require.NoError(t, json.Unmarshal([]byte(`{"id":"tool-1","type":"agent.tool_use","tool_use_id":"call-1","name":"shell","input":{"command":"pwd"},"evaluated_permission":"ask"}`), &tool))
	event, err = projectSDKEvent(&tool)
	require.NoError(t, err)
	require.Equal(t, "shell", event.ToolName)
	require.Equal(t, "call-1", event.ToolUseID)
	require.Equal(t, `{"command":"pwd"}`, event.ToolInput)
	require.Equal(t, "ask", event.Permission)
	var idle session.ManagedAgentsSessionStatusIdleEvent
	require.NoError(t, json.Unmarshal([]byte(`{"type":"session.status_idle","stop_reason":{"type":"requires_action","event_ids":["tool-1"]}}`), &idle))
	event, err = projectSDKEvent(&idle)
	require.NoError(t, err)
	require.Equal(t, "requires_action", event.StopReason)
	require.Equal(t, []string{"tool-1"}, event.ActionEventIDs)
}

func TestTypedSDKSpanKeepsThreadOwnership(t *testing.T) {
	var span session.ManagedAgentsSpanModelRequestStartEvent
	require.NoError(t, json.Unmarshal([]byte(`{"id":"span-1","type":"span.model_request_start","session_thread_id":"sthr-child"}`), &span))
	event, err := projectSDKEvent(&span)
	require.NoError(t, err)
	require.Equal(t, "sthr-child", event.SessionThreadID)
}

func TestTypedToolResultsMapStatusAndFileContent(t *testing.T) {
	for _, tc := range []struct {
		kind  string
		value session.ManagedAgentsSessionEvent
	}{
		{"agent.tool_result", &session.ManagedAgentsAgentToolResultEvent{}},
		{"agent.mcp_tool_result", &session.ManagedAgentsAgentMCPToolResultEvent{}},
	} {
		for _, failed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/error=%t", tc.kind, failed), func(t *testing.T) {
				raw := []byte(fmt.Sprintf(`{"id":"result-1","type":%q,"tool_use_id":"tool-1","is_error":%t,"content":[{"type":"document","title":"report.pdf","source":{"type":"file","file_id":"file-report"}}]}`, tc.kind, failed))
				require.NoError(t, json.Unmarshal(raw, tc.value))
				event, err := projectSDKEvent(tc.value)
				require.NoError(t, err)
				require.Equal(t, tc.kind, event.Type)
				require.Equal(t, failed, event.IsError)
				require.Equal(t, "tool-1", event.ToolUseID)
				require.Contains(t, string(event.Raw), `"file_id":"file-report"`)
			})
		}
	}
}
