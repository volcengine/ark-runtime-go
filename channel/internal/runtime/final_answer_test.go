// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFinalAnswerUsesLastNonEmptyMainMessage(t *testing.T) {
	events := []Event{
		{ID: "input", Type: "user.message"},
		displayMessage("check", "让我先检查 lark-cli。"),
		{ID: "read", Type: "agent.tool_use", ToolName: "read"},
		{ID: "read-result", Type: "agent.tool_result", ToolUseID: "read"},
		displayMessage("progress", "现在生成文档。"),
		displayMessage("final", "文档已创建：https://example.com/docx/1"),
		displayMessage("blank", "  \n"),
		{ID: "idle", Type: "session.status_idle", StopReason: "end_turn"},
	}
	outcome := judgeTurn(events, displayJob())
	require.Equal(t, "done", outcome.Status)
	require.Equal(t, "文档已创建：https://example.com/docx/1", outcome.Text)
}

func TestFinalAnswerSurvivesLateToolResults(t *testing.T) {
	const answer = "视频已完成：`/mnt/session/outputs/clip.mp4`"
	for _, tc := range []struct {
		name    string
		uses    []Event
		results []Event
	}{
		{
			name:    "local result after final answer",
			uses:    []Event{{ID: "tool", Type: "agent.tool_use"}},
			results: []Event{{ID: "result", Type: "agent.tool_result", ToolUseID: "tool"}},
		},
		{
			name:    "mcp result after final answer",
			uses:    []Event{{ID: "tool", Type: "agent.mcp_tool_use"}},
			results: []Event{{ID: "result", Type: "agent.mcp_tool_result", ToolUseID: "tool"}},
		},
		{
			name:    "failed result after final answer",
			uses:    []Event{{ID: "tool", Type: "agent.tool_use"}},
			results: []Event{{ID: "result", Type: "agent.tool_result", ToolUseID: "tool", IsError: true}},
		},
		{
			name: "concurrent results finish out of order",
			uses: []Event{{ID: "local", Type: "agent.tool_use"}, {ID: "mcp", Type: "agent.mcp_tool_use"}},
			results: []Event{
				{ID: "mcp-result", Type: "agent.mcp_tool_result", ToolUseID: "mcp"},
				{ID: "local-result", Type: "agent.tool_result", ToolUseID: "local"},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			j := displayJob()
			j.binding.DeliverArtifacts = true
			j.connection.MAClient = &sessionOutputFixture{files: []SessionOutputFile{{
				FileID: "file-video", Path: "/mnt/session/outputs/clip.mp4", MimeType: "video/mp4",
			}}}
			events := append([]Event{{ID: "input", Type: "user.message"}}, tc.uses...)
			events = append(events, displayMessage("final", answer))
			events = append(events, tc.results...)
			events = append(events, Event{ID: "idle", Type: "session.status_idle", StopReason: "end_turn"})

			outcome := judgeTurn(events, j)
			require.Equal(t, "done", outcome.Status)
			require.Equal(t, answer, outcome.Text)
			artifacts, err := (&Runtime{}).collectSessionArtifacts(t.Context(), j, outcome.Text, outcome.Artifacts)
			require.NoError(t, err)
			require.Equal(t, []Artifact{{Kind: "video", FileID: "file-video", Path: "/mnt/session/outputs/clip.mp4", FileName: "clip.mp4", MimeType: "video/mp4", SessionID: "sesn-main"}}, artifacts)
		})
	}
}

func TestFinalAnswerKeepsConservativeToolBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name   string
		before []Event
		use    Event
		after  []Event
	}{
		{
			name:  "new local call invalidates earlier text",
			use:   Event{ID: "tool", Type: "agent.tool_use"},
			after: []Event{{ID: "new-tool", Type: "agent.tool_use"}, {ID: "result", Type: "agent.tool_result", ToolUseID: "tool"}},
		},
		{
			name:  "new mcp call invalidates earlier text",
			use:   Event{ID: "tool", Type: "agent.mcp_tool_use"},
			after: []Event{{ID: "new-tool", Type: "agent.mcp_tool_use"}},
		},
		{
			name:  "unknown result invalidates earlier text",
			use:   Event{ID: "tool", Type: "agent.tool_use"},
			after: []Event{{ID: "result", Type: "agent.tool_result", ToolUseID: "unknown"}},
		},
		{
			name:  "unlinked result invalidates earlier text",
			use:   Event{ID: "tool", Type: "agent.tool_use"},
			after: []Event{{ID: "result", Type: "agent.tool_result"}},
		},
		{
			name:  "result family must match the call",
			use:   Event{ID: "tool", Type: "agent.mcp_tool_use"},
			after: []Event{{ID: "result", Type: "agent.tool_result", ToolUseID: "tool"}},
		},
		{
			name:   "call before current input is not trusted",
			before: []Event{{ID: "old-tool", Type: "agent.tool_use"}},
			use:    Event{ID: "tool", Type: "agent.tool_use"},
			after:  []Event{{ID: "result", Type: "agent.tool_result", ToolUseID: "old-tool"}},
		},
		{
			name:  "child call cannot authorize a main thread result",
			use:   Event{ID: "tool", Type: "agent.tool_use", SessionThreadID: "sthr-child"},
			after: []Event{{ID: "result", Type: "agent.tool_result", ToolUseID: "tool"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			events := append([]Event{}, tc.before...)
			events = append(events, Event{ID: "input", Type: "user.message"}, tc.use, displayMessage("early", "尚未完成"))
			events = append(events, tc.after...)
			events = append(events, Event{ID: "idle", Type: "session.status_idle", StopReason: "end_turn"})
			outcome := judgeTurn(events, displayJob())
			require.Equal(t, "done", outcome.Status)
			require.Empty(t, outcome.Text)
		})
	}
}

func TestFinalAnswerRejectsNextInputAndForeignThread(t *testing.T) {
	events := []Event{
		{ID: "input", Type: "user.message"},
		displayMessage("own", "本轮"),
		{ID: "child", Type: "agent.message", SessionThreadID: "sthr-child", Content: []TextBlock{{Type: "text", Text: "子线程"}}},
		{ID: "next", Type: "user.message"},
		displayMessage("foreign", "下一轮"),
		{ID: "idle", Type: "session.status_idle", StopReason: "end_turn"},
	}
	outcome := judgeTurn(events, displayJob())
	require.Equal(t, "unsupported_contract", outcome.Status)
	require.Empty(t, outcome.Text)
}

func TestFinalAnswerAbsentForFailureCancelAndRequiresAction(t *testing.T) {
	for _, end := range []Event{
		{ID: "failed", Type: "session.error", RetryStatus: "terminal"},
		{ID: "cancel", Type: "session.status_terminated"},
		{ID: "wait", Type: "session.status_idle", StopReason: "requires_action"},
	} {
		t.Run(end.ID, func(t *testing.T) {
			outcome := judgeTurn([]Event{{ID: "input", Type: "user.message"}, displayMessage("partial", "过程"), end}, displayJob())
			require.NotEqual(t, "done", outcome.Status)
			require.Empty(t, outcome.Text)
		})
	}
}

func TestReadWithoutFinalTextUsesTerminalStatus(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content []TextBlock
		end     Event
		status  string
		waiting bool
	}{
		{name: "empty result", end: Event{ID: "idle", Type: "session.status_idle", StopReason: "end_turn"}, status: "done"},
		{name: "text result", content: []TextBlock{{Type: "text", Text: "文档正文"}}, end: Event{ID: "idle", Type: "session.status_idle", StopReason: "end_turn"}, status: "done"},
		{name: "image preview", content: []TextBlock{{Type: "image"}}, end: Event{ID: "idle", Type: "session.status_idle", StopReason: "end_turn"}, status: "done"},
		{name: "missing terminal", content: []TextBlock{{Type: "image"}}},
		{name: "requires action", end: Event{ID: "idle", Type: "session.status_idle", StopReason: "requires_action"}, waiting: true},
		{name: "unknown stop", end: Event{ID: "idle", Type: "session.status_idle", StopReason: "future_reason"}, status: "unsupported_contract"},
		{name: "terminal error", end: Event{ID: "error", Type: "session.error", RetryStatus: "terminal"}, status: "agent_failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			events := []Event{
				{ID: "input", Type: "user.message"},
				displayMessage("before", "现在读取文件。"),
				{ID: "read", Type: "agent.tool_use", ToolName: "read"},
				{ID: "result", Type: "agent.tool_result", ToolUseID: "read", Content: tc.content},
			}
			if tc.end.ID != "" {
				events = append(events, tc.end)
			}
			outcome := judgeTurn(events, displayJob())
			require.Equal(t, tc.status, outcome.Status)
			require.Equal(t, tc.waiting, outcome.WaitingAction)
			require.Empty(t, outcome.Text, "工具前的过程文字和工具结果都不能充当最终答案")
			require.Empty(t, outcome.Artifacts, "读取预览图本身不能触发产物投递")
		})
	}
}

func TestRetryingErrorDoesNotEndTurn(t *testing.T) {
	outcome := judgeTurn([]Event{
		{ID: "input", Type: "user.message"},
		{ID: "retry", Type: "session.error", RetryStatus: "retrying"},
		displayMessage("answer", "最终答案"),
		{ID: "idle", Type: "session.status_idle", StopReason: "end_turn"},
	}, displayJob())
	require.Equal(t, "done", outcome.Status)
	require.Equal(t, "最终答案", outcome.Text)
}

func TestFinalAnswerIgnoresMessagesOutsideCurrentTurn(t *testing.T) {
	events := []Event{
		displayMessage("old", "previous private answer"),
		{ID: "input", Type: "user.message"},
		displayMessage("answer", "current answer"),
		{ID: "idle", Type: "session.status_idle", StopReason: "end_turn"},
		displayMessage("late", "later private answer"),
	}
	outcome := judgeTurn(events, displayJob())
	require.Equal(t, "done", outcome.Status)
	require.Equal(t, "current answer", outcome.Text)
}
