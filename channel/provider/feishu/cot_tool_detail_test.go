// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestCOTToolResultUsesDisplayContent(t *testing.T) {
	events, _ := buildCOTEvents(cotCursor{Tools: map[string]string{}}, core.ProgressUpdate{
		TurnID: "turn", State: core.ProgressRunning,
		Steps: []core.ProgressStep{{EventID: "tool", Name: "bash", Done: true, DurationMS: 1831}},
	})
	var kinds []string
	for _, event := range events {
		kinds = append(kinds, event.Type)
		if event.Type != "TOOL_CALL_RESULT" {
			continue
		}
		var result map[string]any
		require.NoError(t, json.Unmarshal([]byte(event.Content), &result))
		require.NotEmpty(t, result["content"], "飞书明细必须显示结果正文或无输出说明")
		require.Equal(t, "tool", result["role"])
		require.NotEmpty(t, result["messageId"])
		require.NotContains(t, result, "durationMs", "元数据不能作为展开正文")
	}
	require.Contains(t, kinds, "TOOL_CALL_END")
	require.Contains(t, kinds, "TOOL_CALL_RESULT")
}

func TestCOTToolDetailsSurviveResumeWithoutReplay(t *testing.T) {
	update := core.ProgressUpdate{TurnID: "turn", State: core.ProgressRunning, Steps: []core.ProgressStep{
		{EventID: "tool", Name: "bash", Arguments: `{"command":"printf 'hello\\n'"}`},
	}}
	events, cursor := buildCOTEvents(cotCursor{}, update)
	require.Equal(t, []string{"RUN_STARTED", "TOOL_CALL_START", "TOOL_CALL_ARGS", "TOOL_CALL_END"}, cotToolEventKinds(events))
	var args map[string]string
	require.NoError(t, json.Unmarshal([]byte(events[2].Content), &args))
	require.JSONEq(t, update.Steps[0].Arguments, args["delta"])
	update.Steps[0].Done, update.Steps[0].Output = true, "hello\nfile.md"
	events, cursor = buildCOTEvents(cursor, update)
	require.Equal(t, []string{"TOOL_CALL_RESULT"}, cotToolEventKinds(events))
	var result map[string]string
	require.NoError(t, json.Unmarshal([]byte(events[0].Content), &result))
	require.Equal(t, "tool", result["toolCallId"])
	require.Contains(t, result["content"], "hello\nfile.md")
	encoded, err := json.Marshal(cursor)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "printf")
	require.NotContains(t, string(encoded), "hello")
	var restored cotCursor
	require.NoError(t, json.Unmarshal(encoded, &restored))
	events, restored = buildCOTEvents(restored, update)
	require.Empty(t, events, "恢复后不能重复追加工具参数或输出")
	update.Steps[0].Output += "\nnew line"
	events, _ = buildCOTEvents(restored, update)
	require.Equal(t, []string{"TOOL_CALL_RESULT"}, cotToolEventKinds(events), "同一结果可以覆盖补齐")
	var replaced map[string]string
	require.NoError(t, json.Unmarshal([]byte(events[0].Content), &replaced))
	require.Equal(t, result["messageId"], replaced["messageId"])
	require.Contains(t, replaced["content"], "new line")
}

func TestCOTToolDetailsBoundEncodedContent(t *testing.T) {
	for _, text := range []string{
		strings.Repeat("中文\n", 6000),
		strings.Repeat("\"\\\n\t<&>", 6000),
	} {
		args, err := json.Marshal(map[string]string{"command": text})
		require.NoError(t, err)
		events, _ := buildCOTEvents(cotCursor{}, core.ProgressUpdate{TurnID: "turn", State: core.ProgressRunning, Steps: []core.ProgressStep{
			{EventID: strings.Repeat("a", 100), Name: "bash", Arguments: string(args), Output: text, Done: true},
		}})
		require.Equal(t, []string{"RUN_STARTED", "TOOL_CALL_START", "TOOL_CALL_ARGS", "TOOL_CALL_END", "TOOL_CALL_RESULT"}, cotToolEventKinds(events))
		for _, event := range events {
			require.True(t, json.Valid([]byte(event.Content)))
			require.True(t, utf8.ValidString(event.Content))
			require.LessOrEqual(t, len(event.Content), 4096)
			var content map[string]string
			if event.Type == "TOOL_CALL_ARGS" || event.Type == "TOOL_CALL_RESULT" {
				require.NoError(t, json.Unmarshal([]byte(event.Content), &content))
				require.Contains(t, event.Content, "已截断")
			}
			if event.Type == "TOOL_CALL_ARGS" {
				require.True(t, json.Valid([]byte(content["delta"])), "截断后的参数仍是 JSON 字符串")
			}
		}
	}
}

func TestCOTToolDetailsUpgradeOldCursorAndEmptyFailure(t *testing.T) {
	events, _ := buildCOTEvents(cotCursor{Started: true, Tools: map[string]string{"tool": "failed"}}, core.ProgressUpdate{
		TurnID: "turn", State: core.ProgressRunning, Steps: []core.ProgressStep{{EventID: "tool", Name: "bash", Failed: true}},
	})
	require.Equal(t, []string{"TOOL_CALL_END", "TOOL_CALL_RESULT"}, cotToolEventKinds(events), "旧游标不重放开始事件，但补齐可读明细")
	require.Contains(t, events[1].Content, "执行失败")
	require.Contains(t, events[1].Content, "工具未返回文本输出")
}

func cotToolEventKinds(events []cotEvent) []string {
	var kinds []string
	for _, event := range events {
		kinds = append(kinds, event.Type)
	}
	return kinds
}
