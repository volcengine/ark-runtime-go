// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"encoding/json"
	"fmt"
	"strings"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// cotToolCursor 只保存发送状态和摘要，不持久化工具参数或结果正文。
type cotToolCursor struct {
	Arguments bool   `json:"arguments,omitempty"`
	Ended     bool   `json:"ended,omitempty"`
	Result    string `json:"result,omitempty"`
}

func appendCOTToolEvents(cursor *cotCursor, steps []core.ProgressStep, add func(string, any)) {
	if cursor.Tools == nil {
		cursor.Tools = map[string]string{}
	}
	if cursor.ToolDetails == nil {
		cursor.ToolDetails = map[string]cotToolCursor{}
	}
	if len(steps) > maxProgressSteps {
		steps = steps[len(steps)-maxProgressSteps:]
	}
	visible := map[string]bool{}
	for _, step := range steps {
		visible[cotToolID(step.EventID)] = true
	}
	for id := range cursor.Tools {
		if !visible[id] {
			delete(cursor.Tools, id)
			delete(cursor.ToolDetails, id)
		}
	}
	for _, step := range steps {
		id := cotToolID(step.EventID)
		if cursor.Tools[id] == "" {
			add("TOOL_CALL_START", map[string]string{"toolCallId": id, "toolCallName": progressText(step.Name, maxStepName), "title": progressText(step.Name, maxStepName)})
		}
		status := "running"
		if step.Done || step.Failed {
			status = "completed"
			if step.Failed {
				status = "failed"
			}
		}
		detail := cursor.ToolDetails[id]
		if !detail.Ended && (step.Arguments != "" || status != "running") {
			if !detail.Arguments && step.Arguments != "" {
				add("TOOL_CALL_ARGS", map[string]string{"toolCallId": id, "delta": cotToolArguments(id, step.Arguments)})
				detail.Arguments = true
			}
			add("TOOL_CALL_END", map[string]string{"toolCallId": id})
			detail.Ended = true
		}
		if status != "running" {
			result := cotToolResult(id, step)
			digest := cotDigest(result["content"])
			if detail.Result != digest {
				add("TOOL_CALL_RESULT", result)
				detail.Result = digest
			}
		}
		cursor.Tools[id], cursor.ToolDetails[id] = status, detail
	}
}

func cotToolID(id string) string {
	if validCOTID(id) {
		return id
	}
	return "tool-" + cotDigest(id)[:16]
}

func cotToolArguments(id, arguments string) string {
	if !json.Valid([]byte(arguments)) {
		encoded, _ := json.Marshal(map[string]string{"input": arguments})
		arguments = string(encoded)
	}
	wrap := func(delta string) any { return map[string]string{"toolCallId": id, "delta": delta} }
	if cotToolContentFits(wrap(arguments)) {
		return arguments
	}
	preview := boundedCOTToolText(arguments, func(text string) any {
		encoded, _ := json.Marshal(map[string]string{"preview": text})
		return wrap(string(encoded))
	})
	encoded, _ := json.Marshal(map[string]string{"preview": preview})
	return string(encoded)
}

func cotToolResult(id string, step core.ProgressStep) map[string]string {
	status := "已完成"
	if step.Failed {
		status = "执行失败"
	}
	if step.DurationMS > 0 {
		status += fmt.Sprintf("（耗时 %d ms）", step.DurationMS)
	}
	output := strings.TrimSpace(step.Output)
	if output == "" {
		output = "工具未返回文本输出"
	}
	result := map[string]string{"toolCallId": id, "messageId": "tool-result-" + cotDigest(id)[:24], "role": "tool"}
	result["content"] = boundedCOTToolText(status+"\n\n"+output, func(text string) any {
		result["content"] = text
		return result
	})
	return result
}

func cotToolContentFits(content any) bool {
	encoded, err := json.Marshal(content)
	return err == nil && len(encoded) <= 4096
}

// boundedCOTToolText 按事件编码后的实际字节数截断，避免中文和双重 JSON 转义导致事件被丢弃。
func boundedCOTToolText(text string, wrap func(string) any) string {
	if cotToolContentFits(wrap(text)) {
		return text
	}
	const suffix = "\n…（已截断）"
	runes := []rune(text)
	low, high := 0, len(runes)
	for low < high {
		middle := (low + high + 1) / 2
		if cotToolContentFits(wrap(string(runes[:middle]) + suffix)) {
			low = middle
		} else {
			high = middle - 1
		}
	}
	return string(runes[:low]) + suffix
}
