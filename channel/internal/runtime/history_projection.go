// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"encoding/json"
	"unsafe"
)

// compactHistoryEvent 仅在历史扫描中释放无消费者的原文；正文、工具参数和判定字段保持完整。
func compactHistoryEvent(event Event) Event {
	if event.Type != "agent.tool_result" && event.Type != "agent.mcp_tool_result" {
		event.Raw = nil
		return event
	}
	artifacts := fileBlocks(event.Raw)
	event.Raw = nil
	if len(artifacts) == 0 {
		return event
	}
	type fileSource struct {
		Type   string `json:"type"`
		FileID string `json:"file_id"`
	}
	type fileBlock struct {
		Type   string     `json:"type"`
		Title  string     `json:"title,omitempty"`
		Source fileSource `json:"source"`
	}
	wire := struct {
		Content []fileBlock `json:"content"`
	}{}
	for _, artifact := range artifacts {
		wire.Content = append(wire.Content, fileBlock{Type: artifact.Kind, Title: artifact.FileName, Source: fileSource{Type: "file", FileID: artifact.FileID}})
	}
	// 历史扫描只保留交付所需的附件引用，释放内联媒体等无消费者的原文。
	event.Raw, _ = json.Marshal(wire)
	return event
}

// historyEventBytes 估算保留字段及集合容量，避免内联媒体误占预算或空元素绕过内存防护。
func historyEventBytes(event Event) int {
	// Sizeof 仅用于编译期布局计费，不读取或转换指针；Event 在列表和去重索引中各保留一份。
	size := 2*int(unsafe.Sizeof(event)) + cap(event.Raw) + len(event.ID) + len(event.Type) + len(event.SessionThreadID) +
		len(event.StopReason) + len(event.RetryStatus) + len(event.ToolName) + len(event.MCPServerName) +
		len(event.ToolUseID) + len(event.Permission) + len(event.ToolInput)
	size += cap(event.Content)*int(unsafe.Sizeof(TextBlock{})) + cap(event.ActionEventIDs)*int(unsafe.Sizeof(""))
	for _, block := range event.Content {
		size += len(block.Type) + len(block.Text)
	}
	for _, id := range event.ActionEventIDs {
		size += len(id)
	}

	if event.Preview != nil {
		size += int(unsafe.Sizeof(*event.Preview)) + len(event.Preview.EventID) + len(event.Preview.EventType) + len(event.Preview.Text)
	}
	if event.ProcessedAt != nil {
		size += int(unsafe.Sizeof(*event.ProcessedAt))
	}
	return size
}
