// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"time"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

type cotCursor struct {
	Started      bool                     `json:"started"`
	TextID       string                   `json:"text_id,omitempty"`
	SourceTextID string                   `json:"source_text_id,omitempty"`
	Bytes        int                      `json:"bytes,omitempty"`
	Digest       string                   `json:"digest,omitempty"`
	Tools        map[string]string        `json:"tools,omitempty"`
	ToolDetails  map[string]cotToolCursor `json:"tool_details,omitempty"`
}

func cotDigest(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])
}

// ReportDurableProgress 使用原生 COT 生命周期；返回非密游标，即使远端结果未知也保留已知句柄。
func (p *Provider) ReportDurableProgress(ctx context.Context, checkpoint core.ProgressCheckpoint, update core.ProgressUpdate) (core.ProgressCheckpoint, error) {
	if ctx == nil || p.client == nil || p.httpClient == nil || p.cotShared == nil || update.TurnID == "" ||
		update.State != core.ProgressRunning && update.State != core.ProgressDone && update.State != core.ProgressFailed && update.State != core.ProgressStopped {
		return checkpoint, failure{"invalid cot progress", false, false, "invalid_cot_request"}
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var handle cotHandle
	var err error
	if checkpoint.Handle == "" {
		if update.State != core.ProgressRunning || update.TurnID == "" || update.ReplyTarget.ChatID == "" {
			return checkpoint, failure{"cot creation requires running turn", false, false, "invalid_cot_request"}
		}
		raw, e := p.cotRequest(ctx, http.MethodPost, "/open-apis/im/v1/message_cot", url.Values{"receive_id_type": {"chat_id"}}, struct {
			ReceiveID       string `json:"receive_id"`
			OriginMessageID string `json:"origin_message_id,omitempty"`
			ReplyInThread   bool   `json:"reply_in_thread"`
			Hidden          bool   `json:"cot_hidden"`
		}{update.ReplyTarget.ChatID, update.ReplyTarget.MessageID, update.ReplyTarget.Mode == "thread", true})
		if e != nil {
			return checkpoint, e
		}
		var response struct {
			CotID     string    `json:"cot_id"`
			MessageID string    `json:"message_id"`
			Data      cotHandle `json:"data"`
		}
		if json.Unmarshal(raw, &response) != nil {
			return checkpoint, failure{"cot creation unknown", true, false, "cot_response_unknown"}
		}
		handle = cotHandle{response.CotID, response.MessageID}
		if handle.CotID == "" {
			handle = response.Data
		}
		encoded, _ := json.Marshal(handle)
		if _, e = parseCOTHandle(string(encoded)); e != nil {
			return checkpoint, failure{"cot creation unknown", true, false, "cot_response_unknown"}
		}
		checkpoint.Handle = string(encoded)
	} else if handle, err = parseCOTHandle(checkpoint.Handle); err != nil {
		return checkpoint, err
	}
	var cursor cotCursor
	if checkpoint.Cursor != "" && (len(checkpoint.Cursor) > 16384 || json.Unmarshal([]byte(checkpoint.Cursor), &cursor) != nil) {
		return checkpoint, failure{"invalid cot cursor", false, false, "invalid_cot_cursor"}
	}
	if cursor.Tools == nil {
		cursor.Tools = map[string]string{}
	}
	if update.State != core.ProgressRunning && (checkpoint.Failed || checkpoint.Attempted) {
		// 未确认的 Append 可能已经受理，不重放 RUN_STARTED 或文本闭合片段。
		cursor.Started, cursor.TextID = true, ""
	}
	events, next := buildCOTEvents(cursor, update)
	for len(events) > 0 {
		count := min(50, len(events))
		_, err = p.cotRequest(ctx, http.MethodPut, "/open-apis/im/v1/message_cot", nil, struct {
			CotID     string     `json:"cot_id"`
			MessageID string     `json:"message_id"`
			Events    []cotEvent `json:"events"`
		}{handle.CotID, handle.MessageID, events[:count]})
		if err != nil {
			return checkpoint, err
		}
		events = events[count:]
	}
	if update.State != core.ProgressRunning {
		reason := "done"
		if update.State != core.ProgressDone {
			// 主动停止的展示由 RUN_FINISHED/interrupted 表达，Complete 仍按外部协议传 error。
			reason = "error"
		}
		_, err = p.cotRequest(ctx, http.MethodPost, "/open-apis/im/v1/message_cot/complete/"+handle.CotID, url.Values{"message_id": {handle.MessageID}, "reason": {reason}}, nil)
		if err != nil {
			return checkpoint, err
		}
	}
	encoded, _ := json.Marshal(next)
	checkpoint.Cursor = string(encoded)
	return checkpoint, nil
}

func buildCOTEvents(cursor cotCursor, update core.ProgressUpdate) ([]cotEvent, cotCursor) {
	var events []cotEvent
	at := update.At
	if at.IsZero() {
		at = time.Now()
	}
	add := func(kind string, content any) {
		raw, err := json.Marshal(content)
		if err == nil && len(raw) <= 4096 {
			events = append(events, cotEvent{kind, string(raw), at.UnixMilli()})
		}
	}
	if !cursor.Started {
		add("RUN_STARTED", map[string]string{"threadId": update.SessionID, "runId": update.TurnID})
		cursor.Started = true
	}
	appendCOTToolEvents(&cursor, update.Steps, add)
	sourceID := cotDigest(update.TextID)
	// 同一事件在重连后可能仅重建出较短前缀；等待追平，不重复新建文本段。
	rebuilding := cursor.SourceTextID == sourceID && cursor.Bytes > len(update.Text)
	if update.State == core.ProgressRunning && update.TextID != "" && update.Text != "" && !rebuilding {
		text := update.Text
		if len([]rune(text)) > 16000 {
			text = string([]rune(text)[:16000])
		}
		same := cursor.SourceTextID == sourceID && cursor.Bytes <= len(text) && cursor.Bytes >= 0 && cotDigest(text[:cursor.Bytes]) == cursor.Digest
		if !same {
			if cursor.TextID != "" {
				add("TEXT_MESSAGE_END", map[string]string{"messageId": cursor.TextID})
			}
			cursor.TextID, cursor.SourceTextID, cursor.Bytes = "message-"+cotDigest(update.TextID + text)[:24], sourceID, 0
			add("TEXT_MESSAGE_START", map[string]string{"messageId": cursor.TextID, "role": "assistant"})
		}
		remaining := []rune(text[cursor.Bytes:])
		for len(remaining) > 0 {
			count := min(384, len(remaining))
			add("TEXT_MESSAGE_CONTENT", map[string]any{"messageId": cursor.TextID, "delta": string(remaining[:count]), "index": 0})
			remaining = remaining[count:]
		}
		cursor.Bytes, cursor.Digest = len(text), cotDigest(text)
	}
	if update.State != core.ProgressRunning {
		if cursor.TextID != "" {
			add("TEXT_MESSAGE_END", map[string]string{"messageId": cursor.TextID})
		}
		kind, status := "RUN_FINISHED", "done"
		switch update.State {
		case core.ProgressStopped:
			status = "interrupted"
		case core.ProgressFailed:
			kind, status = "RUN_ERROR", "failed"
		}
		content := map[string]any{"threadId": update.SessionID, "runId": update.TurnID, "status": status}
		if update.State == core.ProgressFailed {
			content["error"] = map[string]string{"message": progressText(strings.TrimSpace(update.Summary), 200)}
		}
		add(kind, content)
	}
	return events, cursor
}
