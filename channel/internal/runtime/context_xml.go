// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"encoding/xml"
	"strings"
	"time"
)

// xmlText 去除 XML 1.0 不接受的控制字符，再编码文本与属性边界。
func xmlText(value string) string {
	value = strings.Map(func(r rune) rune {
		if r == '\t' || r == '\n' || r == '\r' || r >= 0x20 && r <= 0xd7ff || r >= 0xe000 && r <= 0xfffd || r >= 0x10000 && r <= 0x10ffff {
			return r
		}
		return -1
	}, value)
	var result strings.Builder
	_ = xml.EscapeText(&result, []byte(value))
	return result.String()
}

func xmlAttr(out *strings.Builder, name, value string) {
	if value == "" {
		return
	}
	out.WriteByte(' ')
	out.WriteString(name)
	out.WriteString(`="`)
	out.WriteString(xmlText(value))
	out.WriteByte('"')
}

func historyMessageXML(message ContextMessage, senderID string, truncated bool) string {
	var out strings.Builder
	out.WriteString("<Message")
	xmlAttr(&out, "id", message.MessageID)
	xmlAttr(&out, "sender_id", message.SenderID)
	xmlAttr(&out, "sender_id_type", message.SenderIDType)
	role := "user"
	if message.SenderIsBot {
		role = "bot"
	}
	xmlAttr(&out, "sender_role", role)
	if !message.SenderIsBot && message.SenderID == senderID && message.SenderIDType == "open_id" {
		xmlAttr(&out, "sender_relation", "current_user")
	}
	if !message.At.IsZero() {
		xmlAttr(&out, "sent_at", message.At.UTC().Format(time.RFC3339Nano))
	}
	if truncated {
		xmlAttr(&out, "content_truncated", "true")
	}
	out.WriteByte('>')
	out.WriteString(xmlText(strings.TrimSpace(message.Text)))
	out.WriteString("</Message>")
	return out.String()
}

// renderHistoryXML 保留最近完整消息；预算计入声明、标签与转义后的实际字符数。
type historyFormat struct {
	sourceKind string
	scope      string
	maxChars   int
}

func renderHistoryXML(messages []ContextMessage, senderID string, format historyFormat) string {
	var lines []string
	for _, message := range messages {
		if strings.TrimSpace(message.Text) != "" {
			lines = append(lines, historyMessageXML(message, senderID, false))
		}
	}
	for start := 0; start < len(lines); start++ {
		result := historyEnvelope(lines[start:], format, start > 0)
		if len([]rune(result)) <= format.maxChars {
			return result
		}
	}
	if format.sourceKind == "reference" && len(messages) == 1 {
		return fitReferenceXML(messages[0], senderID, format)
	}
	return ""
}

func fitReferenceXML(message ContextMessage, senderID string, format historyFormat) string {
	runes := []rune(message.Text)
	low, high := 0, len(runes)
	result := ""
	for low <= high {
		mid := low + (high-low)/2
		candidate := message
		candidate.Text = string(runes[:mid]) + "…"
		rendered := historyEnvelope([]string{historyMessageXML(candidate, senderID, true)}, format, true)
		if len([]rune(rendered)) <= format.maxChars {
			result, low = rendered, mid+1
		} else {
			high = mid - 1
		}
	}
	return result
}

func historyEnvelope(lines []string, format historyFormat, truncated bool) string {
	header := contextHeader
	if format.sourceKind == "reference" {
		header = referenceHeader
	}
	var out strings.Builder
	out.WriteString(header)
	out.WriteString(`<ChatHistory snapshot="partial"`)
	xmlAttr(&out, "source_kind", format.sourceKind)
	xmlAttr(&out, "scope", format.scope)
	if truncated {
		xmlAttr(&out, "truncated", "true")
	}
	out.WriteString(">\n")
	out.WriteString(strings.Join(lines, "\n"))
	out.WriteString("\n</ChatHistory>")
	out.WriteString(contextFooter)
	return out.String()
}
