// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

var _ core.ProgressReporter = (*Provider)(nil)

const (
	maxProgressSteps = 20
	maxStepName      = 64
)

// ReportProgress 以交互卡片展示过程：首次调用回复一张卡片并返回 message_id，后续调用 PATCH
// 同一张卡片。卡片只列工具名与状态，不展示参数或结果。卡片 UUID 由轮次派生，重试不会产生重复卡片。
func (p *Provider) ReportProgress(ctx context.Context, handle string, update core.ProgressUpdate) (string, error) {
	if p.client == nil || ctx == nil || update.TurnID == "" {
		return "", failure{"feishu client unavailable", false, false, "invalid_client"}
	}
	content, err := progressCard(update)
	if err != nil {
		return "", failure{"feishu progress invalid", false, false, "invalid_progress"}
	}
	ctx, cancel := boundedContext(ctx)
	defer cancel()
	if handle != "" {
		resp, err := p.client.Im.Message.Patch(ctx, larkim.NewPatchMessageReqBuilder().MessageId(handle).Body(larkim.NewPatchMessageReqBodyBuilder().Content(content).Build()).Build())
		if err != nil {
			return "", transportFailure(err)
		}
		if resp == nil || resp.ApiResp == nil {
			return "", failure{"feishu response missing", true, false, "response_missing"}
		}
		if err := responseFailure(resp.StatusCode, resp.Code, resp.Success(), true); err != nil {
			return "", err
		}
		return handle, nil
	}
	uuid := progressUUID(update.TurnID)
	target := update.ReplyTarget
	switch target.Mode {
	case "chat":
		body := larkim.NewCreateMessageReqBodyBuilder().ReceiveId(target.ChatID).MsgType("interactive").Content(content).Uuid(uuid).Build()
		resp, err := p.client.Im.Message.Create(ctx, larkim.NewCreateMessageReqBuilder().ReceiveIdType("chat_id").Body(body).Build())
		if err != nil {
			return "", transportFailure(err)
		}
		if resp == nil || resp.ApiResp == nil {
			return "", failure{"feishu response missing", true, false, "response_missing"}
		}
		if err := responseFailure(resp.StatusCode, resp.Code, resp.Success(), resp.Data != nil && resp.Data.MessageId != nil && *resp.Data.MessageId != ""); err != nil {
			return "", err
		}
		return *resp.Data.MessageId, nil
	case "reply", "thread":
		body := larkim.NewReplyMessageReqBodyBuilder().MsgType("interactive").Content(content).Uuid(uuid)
		if target.Mode == "thread" {
			body.ReplyInThread(true)
		}
		resp, err := p.client.Im.Message.Reply(ctx, larkim.NewReplyMessageReqBuilder().MessageId(target.MessageID).Body(body.Build()).Build())
		if err != nil {
			return "", transportFailure(err)
		}
		if resp == nil || resp.ApiResp == nil {
			return "", failure{"feishu response missing", true, false, "response_missing"}
		}
		if err := responseFailure(resp.StatusCode, resp.Code, resp.Success(), resp.Data != nil && resp.Data.MessageId != nil && *resp.Data.MessageId != ""); err != nil {
			return "", err
		}
		return *resp.Data.MessageId, nil
	}
	return "", failure{"feishu reply mode unsupported", false, false, "invalid_target"}
}

func progressUUID(turnID string) string {
	return deliveryUUID("progress:" + turnID)
}

// deliveryUUID 生成稳定的 UUID 形式幂等键（飞书要求 ≤50 字符）。
func deliveryUUID(key string) string {
	h := sha256Hex(key)
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

func truncate(text string, limit int) string {
	if utf8.RuneCountInString(text) <= limit {
		return text
	}
	runes := []rune(text)
	return string(runes[:limit-1]) + "…"
}

// escapeMarkdown 去掉会改变卡片 markdown 语义的字符，工具名来自 Agent，不可信。
func escapeMarkdown(text string) string {
	replacer := strings.NewReplacer("*", "", "_", " ", "`", "", "[", "(", "]", ")", "<", "‹", ">", "›", "\n", " ")
	return replacer.Replace(text)
}

// progressText 限制工具名和摘要长度，并将控制字符归一为空格；卡片以纯文本展示。
func progressText(text string, limit int) string {
	return truncate(strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || r == '\u2028' || r == '\u2029' {
			return ' '
		}
		return r
	}, text), limit)
}

func progressCard(update core.ProgressUpdate) (string, error) {
	title, template := "处理中", "blue"
	switch update.State {
	case core.ProgressDone:
		title, template = "已完成", "green"
	case core.ProgressFailed:
		title, template = "未完成", "red"
	case core.ProgressStopped:
		title, template = "已停止", "grey"
	}
	var lines []string
	steps := update.Steps
	hidden := 0
	if len(steps) > maxProgressSteps {
		hidden = len(steps) - maxProgressSteps
		steps = steps[hidden:]
	}
	if hidden > 0 {
		lines = append(lines, fmt.Sprintf("… 之前还有 %d 步", hidden))
	}
	for _, step := range steps {
		mark := "⏳"
		switch {
		case step.Failed:
			mark = "⚠️"
		case step.Done:
			mark = "✅"
		case update.State != core.ProgressRunning:
			mark = "⏹"
		}
		lines = append(lines, mark+" "+progressText(step.Name, maxStepName))
	}
	if len(lines) == 0 {
		if update.State == core.ProgressRunning {
			lines = append(lines, "正在思考…")
		} else {
			lines = append(lines, title)
		}
	}
	if update.Summary != "" {
		lines = append(lines, "", progressText(update.Summary, 200))
	}
	card := map[string]any{
		"config": map[string]any{"wide_screen_mode": true, "update_multi": true},
		"header": map[string]any{"template": template, "title": map[string]string{"tag": "plain_text", "content": title}},
		"elements": []any{
			map[string]any{"tag": "div", "text": map[string]string{"tag": "plain_text", "content": strings.Join(lines, "\n")}},
		},
	}
	data, err := json.Marshal(card)
	return string(data), err
}
