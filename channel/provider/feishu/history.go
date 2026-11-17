// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"time"

	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

var _ core.HistoryFetcher = (*Provider)(nil)
var _ core.ReferenceFetcher = (*Provider)(nil)

// historyPageSize 是单页拉取条数；飞书上限 50。
const historyPageSize = 50

// historyMaxPages 限制单次拉取的翻页数，避免活跃群里扫描过多历史。
const historyMaxPages = 4

// FetchHistory 拉取当前消息所在会话范围内、早于当前消息的近期消息。
//
// 范围：话题消息只取同一话题（container_id_type=thread）；其余取所在单聊或群（chat）。
// 群聊顶层消息会包含其他成员的发言——这正是“群聊背景”的含义，但不会跨群或混入其他私聊。
// 只保留文本与富文本的文字部分，排除当前触发消息、已撤回消息和卡片等非文本消息。
func (p *Provider) FetchHistory(ctx context.Context, query core.HistoryQuery) ([]core.ContextMessage, error) {
	scope, valid := scopedHistoryQuery(query)
	if p.client == nil || !valid || query.Limit <= 0 {
		return nil, errors.New("feishu history query invalid")
	}
	containerType, containerID := "chat", query.Message.ChatID
	if scope == core.HistoryScopeTopic {
		containerType, containerID = "thread", query.Message.ThreadID
	}
	end := query.Message.ReceivedAt
	if end.IsZero() {
		end = time.Now()
	}
	var collected []core.ContextMessage
	seen := make(map[string]bool)
	pageToken := ""
	for page := 0; page < historyMaxPages && len(collected) < query.Limit; page++ {
		builder := larkim.NewListMessageReqBuilder().ContainerIdType(containerType).ContainerId(containerID).
			SortType("ByCreateTimeDesc").PageSize(historyPageSize)
		// 飞书 thread 容器不支持时间范围参数，统一在逐条校验中截取时间窗口。
		if scope != core.HistoryScopeTopic {
			builder.EndTime(strconv.FormatInt(end.Unix(), 10))
			if !query.Since.IsZero() {
				builder.StartTime(strconv.FormatInt(query.Since.Unix(), 10))
			}
		}
		if pageToken != "" {
			builder.PageToken(pageToken)
		}
		callCtx, cancel := boundedContext(ctx)
		resp, err := p.client.Im.Message.List(callCtx, builder.Build())
		cancel()
		if err != nil || resp == nil || !resp.Success() || resp.Data == nil {
			return nil, errors.New("feishu history unavailable")
		}
		for _, item := range resp.Data.Items {
			if message, ok := p.contextMessage(item, query); ok {
				if seen[message.MessageID] {
					continue
				}
				seen[message.MessageID] = true
				collected = append(collected, message)
				if len(collected) >= query.Limit {
					break
				}
			}
		}
		if resp.Data.HasMore == nil || !*resp.Data.HasMore || resp.Data.PageToken == nil {
			break
		}
		pageToken = *resp.Data.PageToken
	}
	sort.SliceStable(collected, func(i, j int) bool { return collected[i].At.Before(collected[j].At) })
	return collected, nil
}

// FetchReference 只读取入站事件明确指定的父消息；Get 可能返回合并转发的子消息，必须精确匹配 ID。
func (p *Provider) FetchReference(ctx context.Context, message core.Message) (core.ContextMessage, error) {
	query := core.HistoryQuery{Message: message}
	if _, valid := scopedHistoryQuery(query); p.client == nil || !valid || message.ParentID == "" || message.ParentID == message.MessageID {
		return core.ContextMessage{}, errors.New("feishu reference query invalid")
	}
	callCtx, cancel := boundedContext(ctx)
	defer cancel()
	resp, err := p.client.Im.Message.Get(callCtx, larkim.NewGetMessageReqBuilder().MessageId(message.ParentID).Build())
	if err != nil || resp == nil || !resp.Success() || resp.Data == nil {
		return core.ContextMessage{}, errors.New("feishu reference unavailable")
	}
	for _, item := range resp.Data.Items {
		if item == nil || deref(item.MessageId) != message.ParentID {
			continue
		}
		if reference, ok := p.contextMessage(item, query); ok {
			return reference, nil
		}
		break
	}
	return core.ContextMessage{}, errors.New("feishu reference outside proven conversation")
}

func scopedHistoryQuery(query core.HistoryQuery) (core.HistoryScope, bool) {
	message := query.Message
	if message.ChatID == "" || message.TenantKey == "" || message.ChatType != "group" && message.ChatType != "p2p" {
		return "", false
	}
	scope := core.HistoryScopeGroup
	if message.ThreadID != "" {
		scope = core.HistoryScopeTopic
	} else if message.ChatType == "p2p" {
		scope = core.HistoryScopeP2P
	}
	return scope, query.Scope == "" || query.Scope == scope
}

func (p *Provider) contextMessage(item *larkim.Message, query core.HistoryQuery) (core.ContextMessage, bool) {
	_, valid := scopedHistoryQuery(query)
	if !valid || item == nil || item.MessageId == nil || *item.MessageId == "" || *item.MessageId == query.Message.MessageID {
		return core.ContextMessage{}, false
	}
	if item.Deleted != nil && *item.Deleted || item.MsgType == nil || item.Body == nil || item.Body.Content == nil {
		return core.ContextMessage{}, false
	}
	if *item.MsgType != "text" && *item.MsgType != "post" {
		return core.ContextMessage{}, false
	}
	// API 容器参数不是返回内容的归属证明；逐条核验，缺字段时也不注入。
	if deref(item.ChatId) != query.Message.ChatID || deref(item.ThreadId) != query.Message.ThreadID || item.Sender == nil || deref(item.Sender.TenantKey) != query.Message.TenantKey {
		return core.ContextMessage{}, false
	}
	text, _, err := parseContent(*item.MsgType, *item.Body.Content)
	if err != nil || text == "" {
		return core.ContextMessage{}, false
	}
	millis, err := strconv.ParseInt(deref(item.CreateTime), 10, 64)
	if err != nil || millis <= 0 {
		return core.ContextMessage{}, false
	}
	at := time.UnixMilli(millis)
	if !query.Since.IsZero() && at.Before(query.Since) || !query.Message.ReceivedAt.IsZero() && !at.Before(query.Message.ReceivedAt) {
		return core.ContextMessage{}, false
	}
	message := core.ContextMessage{MessageID: *item.MessageId, Text: text, At: at}
	if item.Sender != nil {
		message.SenderID = deref(item.Sender.Id)
		message.SenderIDType = deref(item.Sender.IdType)
		message.SenderIsBot = deref(item.Sender.SenderType) == "app"
	}
	return message, true
}
