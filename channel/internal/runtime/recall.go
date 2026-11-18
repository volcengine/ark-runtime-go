// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

// RecallMessageID 返回同一原消息的稳定撤回事实键，重复事件不会再次停止下一条任务。
func RecallMessageID(bindingID, messageID string) string {
	return "recall:" + stableKey(bindingID, messageID)
}

// ResolveRecall 仅用已受理用户消息恢复撤回范围，不信任回调缺失的 thread 或 sender。
func ResolveRecall(recall Message, original InboundMessage) (Message, error) {
	if original.Kind != MessageUser || original.BindingID != recall.BindingID ||
		original.ProviderMessageID != recall.RecalledMessageID || original.Message.TenantKey != recall.TenantKey || original.Message.ChatID != recall.ChatID {
		return Message{}, ErrInvalid
	}
	message := original.Message
	message.TrackReactions = false
	message.RecalledMessageID = recall.RecalledMessageID
	message.MessageID = RecallMessageID(recall.BindingID, recall.RecalledMessageID)
	message.ProviderEventID = recall.ProviderEventID
	message.ReceivedAt, message.AdmitUntil = recall.ReceivedAt, recall.AdmitUntil
	message.Text, message.Attachments = "/stop", nil
	message.Mentioned = true
	return message, nil
}

// RecallReplyTarget 保留原会话，话题中优先回复仍可用的 parent；不改投其他会话。
func RecallReplyTarget(message Message) ReplyTarget {
	if message.ThreadID == "" {
		return ReplyTarget{ChatID: message.ChatID, Mode: "chat"}
	}
	if message.ParentID != "" {
		return ReplyTarget{ChatID: message.ChatID, ThreadID: message.ThreadID, MessageID: message.ParentID, Mode: "thread"}
	}
	return ReplyTarget{ChatID: message.ChatID, ThreadID: message.ThreadID, Mode: "none"}
}

// ApplyStopReplyTarget 在结算事务中使用最新的撤回反馈位置，避免与 Worker 的旧副本竞争。
func ApplyStopReplyTarget(turn Turn, deliveries []Delivery) []Delivery {
	if turn.StopReplyTarget == nil {
		return deliveries
	}
	out := make([]Delivery, 0, len(deliveries))
	for _, delivery := range deliveries {
		if delivery.Source.Kind == SourceTurn {
			if turn.StopReplyTarget.Mode == "none" {
				continue
			}
			delivery.ReplyTarget = *turn.StopReplyTarget
		}
		out = append(out, delivery)
	}
	return out
}
