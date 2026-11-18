// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"time"
)

// MessageReaction 是渠道无关的处理状态；空值清理本 Bot 的处理表情。
type MessageReaction string

const (
	// ReactionQueued 表示消息正在等待处理。
	ReactionQueued MessageReaction = "queued"
	// ReactionRunning 表示已开始处理。
	ReactionRunning MessageReaction = "running"
	// ReactionDone 表示已成功完成。
	ReactionDone MessageReaction = "done"
)

// MessageReactionState 是可重启的表情投递进度，Failures 不含正常等待次数。
type MessageReactionState struct {
	Applied   MessageReaction `json:"applied"`
	CheckedAt time.Time       `json:"checked_at"`
	Failures  uint64          `json:"failures,omitempty"`
}

// MessageReactionReporter 将当前 Bot 的处理表情收敛到指定状态。
type MessageReactionReporter interface {
	SyncMessageReaction(context.Context, ReplyTarget, MessageReaction) error
}

// ReactionForTurn 根据持久业务状态决定反馈，不把观察重试误判为重新排队。
func ReactionForTurn(turn Turn) MessageReaction {
	if turn.Status == TurnDone {
		return ReactionDone
	}
	if !turn.StopRequestedAt.IsZero() {
		return ""
	}
	switch turn.Status {
	case TurnFailed, TurnStopped:
		return ""
	}
	if !turn.Schedule.StartedAt.IsZero() {
		return ReactionRunning
	}
	return ReactionQueued
}

// NewMessageReactionDelivery 为已受理消息建立一个持续跟踪 Turn 的独立 Outbox 项。
func NewMessageReactionDelivery(turn Turn, message Message) Delivery {
	d := NewDelivery(DeliverySource{Kind: SourceReaction, ID: turn.ID}, 0, "message-reaction", turn.BindingID,
		ReplyTarget{ChatID: message.ChatID, ThreadID: message.ThreadID, MessageID: message.MessageID}, DeliveryBody{Type: BodyReaction})
	// 保持旧 reader 的检查点校验可用；新 reader 只通过 source/body 处理。
	d.WorkKind = WorkKindReaction
	d.Snapshot = turn.Snapshot
	return d
}
