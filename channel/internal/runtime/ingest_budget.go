// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import "strconv"

// IngestChannelScope 隔离同账号同项目的 Channel 执行容量；epoch 与配置版本不改变容量身份。
func IngestChannelScope(b Binding) string {
	channel := b.ChannelID
	if channel == "" {
		channel = b.ID
	}
	return stableKey(b.Owner, b.Project, channel)
}

// RejectedMessage 保存拒绝去重键到真实控制来源和诊断投递的稳定引用。
// 每个新 provider event 别名占一个预算槽位，重复 event 不再占位。
type RejectedMessage struct {
	MessageID  string `json:"message_id"`
	ControlID  string `json:"control_id"`
	DeliveryID string `json:"delivery_id"`
	CreatedAt  int64  `json:"created_at"`
}

// RejectControlKind 是容量拒绝的诊断来源，不执行停止命令。
const RejectControlKind ControlKind = "queue_full"

// NewRejectControl 创建真实的控制来源和确定性诊断；拒绝不会创建 Turn。
func NewRejectControl(message InboundMessage, version BindingVersion) (ControlRequest, Delivery) {
	c := ControlRequest{ID: "reject:" + stableKey(message.ID, strconv.FormatUint(message.Seq, 10))[:40], Seq: message.Seq, MessageID: message.ID, ConversationID: message.ConversationID, Kind: RejectControlKind, CreatedAt: message.ReceivedAt}
	d := NewControlReply(c, message, version.Content, "当前消息较多，请稍后重试。")
	d.Snapshot = NewTurnSnapshot(version.Content, message.Message.AdmitUntil)
	return c, d
}
