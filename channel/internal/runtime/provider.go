// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import "context"

// Provider 接收渠道消息并投递已持久化的回复。
type Provider interface {
	Start(context.Context, func(context.Context, Message) error) error
	Deliver(context.Context, Delivery) (string, error)
}

// DeliveryFailure 让运行时区分可安全重试的拒绝与结果未知的写入。
type DeliveryFailure interface {
	error
	Uncertain() bool
	Retryable() bool
	ErrorKind() string
}

// ApprovalBinding 是卡片回调必须匹配的绑定坐标，由宿主在验签后从当前权威配置填入。
type ApprovalBinding struct {
	BindingID    string
	BindingEpoch uint64
	AppID        string
	TenantKey    string
}

// Controller 是 Runtime 暴露给渠道的交互回调入口（卡片按钮等）。
type Controller interface {
	SubmitApproval(context.Context, ApprovalDecision) error
}

// ControlBinder 由需要回调 Runtime 的 Provider 实现；Runtime 在启动 Provider 前调用一次。
type ControlBinder interface {
	BindControl(Controller)
}
