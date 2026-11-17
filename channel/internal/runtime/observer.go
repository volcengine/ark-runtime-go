// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"time"
)

// 观测事件名。名字与 Status/ErrorKind 都是有限枚举，可直接作为指标 label；ID 类字段只进日志。
const (
	EventIngress        = "ingress"         // Status: accepted / duplicate / rejected
	EventStopRequest    = "stop_request"    // Status: applied / no_target / failed
	EventSessionCreate  = "session_create"  // Status: created / adopted / throttled / rejected / unknown
	EventSessionRenew   = "session_renew"   // 原 Session 不可写，切换到下一代
	EventTurnSubmit     = "turn_submit"     // Status: accepted / throttled / rejected / unknown
	EventTurnWait       = "turn_wait"       // 释放 Worker 等待重新领取；ErrorKind 说明原因
	EventTurnSettle     = "turn_settle"     // Status: done / failed / stopped；Duration 为入队到结算
	EventInterrupt      = "interrupt"       // Status: sent / failed
	EventHistoryError   = "history_error"   // 事件历史读取失败
	EventStreamDrop     = "stream_drop"     // SSE 断开（仅唤醒信号，不影响正确性）
	EventDeliveryResult = "delivery_result" // Status: done / retry / uncertain / failed
	EventProgress       = "progress"        // Status: created / updated / completed / failed（过程展示旁路）
	EventApproval       = "approval"        // Status: prompted / allowed / denied / confirm_resent / confirm_failed / failed
	EventHook           = "hook"            // Status: ok / denied / error / timeout
	EventAuth           = "auth"            // Status: ready / waiting / failed
	EventAttachment     = "attachment"      // Status: prepared / rejected / failed
	EventCleanup        = "cleanup"         // Count 为本次清理的记录数
	EventContext        = "context"         // Status: prepared / skipped
	EventWorkBlocked    = "work_blocked"    // Status: blocked；不支持的持久任务只隔离所属来源
)

// Observation 是一条结构化观测记录。
//
// Name、Status、ErrorKind 构成指标维度；其余 ID 字段只用于日志与排障定位，实现不得把它们
// 放进指标 label。不携带消息正文、Secret 或用户可见文本。
type Observation struct {
	Name           string
	Status         string
	ErrorKind      string
	Duration       time.Duration
	Count          int
	BindingID      string
	TurnID         string
	DeliveryID     string
	ConversationID RouteKey
	SessionID      string
	At             time.Time
}

// Observer 接收 Runtime 的观测记录。实现必须快速返回且并发安全；观测失败不得影响业务。
type Observer interface {
	Observe(context.Context, Observation)
}

type noopObserver struct{}

func (noopObserver) Observe(context.Context, Observation) {}

// report 补全公共字段后上报；Observer 的 panic 被吞掉，观测永远不改变业务结果。
func (r *Runtime) report(ctx context.Context, item Observation) {
	if item.At.IsZero() {
		item.At = r.options.Clock()
	}
	if ctx == nil {
		ctx = context.Background()
	}
	defer func() { _ = recover() }()
	r.options.Observer.Observe(ctx, item)
}

func (r *Runtime) reportTurn(name, status, kind string, j *job) {
	r.report(context.Background(), Observation{Name: name, Status: status, ErrorKind: kind, BindingID: j.BindingID, TurnID: j.ID, ConversationID: j.ConversationID, SessionID: j.Session.SessionID})
}
