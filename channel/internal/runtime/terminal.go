// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

// eventTerminalStatus 统一观察、授权信号和整轮证据的实际终态边界。
// requires_action/retrying 是同一输入的中间状态；缺失或未知终态原因不能作为完成证明。
func eventTerminalStatus(event Event) string {
	switch event.Type {
	case "session.status_terminated", "session.deleted":
		return "session_gone"
	case "session.error":
		switch event.RetryStatus {
		case "retrying":
			return ""
		case "exhausted", "terminal":
			return "agent_failed"
		default:
			return "unsupported_contract"
		}
	case "session.status_idle":
		switch event.StopReason {
		case "requires_action":
			return ""
		case "retries_exhausted":
			return "agent_failed"
		case "end_turn":
			return "done"
		default:
			return "unsupported_contract"
		}
	}
	return ""
}
