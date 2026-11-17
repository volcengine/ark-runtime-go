// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPendingApprovalsKeepsMainThreadWaitDuringChildOutput(t *testing.T) {
	j := &job{Turn: Turn{Session: TurnSession{SessionID: "sesn-main"}, InputEventID: "input"}}
	for _, kind := range []string{"agent.message", "session.status_running"} {
		for _, thread := range []string{"sthr-child", "", "sthr-main"} {
			t.Run(kind+"/"+thread, func(t *testing.T) {
				events := []Event{{ID: "input", Type: "user.message"},
					{ID: "tool", Type: "agent.tool_use", Permission: "ask"},
					{ID: "waiting", Type: "session.status_idle", StopReason: "requires_action", ActionEventIDs: []string{"tool"}},
					{ID: "output", Type: kind, SessionThreadID: thread}}
				pending := pendingApprovals(events, j)
				if thread == "" || thread == "sthr-main" {
					require.Empty(t, pending, "主线程恢复执行后不再展示旧审批")
				} else {
					require.Len(t, pending, 1, "子线程输出不能清除主线程待审批状态")
					require.Equal(t, "tool", pending[0].ID)
				}
			})
		}
	}
}
