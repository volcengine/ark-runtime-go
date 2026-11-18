// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"strings"
	"testing"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// logUserAuthOnFailure 记录非密状态，区分测试等待不足和 Runtime 停在错误阶段。
func logUserAuthOnFailure(t *testing.T, current func() core.Store, turnID string, principal core.UserPrincipal) func() {
	t.Helper()
	return func() {
		if !t.Failed() {
			return
		}
		store := current()
		turn, err := store.GetTurn(context.Background(), turnID)
		if turn != nil {
			t.Logf("turn: status=%s phase=%s wait=%s schedule=%+v outcome=%+v lease=%+v auth_wait=%+v err=%v", turn.Status, turn.Phase, turn.WaitReason, turn.Schedule, turn.Outcome, turn.Lease, turn.AuthWait, err)
		} else {
			t.Logf("turn missing: %v", err)
		}
		if auth, ok := store.(core.UserAuthStore); ok {
			record, err := auth.GetUserAuth(context.Background(), principal)
			if record != nil {
				t.Logf("auth: state=%s operation=%s attempt=%d revision=%d lease=%+v err=%v", record.State, record.Operation, record.Attempt, record.Revision, record.Lease, err)
			}
		}
		if reservations, ok := store.(core.BotVaultStore); ok {
			name := "channel-user-" + strings.TrimPrefix(principal.Key(), "user:")
			record, err := reservations.GetBotVaultReservation(context.Background(), principal.Key(), name)
			t.Logf("vault reservation: %+v err=%v", record, err)
		}
	}
}
