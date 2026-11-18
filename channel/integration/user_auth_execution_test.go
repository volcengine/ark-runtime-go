// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// 执行中的鉴权错误交给原轮次结算，Channel 不重授、不追加输入。
func TestRuntimeToolAuthErrorDoesNotReauthorizeOrResubmit(t *testing.T) {
	for _, kind := range []string{"embedded", "sql"} {
		for _, terminal := range []struct {
			name   string
			reason string
			status core.TurnStatus
		}{
			{name: "idle", reason: "end_turn", status: core.TurnDone},
			{name: "failed", reason: "retries_exhausted", status: core.TurnFailed},
		} {
			t.Run(kind+"/"+terminal.name, func(t *testing.T) {
				store := userRuntimeStore(t, kind)
				auth := &userAuthFixture{ready: true, store: store.(core.UserAuthStore)}
				fixture, provider := &conformance.MAServer{EnvironmentType: "cloud"}, &runtimeProvider{}
				binding, options := userBinding(), testRuntimeOptions()
				options.MessageDeadline, options.TurnTimeout = time.Minute, 5*time.Second
				binding.MaxTurnDuration = options.TurnTimeout
				client := &userVaultMA{MAClient: newMAForHarness(t, fixture)}
				runtime, err := core.NewRuntime(store, []core.Connection{{Binding: binding, MAClient: client, Provider: provider, UserAuthorizer: auth}}, options)
				require.NoError(t, err)
				admitted := mustEnqueue(t, runtime, binding.ID, runtimeMessage())
				runUntilCleanup(t, runtime)
				fixture.AppendAfterInput(t,
					map[string]any{"id": "tool", "type": "agent.tool_use", "tool_use_id": "tool", "name": "bash", "input": map[string]string{"command": "lark-cli docs +fetch --doc-token doc123"}},
					map[string]any{"id": "result", "type": "agent.tool_result", "tool_use_id": "tool", "is_error": true, "content": []map[string]string{{"type": "text", "text": "exit_code: 3\n--- stderr ---\n{\"ok\":false,\"identity\":\"user\",\"error\":{\"type\":\"authentication\",\"subtype\":\"token_invalid\"}}"}}},
					map[string]any{"id": "answer", "type": "agent.message", "content": []map[string]string{{"type": "text", "text": "用户授权失效，请重新授权后再试。"}}},
					map[string]any{"id": "terminal", "type": "session.status_idle", "stop_reason": map[string]string{"type": terminal.reason}},
				)
				require.Eventually(t, func() bool {
					auth.mu.Lock()
					started := auth.starts > 0
					auth.mu.Unlock()
					turn, err := store.GetTurn(t.Context(), admitted.Turn.ID)
					return started || err == nil && turn.Status.Terminal()
				}, 3*time.Second, 10*time.Millisecond)
				auth.mu.Lock()
				starts := auth.starts
				auth.mu.Unlock()
				require.Zero(t, starts, "an execution error must not initiate another authorization")
				turn, err := store.GetTurn(t.Context(), admitted.Turn.ID)
				require.NoError(t, err)
				require.Equal(t, terminal.status, turn.Status)
				fixture.Lock()
				defer fixture.Unlock()
				require.Equal(t, 1, fixture.SendCount, "the original input must be the only submitted message")
			})
		}
	}
}
