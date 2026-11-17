// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestM1AmbiguousHistoryDeliversDiagnosticInsteadOfOtherInputAnswer(t *testing.T) {
	f, p := &conformance.MAServer{}, &runtimeProvider{}
	h := newHarness(t, f, p)
	h.enqueue(runtimeMessage())
	h.run()
	f.AppendAfterInput(t,
		map[string]any{"id": "partial", "type": "agent.message", "content": []map[string]string{{"type": "text", "text": "incomplete private draft"}}},
		map[string]any{"id": "foreign-input", "type": "user.message", "content": []map[string]string{{"type": "text", "text": "foreign private question"}}},
		map[string]any{"id": "foreign-answer", "type": "agent.message", "content": []map[string]string{{"type": "text", "text": "foreign private answer"}}},
		map[string]any{"id": "idle", "type": "session.status_idle", "stop_reason": map[string]string{"type": "end_turn"}})
	state := h.waitSettled(core.TurnFailed, core.DeliveryDone, 3*time.Second)
	require.Equal(t, "unsupported_contract", state.turn.Outcome.ErrorKind)
	require.Equal(t, "收到了当前 SDK 无法识别的执行结果，本次请求已结束。", state.export.Deliveries[0].Body.Text)
	require.Equal(t, state.export.Deliveries[0].Body, p.delivered()[0].Body)
	f.Lock()
	defer f.Unlock()
	require.Equal(t, 1, f.SendCount)
}

func TestRuntimeReadPreviewWithoutFinalTextCompletesSilently(t *testing.T) {
	f, p := &conformance.MAServer{}, &runtimeProvider{}
	h := newHarness(t, f, p)
	h.enqueue(runtimeMessage())
	h.run()
	f.AppendAfterInput(t,
		map[string]any{"id": "progress", "type": "agent.message", "content": []map[string]string{{"type": "text", "text": "检查视频预览。"}}},
		map[string]any{"id": "read", "type": "agent.tool_use", "name": "read", "input": map[string]string{"file_path": "/tmp/preview.png"}},
		map[string]any{"id": "read-result", "type": "agent.tool_result", "tool_use_id": "read", "content": []map[string]any{{"type": "image", "source": map[string]string{"type": "base64", "media_type": "image/png", "data": "cHJldmlldw=="}}}},
		map[string]any{"id": "idle", "type": "session.status_idle", "stop_reason": map[string]string{"type": "end_turn"}})
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 3*time.Second)
	require.Empty(t, state.turn.Outcome.ErrorKind)
	require.Empty(t, state.export.Deliveries, "正常静默结束不应投递错误提示、过程文字或预览图")
	require.Empty(t, p.delivered())
	f.Lock()
	defer f.Unlock()
	require.Equal(t, 1, f.SendCount)
	require.Zero(t, f.InterruptCount, "已确认 end_turn 的轮次不应被错误中断")
}

func TestM1StatusOnlyFastFailureShowsStateWithoutTools(t *testing.T) {
	f, p := &conformance.MAServer{CreateCode: 400}, &progressProvider{}
	b := runtimeBinding()
	b.ProgressMode = "status_only"
	b.ShowProgress = false
	h := newHarnessWith(t, f, p, b, testRuntimeOptions())
	h.enqueue(runtimeMessage())
	h.run()
	h.waitSettled(core.TurnFailed, core.DeliveryDone, 3*time.Second)
	updates, _ := p.snapshot()
	require.Len(t, updates, 1)
	require.Equal(t, core.ProgressFailed, updates[0].State)
	require.Empty(t, updates[0].Steps)
	require.Contains(t, updates[0].Summary, "请求")
	data, err := json.Marshal(updates)
	require.NoError(t, err)
	require.NotContains(t, string(data), "input")
}
