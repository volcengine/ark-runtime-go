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

func TestRuntimeRecallInterruptsAndUsesAvailableReplyTarget(t *testing.T) {
	f, p := &conformance.MAServer{}, &runtimeProvider{}
	h := newHarness(t, f, p)
	original := runtimeMessage()
	h.enqueue(original)
	h.run()
	h.phaseEventually(core.PhaseObserve)
	recall := core.Message{BindingID: original.BindingID, ProviderEventID: "recall-event", MessageID: "recall", RecalledMessageID: original.MessageID, TenantKey: original.TenantKey, ChatID: original.ChatID, ChatType: "recall"}
	result := h.enqueue(recall)
	require.NotNil(t, result.Control)
	require.True(t, h.enqueue(recall).Duplicate)
	state := h.waitSettled(core.TurnStopped, core.DeliveryDone, 3*time.Second)
	require.Equal(t, "已停止。", state.export.Deliveries[0].Body.Text)
	require.Equal(t, "chat", state.export.Deliveries[0].ReplyTarget.Mode)
	require.Empty(t, state.export.Deliveries[0].ReplyTarget.MessageID)
	f.Lock()
	require.Equal(t, 1, f.InterruptCount)
	f.Unlock()
}
