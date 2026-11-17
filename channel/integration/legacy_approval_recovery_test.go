// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestRuntimeRecoveredApprovalRedactsLegacyTruncatedJSON(t *testing.T) {
	const credential = "fixture-private-key"
	input := `{"api_key":"` + credential + `","action":"deploy","padding":"` + strings.Repeat("x", 400) + `"}`
	legacy := string([]rune(input)[:299]) + "…"
	f, p := &conformance.MAServer{}, &approvalProvider{}
	h := approvalHarness(t, f, p, true, testRuntimeOptions())
	message := runtimeMessage()
	ingested := h.enqueue(message)
	ctx := t.Context()
	lease, err := h.store.LeaseTurn(ctx, core.ClaimOptions{Owner: "previous-runtime", TTL: time.Minute, MessageDeadline: time.Minute})
	require.NoError(t, err)
	require.NotNil(t, lease)
	now := time.Now()
	_, err = h.store.PutInteraction(ctx, lease.Turn.Lease, core.Interaction{
		ID: core.InteractionID(ingested.Turn.ID, "tool-ask"), TurnID: ingested.Turn.ID, Kind: core.InteractionApproval,
		Status: core.InteractionPending, ApproverID: message.SenderID, ToolUseID: "tool-ask", ToolName: "delete_repo",
		InputPreview: legacy, ConfirmEventID: "existing-confirm", BindingID: runtimeBinding().ID, AppID: runtimeBinding().AppID,
		TenantKey: message.TenantKey, ChatID: message.ChatID, CreatedAt: now, ExpiresAt: now.Add(time.Minute),
	})
	require.NoError(t, err)
	require.NoError(t, h.store.ReleaseTurn(ctx, lease.Turn.Lease))

	h.run()
	events := askEvents()
	events[0]["input"] = input
	f.AppendAfterInput(t, events...)
	require.Eventually(t, func() bool { return len(p.prompted()) > 0 }, 2*time.Second, 5*time.Millisecond)
	prompt := p.prompted()[0]
	require.NotContains(t, prompt.InputPreview, credential)
	require.Contains(t, prompt.InputPreview, "deploy")
	require.Contains(t, prompt.InputPreview, "[REDACTED]")
}
