// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestParseVerifiedApprovalActionRejectsInconsistentAuthority(t *testing.T) {
	for _, tc := range []struct {
		name    string
		binding core.ApprovalBinding
	}{
		{"missing binding", core.ApprovalBinding{AppID: "app-1", TenantKey: "tenant-1"}},
		{"foreign app", core.ApprovalBinding{BindingID: "binding-1", BindingEpoch: 7, AppID: "foreign", TenantKey: "tenant-1"}},
		{"foreign tenant", core.ApprovalBinding{BindingID: "binding-1", BindingEpoch: 7, AppID: "app-1", TenantKey: "foreign"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, transport := hostedCardProvider(t, core.Binding{ID: "binding-1", AppID: "app-1", BindingEpoch: 7})
			decision, immediate, err := p.ParseVerifiedApprovalAction(context.Background(), cardAction(actionApprove, "ou-sender"), tc.binding)
			require.Error(t, err)
			require.Nil(t, immediate)
			require.Empty(t, decision.TurnID)
			require.Zero(t, transport.calls.Load())
		})
	}
}

func TestParseVerifiedApprovalActionDoesNotSubmitOrTrustButtonCoordinates(t *testing.T) {
	p, transport := hostedCardProvider(t, core.Binding{ID: "binding-1", AppID: "app-1", BindingEpoch: 7})
	controller := &recordingController{}
	p.BindControl(controller)
	raw := updateCardAction(t, func(event map[string]any) {
		value := event["event"].(map[string]any)["action"].(map[string]any)["value"].(map[string]any)
		value["binding_id"], value["binding_epoch"], value["tenant_key"] = "forged", 999, "forged"
	})
	decision, immediate, err := p.ParseVerifiedApprovalAction(context.Background(), raw, hostedApprovalBinding())
	require.NoError(t, err)
	require.Nil(t, immediate)
	require.Equal(t, core.ApprovalDecision{TurnID: "turn:1", ToolUseID: "tool-ask", Allow: true, DeciderID: "ou-sender", BindingID: "binding-1", BindingEpoch: 7, AppID: "app-1", TenantKey: "tenant-1"}, decision)
	require.Empty(t, controller.decisions)
	require.Zero(t, transport.calls.Load())
}
