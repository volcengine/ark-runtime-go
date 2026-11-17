// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestApprovalDecisionRejectsForwardedChat(t *testing.T) {
	var interaction Interaction
	require.NoError(t, json.Unmarshal([]byte(`{"id":"ix:turn:tool","turn_id":"turn","kind":"approval","status":"pending","approver_id":"user","tool_use_id":"tool","confirm_event_id":"confirm","binding_id":"binding","binding_epoch":1,"app_id":"app","tenant_key":"tenant","chat_id":"original"}`), &interaction))
	interaction.ExpiresAt = time.Now().Add(time.Minute)
	for _, chat := range []string{"forwarded", ""} {
		var decision InteractionDecision
		require.NoError(t, json.Unmarshal([]byte(`{"Allow":true,"DeciderID":"user","BindingID":"binding","BindingEpoch":1,"AppID":"app","TenantKey":"tenant","ChatID":"`+chat+`"}`), &decision))
		got, changed, err := interaction.Decide(decision, time.Now())
		require.ErrorIs(t, err, ErrApprovalForbidden)
		require.False(t, changed)
		require.Equal(t, InteractionPending, got.Status)
	}
}
