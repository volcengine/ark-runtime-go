// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

type recordingController struct {
	mu        sync.Mutex
	decisions []core.ApprovalDecision
	err       error
}

func (c *recordingController) SubmitApproval(_ context.Context, decision core.ApprovalDecision) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.decisions = append(c.decisions, decision)
	return c.err
}

func cardAction(action, operator string) []byte {
	payload := map[string]any{
		"schema": "2.0",
		"header": map[string]any{"event_id": "ev-card", "event_type": "card.action.trigger", "token": "verify-token", "app_id": "app-1", "tenant_key": "tenant-1", "create_time": "1700000000000"},
		"event": map[string]any{
			"operator": map[string]any{"open_id": operator},
			"action":   map[string]any{"tag": "button", "value": map[string]any{"action": action, "turn_id": "turn:1", "tool_use_id": "tool-ask"}},
			"context":  map[string]any{"open_message_id": "om-card"},
		},
	}
	data, _ := json.Marshal(payload)
	return data
}

func TestCardActionSubmitsDecisionAndReplacesCard(t *testing.T) {
	p, _, _ := webhookProvider(t, "")
	controller := &recordingController{}
	rec := signedPost(t, p, cardAction(actionApprove, "ou-sender"))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "稍后重试", "callbacks before BindControl ask feishu to retry")
	p.BindControl(controller)
	rec = signedPost(t, p, cardAction(actionApprove, "ou-sender"))
	require.Equal(t, http.StatusOK, rec.Code)
	var response struct {
		Toast struct {
			Content string `json:"content"`
		} `json:"toast"`
		Card struct {
			Type string `json:"type"`
		} `json:"card"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	require.Equal(t, "已允许", response.Toast.Content)
	require.Equal(t, "raw", response.Card.Type, "the card is replaced so it cannot be clicked twice")
	rec = signedPost(t, p, cardAction(actionDeny, "ou-sender"))
	require.Contains(t, rec.Body.String(), "已拒绝")
	controller.mu.Lock()
	require.Len(t, controller.decisions, 2)
	require.Equal(t, core.ApprovalDecision{TurnID: "turn:1", ToolUseID: "tool-ask", Allow: true, DeciderID: "ou-sender", BindingID: "binding-1", AppID: "app-1"}, controller.decisions[0])
	require.False(t, controller.decisions[1].Allow)
	require.NotEmpty(t, controller.decisions[1].DenyReason)
	controller.mu.Unlock()
	controller.err = core.ErrApprovalForbidden
	rec = signedPost(t, p, cardAction(actionApprove, "ou-intruder"))
	require.Contains(t, rec.Body.String(), "只有发起请求的用户")
	controller.err = core.ErrApprovalExpired
	rec = signedPost(t, p, cardAction(actionApprove, "ou-sender"))
	require.Contains(t, rec.Body.String(), "已失效")
	rec = signedPost(t, p, cardAction("unknown_action", "ou-sender"))
	require.Contains(t, rec.Body.String(), "无法识别")
}

func TestPromptApprovalSendsButtonsWithIDsOnly(t *testing.T) {
	var sent map[string]any
	p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal") {
			tokenResponse(w)
			return
		}
		require.Equal(t, "/open-apis/im/v1/messages/om-source/reply", r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&sent))
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"code":0,"data":{"message_id":"om-approval"}}`)
	})
	err := p.PromptApproval(context.Background(), core.ApprovalRequest{TurnID: "turn:1", ToolUseID: "tool-ask", ToolName: "delete_repo", InputPreview: `{"repo":"prod"}`, ApproverID: "ou-sender", ReplyTarget: core.ReplyTarget{Mode: "reply", MessageID: "om-source"}})
	require.NoError(t, err)
	require.Equal(t, "interactive", sent["msg_type"])
	content := sent["content"].(string)
	require.Contains(t, content, "delete repo", "tool names are sanitized for markdown")
	require.Contains(t, content, actionApprove)
	require.Contains(t, content, actionDeny)
	require.Contains(t, content, "tool-ask")
	require.NotContains(t, content, "ou-sender", "approver identity is enforced server-side, not embedded in the card")
}

func TestPromptApprovalEmptyChatIDFailsBeforeNetwork(t *testing.T) {
	var calls atomic.Int64
	p := newFixtureProvider(t, func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"code":400,"msg":"invalid target"}`)
	})
	err := p.PromptApproval(context.Background(), core.ApprovalRequest{TurnID: "turn", ToolUseID: "tool", ReplyTarget: core.ReplyTarget{Mode: "chat"}})
	var failure failure
	require.ErrorAs(t, err, &failure)
	require.Equal(t, "invalid_target", failure.ErrorKind())
	require.False(t, failure.Uncertain())
	require.False(t, failure.Retryable())
	require.Zero(t, calls.Load(), "invalid target must not request credentials or send a message")
}

func TestBindControlNilUnsetsControllerWithoutLosingCallback(t *testing.T) {
	p, _, _ := webhookProvider(t, "")
	controller := &recordingController{}
	p.BindControl(controller)
	first := signedPost(t, p, cardAction(actionApprove, "ou-sender"))
	require.Equal(t, http.StatusOK, first.Code)
	p.BindControl(nil)
	unset := signedPost(t, p, cardAction(actionApprove, "ou-sender"))
	require.Equal(t, http.StatusOK, unset.Code, "teardown must return a retryable toast rather than panic")
	require.Contains(t, unset.Body.String(), "稍后重试")
	controller.mu.Lock()
	require.Len(t, controller.decisions, 1, "unset must not retain the old controller")
	controller.mu.Unlock()
	p.BindControl(controller)
	again := signedPost(t, p, cardAction(actionApprove, "ou-sender"))
	require.Equal(t, http.StatusOK, again.Code)
	require.Contains(t, again.Body.String(), "已允许")
}
