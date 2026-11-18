// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

type cardOfflineTransport struct{ calls atomic.Int64 }

func (t *cardOfflineTransport) RoundTrip(*http.Request) (*http.Response, error) {
	t.calls.Add(1)
	return nil, errors.New("network unavailable in hosted card test")
}

func hostedCardProvider(t *testing.T, binding core.Binding) (*Provider, *cardOfflineTransport) {
	t.Helper()
	transport := &cardOfflineTransport{}
	p, err := NewProvider(ProviderConfig{
		Binding: binding, AppSecretRef: "env:APP", SecretResolver: fixedSecret("fixture-secret"),
		HTTPClient: &http.Client{Transport: transport},
	})
	require.NoError(t, err)
	return p, transport
}

// hostedApprovalBinding 是宿主验签后传给卡片入口的当前绑定坐标。
func hostedApprovalBinding() core.ApprovalBinding {
	return core.ApprovalBinding{BindingID: "binding-1", BindingEpoch: 7, AppID: "app-1", TenantKey: "tenant-1"}
}

func updateCardAction(t *testing.T, change func(map[string]any)) []byte {
	t.Helper()
	var event map[string]any
	require.NoError(t, json.Unmarshal(cardAction(actionApprove, "ou-sender"), &event))
	change(event)
	raw, err := json.Marshal(event)
	require.NoError(t, err)
	return raw
}

func TestHostedCardActionUsesHostBindingNotButtonPayload(t *testing.T) {
	// 按钮载荷里可能携带伪造的 binding/epoch/tenant；入口必须只使用宿主传入的坐标。
	for _, tc := range []struct {
		name    string
		binding core.ApprovalBinding
		change  func(map[string]any)
	}{
		{name: "current binding", binding: hostedApprovalBinding()},
		{name: "retired epoch from host", binding: func() core.ApprovalBinding { b := hostedApprovalBinding(); b.BindingEpoch = 6; return b }()},
		{name: "foreign binding from host", binding: func() core.ApprovalBinding { b := hostedApprovalBinding(); b.BindingID = "binding-foreign"; return b }()},
		{name: "foreign tenant from host", binding: func() core.ApprovalBinding { b := hostedApprovalBinding(); b.TenantKey = "tenant-foreign"; return b }()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := hostedCardProvider(t, core.Binding{Owner: "owner-1", ID: "binding-1", AppID: "app-1", BindingEpoch: 7})
			controller := &recordingController{}
			p.BindControl(controller)
			raw := cardAction(actionApprove, "ou-sender")
			if tc.change != nil {
				raw = updateCardAction(t, tc.change)
			}
			response, err := p.HandleVerifiedCardAction(context.Background(), raw, tc.binding)
			require.NoError(t, err)
			require.NotNil(t, response)
			require.Len(t, controller.decisions, 1)
			got := controller.decisions[0]
			require.Equal(t, tc.binding.BindingID, got.BindingID)
			require.Equal(t, tc.binding.BindingEpoch, got.BindingEpoch)
			require.Equal(t, tc.binding.AppID, got.AppID)
			require.Equal(t, tc.binding.TenantKey, got.TenantKey)
			require.Equal(t, "ou-sender", got.DeciderID)
		})
	}
}

func TestHostedCardActionRejectsForeignApp(t *testing.T) {
	p, transport := hostedCardProvider(t, core.Binding{Owner: "owner-1", ID: "binding-1", AppID: "app-1", BindingEpoch: 7})
	controller := &recordingController{}
	p.BindControl(controller)
	raw := updateCardAction(t, func(event map[string]any) { event["header"].(map[string]any)["app_id"] = "app-foreign" })
	response, err := p.HandleVerifiedCardAction(context.Background(), raw, hostedApprovalBinding())
	require.Error(t, err)
	require.Nil(t, response)
	require.Empty(t, controller.decisions)
	require.Zero(t, transport.calls.Load())
}

func TestHostedCardActionRejectsInvalidBody(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  []byte
	}{
		{name: "empty"},
		{name: "invalid json", raw: []byte(`{`)},
		{name: "null", raw: []byte(`null`)},
		{name: "oversized", raw: bytes.Repeat([]byte("x"), maxWebhookBody+1)},
		{name: "missing envelope", raw: []byte(`{"event":{"operator":{"open_id":"ou-sender"},"action":{"value":{"action":"channel_approve","turn_id":"turn:1","tool_use_id":"tool-ask"}}}}`)},
		{name: "missing app", raw: updateCardAction(t, func(event map[string]any) { delete(event["header"].(map[string]any), "app_id") })},
		{name: "wrong event type", raw: updateCardAction(t, func(event map[string]any) { event["header"].(map[string]any)["event_type"] = "im.message.receive_v1" })},
		{name: "missing operator", raw: updateCardAction(t, func(event map[string]any) { delete(event["event"].(map[string]any), "operator") })},
		{name: "empty operator id", raw: cardAction(actionApprove, "")},
		{name: "missing action", raw: updateCardAction(t, func(event map[string]any) { delete(event["event"].(map[string]any), "action") })},
		{name: "missing turn", raw: updateCardAction(t, func(event map[string]any) {
			delete(event["event"].(map[string]any)["action"].(map[string]any)["value"].(map[string]any), "turn_id")
		})},
		{name: "unknown action", raw: cardAction("unknown", "ou-sender")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, transport := hostedCardProvider(t, core.Binding{ID: "binding-1", AppID: "app-1"})
			controller := &recordingController{}
			p.BindControl(controller)
			response, err := p.HandleVerifiedCardAction(context.Background(), tc.raw, hostedApprovalBinding())
			require.EqualError(t, err, "feishu card action invalid")
			require.Nil(t, response)
			require.Empty(t, controller.decisions)
			require.Zero(t, transport.calls.Load())
		})
	}
}

func TestHostedCardActionWithoutApprovalsShowsUnsupported(t *testing.T) {
	p, transport := hostedCardProvider(t, core.Binding{Owner: "owner-1", ID: "binding-1", AppID: "app-1", BindingEpoch: 7})
	controller := &recordingController{}
	p.BindControl(controller)
	response, err := p.HandleVerifiedCardAction(context.Background(), cardAction(actionApprove, "ou-sender"), core.ApprovalBinding{})
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Equal(t, "warning", response.Toast.Type)
	require.Equal(t, "当前渠道暂不支持卡片审批", response.Toast.Content)
	require.Empty(t, controller.decisions, "a disabled channel must not submit approval")
	require.Zero(t, transport.calls.Load())
}

func TestCardParseFailureDoesNotAffectOtherBindings(t *testing.T) {
	first, _ := hostedCardProvider(t, core.Binding{ID: "binding-1", AppID: "app-1", BindingEpoch: 7})
	second, _ := hostedCardProvider(t, core.Binding{ID: "binding-2", AppID: "app-2", BindingEpoch: 3})
	_, err := first.HandleVerifiedCardAction(context.Background(), []byte(`{`), hostedApprovalBinding())
	require.Error(t, err)
	secondRaw := updateCardAction(t, func(event map[string]any) { event["header"].(map[string]any)["app_id"] = "app-2" })
	secondBinding := core.ApprovalBinding{BindingID: "binding-2", BindingEpoch: 3, AppID: "app-2", TenantKey: "tenant-1"}
	response, err := second.HandleVerifiedCardAction(context.Background(), secondRaw, secondBinding)
	require.NoError(t, err)
	require.NotNil(t, response)
}
