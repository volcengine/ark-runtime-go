// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"time"

	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher/callback"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

var (
	_ core.ApprovalPrompter = (*Provider)(nil)
	_ core.ControlBinder    = (*Provider)(nil)
)

type controllerBox struct{ controller core.Controller }

// controlHolder 保存 Runtime 回调入口；卡片回调在 BindControl 前到达时提示稍后重试。
type controlHolder struct{ value atomic.Pointer[controllerBox] }

// BindControl 实现 core.ControlBinder；nil显式卸载回调入口。
func (p *Provider) BindControl(controller core.Controller) {
	if controller == nil {
		p.control.value.Store(nil)
		return
	}
	p.control.value.Store(&controllerBox{controller: controller})
}

const (
	actionApprove = "channel_approve"
	actionDeny    = "channel_deny"
)

var errCardActionInvalid = errors.New("feishu card action invalid")

// HandleVerifiedCardAction 解析宿主已验签、解密的完整卡片事件并提交审批。宿主必须先按当前
// 渠道 URL 校验 owner、binding 与 tenant、并验明事件 AppID 与当前 epoch 一致，再以精确
// Binding 取得 Provider；binding 坐标由参数传入，不能取自按钮载荷。
// 传入的 binding 为零值时（M1 未开放审批的连接）有效卡片返回不支持。
func (p *Provider) HandleVerifiedCardAction(ctx context.Context, raw []byte, binding core.ApprovalBinding) (*callback.CardActionTriggerResponse, error) {
	var event callback.CardActionTriggerEvent
	if p == nil || len(raw) > maxWebhookBody || json.Unmarshal(raw, &event) != nil ||
		event.EventV2Base == nil || event.EventV2Base.Header == nil ||
		event.EventV2Base.Header.EventType != "card.action.trigger" ||
		p.binding.AppID == "" || event.EventV2Base.Header.AppID != p.binding.AppID {
		return nil, errCardActionInvalid
	}
	decision, err := parseCardAction(&event, p.binding.AppID)
	if err != nil {
		return nil, err
	}
	if binding == (core.ApprovalBinding{}) {
		return toast("warning", "当前渠道暂不支持卡片审批"), nil
	}
	decision.BindingID = binding.BindingID
	decision.BindingEpoch = binding.BindingEpoch
	decision.AppID = binding.AppID
	decision.TenantKey = binding.TenantKey
	return p.submitCardAction(ctx, decision), nil
}

// PromptApproval 在原会话回复一张允许/拒绝卡片。按钮值只带任务与工具调用 ID，审批人身份
// 由 Runtime 按持久任务校验，卡片本身不承担授权。
func (p *Provider) PromptApproval(ctx context.Context, request core.ApprovalRequest) error {
	if p.client == nil || request.TurnID == "" || request.ToolUseID == "" {
		return failure{"feishu approval invalid", false, false, "invalid_approval"}
	}
	content, err := approvalCard(request, "")
	if err != nil {
		return failure{"feishu approval invalid", false, false, "invalid_approval"}
	}
	ctx, cancel := boundedContext(ctx)
	defer cancel()
	uuid := deliveryUUID("approval:" + request.TurnID + ":" + request.ToolUseID)
	target := request.ReplyTarget
	switch target.Mode {
	case "chat":
		if target.ChatID == "" {
			return failure{"feishu chat target missing", false, false, "invalid_target"}
		}
		body := larkim.NewCreateMessageReqBodyBuilder().ReceiveId(target.ChatID).MsgType("interactive").Content(content).Uuid(uuid).Build()
		resp, err := p.client.Im.Message.Create(ctx, larkim.NewCreateMessageReqBuilder().ReceiveIdType("chat_id").Body(body).Build())
		if err != nil {
			return transportFailure(err)
		}
		if resp == nil || resp.ApiResp == nil {
			return failure{"feishu response missing", true, false, "response_missing"}
		}
		return responseFailure(resp.StatusCode, resp.Code, resp.Success(), true)
	case "reply", "thread":
		body := larkim.NewReplyMessageReqBodyBuilder().MsgType("interactive").Content(content).Uuid(uuid)
		if target.Mode == "thread" {
			body.ReplyInThread(true)
		}
		resp, err := p.client.Im.Message.Reply(ctx, larkim.NewReplyMessageReqBuilder().MessageId(target.MessageID).Body(body.Build()).Build())
		if err != nil {
			return transportFailure(err)
		}
		if resp == nil || resp.ApiResp == nil {
			return failure{"feishu response missing", true, false, "response_missing"}
		}
		return responseFailure(resp.StatusCode, resp.Code, resp.Success(), true)
	}
	return failure{"feishu reply mode unsupported", false, false, "invalid_target"}
}

// approvalCard 渲染审批卡片；decided 非空时渲染为已决定状态（无按钮）。
func approvalCard(request core.ApprovalRequest, decided string) (string, error) {
	lines := []string{"Agent 请求执行工具：**" + escapeMarkdown(truncate(request.ToolName, maxStepName)) + "**"}
	if preview := strings.TrimSpace(request.InputPreview); preview != "" {
		lines = append(lines, "参数摘要："+escapeMarkdown(truncate(preview, 300)))
	}
	elements := []any{map[string]any{"tag": "div", "text": map[string]string{"tag": "lark_md", "content": strings.Join(lines, "\n")}}}
	title, template := "需要你的确认", "orange"
	if decided == "" {
		value := func(action string) map[string]string {
			return map[string]string{"action": action, "turn_id": request.TurnID, "tool_use_id": request.ToolUseID}
		}
		elements = append(elements, map[string]any{"tag": "action", "actions": []any{
			map[string]any{"tag": "button", "type": "primary", "text": map[string]string{"tag": "plain_text", "content": "允许"}, "value": value(actionApprove)},
			map[string]any{"tag": "button", "type": "danger", "text": map[string]string{"tag": "plain_text", "content": "拒绝"}, "value": value(actionDeny)},
		}})
		if !request.ExpiresAt.IsZero() {
			elements = append(elements, map[string]any{"tag": "note", "elements": []any{map[string]string{"tag": "plain_text", "content": "请在 " + request.ExpiresAt.Local().Format("15:04") + " 前处理，超时将结束本次请求"}}})
		}
	} else {
		title, template = decided, "grey"
	}
	card := map[string]any{
		"config":   map[string]any{"wide_screen_mode": true, "update_multi": true},
		"header":   map[string]any{"template": template, "title": map[string]string{"tag": "plain_text", "content": title}},
		"elements": elements,
	}
	data, err := json.Marshal(card)
	return string(data), err
}

// registerCallbacks 注册卡片交互回调：解析按钮值，校验操作人，转交 Runtime。
func (p *Provider) registerCallbacks(dispatch *dispatcher.EventDispatcher, _ func(context.Context, core.Message) error) {
	dispatch.OnP2CardActionTrigger(func(ctx context.Context, event *callback.CardActionTriggerEvent) (*callback.CardActionTriggerResponse, error) {
		return p.handleCardAction(ctx, event), nil
	})
}

// ParseVerifiedApprovalAction 只解析宿主已验签、已核对当前 owner/epoch 的卡片事件，
// 得到待提交的审批决定。绑定坐标由宿主按权威配置传入，绝不取自按钮载荷。
//
// binding 为零值表示当前渠道未开启审批（M2 开关关闭或旧 epoch 连接），此时不提交，
// 返回的 toast 用于直接应答飞书；返回的 decision 为零值。非审批卡片或载荷非法返回错误。
// 本方法不访问 Runtime，提交由宿主调用 Runtime.SubmitApproval 完成。
func (p *Provider) ParseVerifiedApprovalAction(ctx context.Context, raw []byte, binding core.ApprovalBinding) (core.ApprovalDecision, *callback.CardActionTriggerResponse, error) {
	_ = ctx
	var event callback.CardActionTriggerEvent
	if p == nil || len(raw) > maxWebhookBody || json.Unmarshal(raw, &event) != nil ||
		event.EventV2Base == nil || event.EventV2Base.Header == nil ||
		event.EventV2Base.Header.EventType != "card.action.trigger" ||
		p.binding.AppID == "" || event.EventV2Base.Header.AppID != p.binding.AppID {
		return core.ApprovalDecision{}, nil, errCardActionInvalid
	}
	decision, err := parseCardAction(&event, p.binding.AppID)
	if err != nil {
		return core.ApprovalDecision{}, nil, err
	}
	if binding == (core.ApprovalBinding{}) {
		return core.ApprovalDecision{}, toast("warning", "当前渠道暂不支持卡片审批"), nil
	}
	if binding.BindingID == "" || binding.BindingID != p.binding.ID || binding.BindingEpoch != p.binding.BindingEpoch ||
		binding.AppID != p.binding.AppID || binding.TenantKey == "" || binding.TenantKey != event.EventV2Base.Header.TenantKey {
		return core.ApprovalDecision{}, nil, errCardActionInvalid
	}
	if tenant := eventTenant(&event); tenant != "" && tenant != binding.TenantKey {
		return core.ApprovalDecision{}, nil, errCardActionInvalid
	}
	decision.BindingID = binding.BindingID
	decision.BindingEpoch = binding.BindingEpoch
	decision.AppID = binding.AppID
	decision.TenantKey = binding.TenantKey
	return decision, nil, nil
}

// ApprovalToast 暴露卡片 toast 应答渲染，供托管宿主在提交决定后使用与自运行一致的文案。
func ApprovalToast(kind, text string) *callback.CardActionTriggerResponse {
	return toast(kind, text)
}

// ApprovalDecidedCard 暴露“已允许/已拒绝/已失效”状态卡片渲染，供托管宿主复用。
func ApprovalDecidedCard(toolUseID, title string) *callback.CardActionTriggerResponse {
	return decidedCard(toolUseID, title)
}

// submitCardAction 把已补齐绑定坐标的决定交给 Runtime，并按结果渲染卡片。
func (p *Provider) submitCardAction(ctx context.Context, decision core.ApprovalDecision) *callback.CardActionTriggerResponse {
	box := p.control.value.Load()
	if box == nil {
		return toast("warning", "服务正在启动，请稍后重试")
	}
	callCtx, cancel := context.WithTimeout(ctx, 2500*time.Millisecond)
	defer cancel()
	err := box.controller.SubmitApproval(callCtx, decision)
	switch {
	case errors.Is(err, core.ErrApprovalForbidden):
		return toast("warning", "只有发起请求的用户在当前渠道版本中可以确认")
	case errors.Is(err, core.ErrApprovalExpired):
		return decidedCard(decision.ToolUseID, "已失效")
	case err != nil:
		return toast("error", "提交失败，请稍后重试")
	}
	if decision.Allow {
		return decidedCard(decision.ToolUseID, "已允许")
	}
	return decidedCard(decision.ToolUseID, "已拒绝")
}

func toast(kind, text string) *callback.CardActionTriggerResponse {
	return &callback.CardActionTriggerResponse{Toast: &callback.Toast{Type: kind, Content: text}}
}

func (p *Provider) handleCardAction(ctx context.Context, event *callback.CardActionTriggerEvent) *callback.CardActionTriggerResponse {
	decision, err := parseCardAction(event, p.binding.AppID)
	if err != nil {
		return toast("error", "无法识别的操作")
	}
	// 自运行 WebSocket 模式：当前连接即绑定坐标。
	decision.BindingID = p.binding.ID
	decision.BindingEpoch = p.binding.BindingEpoch
	decision.AppID = p.binding.AppID
	if tenant := eventTenant(event); tenant != "" {
		decision.TenantKey = tenant
	}
	return p.submitCardAction(ctx, decision)
}

// eventTenant 读取卡片操作人的租户键；缺失时返回空串，由 Runtime 的绑定校验拒绝。
func eventTenant(event *callback.CardActionTriggerEvent) string {
	if event == nil || event.Event == nil || event.Event.Operator == nil || event.Event.Operator.TenantKey == nil {
		return ""
	}
	return *event.Event.Operator.TenantKey
}

// parseCardAction 只解析操作与操作人；审批归属、时限和单次决定仍由 Runtime 校验。
// 返回的决定不包含绑定坐标，由调用方按自托管连接或宿主权威配置补齐。
func parseCardAction(event *callback.CardActionTriggerEvent, appID string) (core.ApprovalDecision, error) {
	if event == nil || event.Event == nil || event.Event.Action == nil || event.Event.Operator == nil || event.Event.Operator.OpenID == "" {
		return core.ApprovalDecision{}, errCardActionInvalid
	}
	if event.EventV2Base != nil && event.EventV2Base.Header != nil && event.EventV2Base.Header.AppID != "" && event.EventV2Base.Header.AppID != appID {
		return core.ApprovalDecision{}, errCardActionInvalid
	}
	values := event.Event.Action.Value
	action, _ := values["action"].(string)
	turnID, _ := values["turn_id"].(string)
	toolUseID, _ := values["tool_use_id"].(string)
	if action != actionApprove && action != actionDeny || turnID == "" || toolUseID == "" {
		return core.ApprovalDecision{}, errCardActionInvalid
	}
	// 按钮载荷中的 binding/epoch/tenant 一律不作为授权依据；宿主必须自行核对。
	allow := action == actionApprove
	decision := core.ApprovalDecision{TurnID: turnID, ToolUseID: toolUseID, Allow: allow, DeciderID: event.Event.Operator.OpenID}
	if event.Event.Context != nil {
		decision.ChatID = event.Event.Context.OpenChatID
	}
	if !allow {
		decision.DenyReason = "用户在飞书拒绝了该操作"
	}
	return decision, nil
}

func decidedCard(toolUseID, title string) *callback.CardActionTriggerResponse {
	content, err := approvalCard(core.ApprovalRequest{ToolUseID: toolUseID, ToolName: "（已处理）"}, title)
	response := toast("success", title)
	if err == nil {
		var data any
		if json.Unmarshal([]byte(content), &data) == nil {
			response.Card = &callback.Card{Type: "raw", Data: data}
		}
	}
	return response
}
