// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"strings"
	"time"
)

// ApprovalRequest 是一次需要终端用户确认的工具调用。只在 evaluated_permission=ask 且 Binding
// 开启审批时产生；custom tool 由客户服务器回填，不会进入这里。
type ApprovalRequest struct {
	TurnID      string
	ReplyTarget ReplyTarget
	// ApproverID 是允许做决定的用户：单聊为发送者，群聊为触发消息的发送者。
	ApproverID string
	// ToolUseID 是 MA 的 tool_use 事件 ID，决定原样回填到 user.tool_confirmation。
	ToolUseID string
	// SessionThreadID 非空时表示该调用来自 subagent，确认需路由回该 thread。
	SessionThreadID string
	ToolName        string
	// InputPreview 是工具参数的有限长度摘要，仅供用户判断；渠道应按不可信内容渲染。
	InputPreview string
	ExpiresAt    time.Time
}

// ApprovalDecision 是用户对一次工具调用的决定。
type ApprovalDecision struct {
	TurnID     string
	ToolUseID  string
	Allow      bool
	DeciderID  string
	DenyReason string
	// BindingID / BindingEpoch / AppID / TenantKey 由宿主在验签并核对当前凭证后填入，
	// 必须与审批登记时冻结的坐标一致；Runtime 不信任卡片载荷中的这些字段。
	BindingID    string
	BindingEpoch uint64
	AppID        string
	TenantKey    string
	// ChatID 来自已验签卡片的会话上下文，拒绝转发到其他聊天后的操作。
	ChatID string
}

// ApprovalPrompter 是 Provider 可选实现的审批交互端口：展示允许/拒绝卡片，并在用户操作后通过
// Runtime.SubmitApproval 回传决定。展示失败时下一次扫描重试；始终没有决定时按超时处理，
// Runtime 不会代替用户做决定。
type ApprovalPrompter interface {
	PromptApproval(ctx context.Context, request ApprovalRequest) error
}

// maxApprovalPreview 是参数摘要的最大字符数。
const maxApprovalPreview = 300

// maxApprovalWait 限制单轮审批占用执行额度的等待上限；消息总时限仍可更早结束。
const maxApprovalWait = 10 * time.Minute

func approvalPreview(input string) string {
	redactor := toolDetailRedactor{}
	input = strings.TrimSpace(redactor.preview(input))
	runes := []rune(input)
	if len(runes) > maxApprovalPreview {
		return string(runes[:maxApprovalPreview-1]) + "…"
	}
	return input
}

// pendingApprovals 返回本轮 requires_action 停顿中需要终端用户确认、且尚未有确认结果的工具调用。
// 只认最后一个 requires_action idle 列出的 event_ids；之后出现的确认事件说明已经处理。
func pendingApprovals(events []Event, j *job) []Event {
	index := -1
	for i, event := range events {
		if event.ID == j.InputEventID {
			index = i
			break
		}
	}
	if index < 0 {
		return nil
	}
	byID := map[string]Event{}
	var waiting []string
	confirmed := map[string]bool{}
	for _, event := range events[index+1:] {
		byID[event.ID] = event
		switch {
		case event.Type == "session.status_idle" && event.StopReason == "requires_action" && onMainThread(event, j.Session.SessionID):
			waiting = event.ActionEventIDs
		case event.Type == "user.tool_confirmation":
			confirmed[event.ToolUseID] = true
		case (event.Type == "session.status_running" || event.Type == "agent.message") && onMainThread(event, j.Session.SessionID):
			waiting = nil
		}
	}
	var result []Event
	for _, id := range waiting {
		event, ok := byID[id]
		if !ok || confirmed[id] || event.Permission != "ask" || event.Type != "agent.tool_use" && event.Type != "agent.mcp_tool_use" {
			continue
		}
		result = append(result, event)
	}
	return result
}

func approvalEventID(turnID, toolUseID string) string {
	return "sevt-ch" + stableKey(turnID, "confirm", toolUseID)[:32]
}

// SubmitApproval 记录用户决定并写回 MA。决定以持久化的 Interaction 为准（只能决定一次、校验审批人
// 与时限），因此进程重启后旧卡片仍然有效。决定已记录但写回 MA 失败时，持有该轮的 Worker 会在下次
// 扫描时按同一事件 ID 补发，所以这里不把写回失败暴露给用户。
func (r *Runtime) SubmitApproval(ctx context.Context, decision ApprovalDecision) error {
	if decision.TurnID == "" || decision.ToolUseID == "" {
		return ErrApprovalExpired
	}
	interaction, err := r.store.DecideInteraction(ctx, InteractionID(decision.TurnID, decision.ToolUseID), InteractionDecision{
		Allow:        decision.Allow,
		DeciderID:    decision.DeciderID,
		DenyReason:   decision.DenyReason,
		BindingID:    decision.BindingID,
		BindingEpoch: decision.BindingEpoch,
		AppID:        decision.AppID,
		TenantKey:    decision.TenantKey,
		ChatID:       decision.ChatID,
	})
	switch {
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrApprovalExpired):
		return ErrApprovalExpired
	case err != nil:
		return err
	}
	turn, err := r.store.GetTurn(ctx, decision.TurnID)
	if err != nil || turn == nil || !turn.Status.Open() {
		return ErrApprovalExpired
	}
	status := string(interaction.Status)
	if err := r.sendConfirmation(ctx, *turn, interaction); err != nil {
		status = "confirm_failed"
	}
	r.report(ctx, Observation{Name: EventApproval, Status: status, BindingID: turn.BindingID, TurnID: turn.ID, ConversationID: turn.ConversationID, SessionID: turn.Session.SessionID})
	return nil
}

// sendConfirmation 把已决定的交互写回 MA；事件 ID 稳定，重复发送由 MA 按 tool_use_id 收敛。
func (r *Runtime) sendConfirmation(ctx context.Context, turn Turn, interaction Interaction) error {
	binding := r.connections[turn.BindingID].Binding
	if turn.Snapshot != nil {
		binding = turn.Snapshot.Binding
	}
	connection, resolveErr := r.resolveTurn(ctx, &job{Turn: turn, binding: binding})
	if resolveErr != nil {
		return resolveErr
	}
	defer releaseConnection(connection)
	if err := r.checkUserConfirmation(ctx, turn, connection); err != nil {
		return err
	}
	if mainThread(turn.Session.SessionID) == "" {
		return ErrRuntimeConfig
	}
	result := "deny"
	denyMessage := interaction.DenyReason
	switch interaction.Status {
	case InteractionAllowed:
		result = "allow"
	case InteractionExpired:
		denyMessage = "审批已超时，按拒绝处理"
	}
	callCtx, cancel := context.WithTimeout(ctx, r.options.DeliveryTimeout)
	defer cancel()
	_, err := connection.MAClient.SendToolConfirmation(callCtx, turn.Session.SessionID, ToolConfirmation{EventID: interaction.ConfirmEventID, ToolUseID: interaction.ToolUseID, Result: result, DenyMessage: denyMessage, SessionThreadID: interaction.SessionThreadID})
	return err
}

// promptApprovals 为本轮待确认的工具调用登记持久交互并展示审批卡片；同一调用只成功展示一次。
// 已决定但 MA 尚未看到确认的调用（决定后写回失败或进程崩溃）在这里补发确认。
func (r *Runtime) promptApprovals(ctx context.Context, j *job, events []Event) {
	prompter, ok := providerPort[ApprovalPrompter](r.jobConnection(j).Provider)
	if !ok || !j.binding.Approvals {
		return
	}
	for _, event := range pendingApprovals(events, j) {
		stored, err := r.approvalInteraction(j, event)
		if err != nil {
			r.reportTurn(EventApproval, "failed", "store_error", j)
			continue
		}
		if r.resumeApproval(ctx, j, stored) {
			continue
		}
		if stored.Prompted || r.promptedInProcess(j.ID, event.ID) {
			r.rememberPrompt(j.ID, event.ID)
			continue
		}
		// A legacy stored preview may be truncated inside a credential-bearing
		// JSON value. Redact the corresponding complete event before truncation.
		request := ApprovalRequest{TurnID: j.ID, ReplyTarget: ReplyTargetFor(j.msg, j.binding), ApproverID: stored.ApproverID, ToolUseID: stored.ToolUseID, SessionThreadID: stored.SessionThreadID, ToolName: stored.ToolName, InputPreview: approvalPreview(event.ToolInput), ExpiresAt: stored.ExpiresAt}
		promptCtx, cancel := context.WithTimeout(j.workContext(), r.options.DeliveryTimeout)
		if j.checkLease(promptCtx) != nil {
			cancel()
			return
		}
		err = func() (err error) {
			defer func() {
				if recover() != nil {
					err = errProgressPanic
				}
			}()
			return prompter.PromptApproval(promptCtx, request)
		}()
		cancel()
		if err != nil {
			// 展示失败时下一次扫描重试。
			r.reportTurn(EventApproval, "failed", "prompt_error", j)
			continue
		}
		stored.Prompted = true
		saveCtx, cancel := shortContext()
		_, _ = r.store.PutInteraction(saveCtx, j.Lease, stored)
		cancel()
		r.rememberPrompt(j.ID, event.ID)
		r.reportTurn(EventApproval, "prompted", "", j)
	}
}

func (r *Runtime) approvalInteraction(j *job, event Event) (Interaction, error) {
	ctx, cancel := shortContext()
	defer cancel()
	id := InteractionID(j.ID, event.ID)
	stored, err := r.store.GetInteraction(ctx, id)
	if err != nil {
		return Interaction{}, err
	}
	if stored != nil {
		return *stored, nil
	}
	now := r.options.Clock()
	expiresAt := now.Add(maxApprovalWait)
	if j.Schedule.DeadlineAt.Before(expiresAt) {
		expiresAt = j.Schedule.DeadlineAt
	}
	return r.store.PutInteraction(ctx, j.Lease, Interaction{
		ID: id, TurnID: j.ID, Kind: InteractionApproval, Status: InteractionPending,
		ApproverID: j.msg.SenderID, ToolUseID: event.ID, SessionThreadID: event.SessionThreadID,
		ToolName: toolDisplayName(event), InputPreview: approvalPreview(event.ToolInput),
		ConfirmEventID: approvalEventID(j.ID, event.ID),
		BindingID:      j.binding.ID, BindingEpoch: j.binding.BindingEpoch, AppID: j.binding.AppID,
		TenantKey: j.msg.TenantKey, ChatID: j.msg.ChatID, CreatedAt: now, ExpiresAt: expiresAt,
	})
}

// resumeApproval 以持久决定恢复工具确认；到期时先 CAS 为 expired，再用稳定事件 ID 回填拒绝。
func (r *Runtime) resumeApproval(ctx context.Context, j *job, interaction Interaction) bool {
	if interaction.Status == InteractionPending && !r.options.Clock().Before(interaction.ExpiresAt) {
		saveCtx, cancel := shortContext()
		decided, err := r.store.DecideInteraction(saveCtx, interaction.ID, InteractionDecision{
			DeciderID: interaction.ApproverID, BindingID: interaction.BindingID, BindingEpoch: interaction.BindingEpoch,
			AppID: interaction.AppID, TenantKey: interaction.TenantKey, ChatID: interaction.ChatID,
		})
		cancel()
		if err != nil && !errors.Is(err, ErrApprovalExpired) {
			r.reportTurn(EventApproval, "failed", "store_error", j)
			return true
		}
		interaction = decided
	}
	switch interaction.Status {
	case InteractionAllowed, InteractionDenied, InteractionExpired:
		r.resendConfirmation(ctx, j, interaction)
		return true
	default:
		return false
	}
}

// resendConfirmation 补发一个已持久的决定；pendingApprovals 只返回 MA 尚未看到确认的调用，
// 因此这里不会重复写入已生效的确认。
func (r *Runtime) resendConfirmation(ctx context.Context, j *job, interaction Interaction) {
	if j.checkLease(ctx) != nil {
		return
	}
	if err := r.sendConfirmation(ctx, j.Turn, interaction); err != nil {
		r.reportTurn(EventApproval, "confirm_failed", "", j)
		return
	}
	r.reportTurn(EventApproval, "confirm_resent", "", j)
}

func (r *Runtime) promptedInProcess(turnID, toolUseID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.prompted[turnID][toolUseID]
}

func (r *Runtime) rememberPrompt(turnID, toolUseID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.prompted == nil {
		r.prompted = make(map[string]map[string]bool)
	}
	if r.prompted[turnID] == nil {
		r.prompted[turnID] = make(map[string]bool)
	}
	r.prompted[turnID][toolUseID] = true
}

// forgetPrompts 在轮次结算后丢弃进程内的展示缓存；持久交互已由存储置为 expired。
func (r *Runtime) forgetPrompts(turnID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.prompted, turnID)
}

func toolDisplayName(event Event) string {
	if event.MCPServerName != "" {
		return event.MCPServerName + "/" + event.ToolName
	}
	return event.ToolName
}
