// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// 面向用户的诊断回复。文本不含内部 ID，便于在群聊中直接展示。
const (
	noticeSessionRenewed = "之前的会话已结束，已为你开始新的会话。"
	replyNewSession      = "已创建新会话。"
	replyStopped         = "已停止。"
	replyAttachment      = "当前渠道不支持处理附件，请联系管理员调整配置。"
	replyHookDenied      = "暂时无法处理该请求，请稍后重试。"
	replyAgentFailed     = "抱歉，这次处理没有成功，请稍后重试。"
	replyRejected        = "抱歉，请求未被受理，请稍后重试或联系管理员。"
	replyTimeout         = "处理超时，本次请求已结束，请稍后重试。"
	replyIdleTimeout     = "任务长时间没有新进展，本次请求已结束，请稍后重试。"
	replyNeedsAction     = "Agent 需要额外确认或工具结果，当前渠道暂不支持该操作，本次请求已结束。"
	replyUnconfirmed     = "未能确认这次请求的处理结果，请稍后重试。"
	replyUnsupported     = "收到了当前 SDK 无法识别的执行结果，本次请求已结束。"
)

// job 是 Worker 持有的一次 Turn 执行：Turn 的最新持久状态，加上不可变的入站消息与冻结配置。
type job struct {
	stage            TurnStage
	terminalObserved bool
	userCredential   *UserCredentialSpec
	Turn
	msg                 Message
	binding             Binding
	connection          Connection
	guard               *taskLease
	progressErr         error
	answerImageFallback string
	disableAnswerStream bool
	answerReferences    []artifactReference
	richAnswer          []RichTextPart
}

func creationKey(j *job, generation uint64) string {
	return "channel-create:" + stableKey(j.ID, string(j.ConversationID), strconv.FormatUint(generation, 10))
}

func lookupKey(route RouteKey, generation uint64) string {
	return "channel-route:" + stableKey(string(route), strconv.FormatUint(generation, 10))
}

// interruptEventID 为本轮生成稳定的 user.interrupt ID，重试时复用。
func interruptEventID(j *job) string {
	return (RemoteOperation{TurnID: j.ID, Generation: j.Session.Generation}).InterruptEventID()
}

func versionNumber(version string) string {
	parsed, err := strconv.ParseUint(strings.TrimPrefix(version, "v"), 10, 64)
	if err != nil || parsed == 0 {
		return ""
	}
	return strconv.FormatUint(parsed, 10)
}

func matchingSession(session Session, binding Binding) bool {
	if binding.ModelEndpoint != "" && session.ModelEndpoint != binding.ModelEndpoint {
		return false
	}
	if mainThread(session.ID) == "" || session.AgentID != binding.AgentID || versionNumber(session.AgentVersion) != versionNumber(binding.AgentVersion) || session.EnvironmentID != binding.EnvironmentID {
		return false
	}
	return sameSet(session.VaultIDs, binding.VaultIDs)
}

// recoverableSession 判断找回的会话是否可采用。配置了 BuildSessionSpec 时，会话可能带有 Hook
// 追加的 Vault（创建当时的追加结果未持久化），此时只要求 binding 的 Vault 全部在会话上；
// Agent、版本、环境、路由标签与创建键仍严格匹配。
func (r *Runtime) recoverableSession(session Session, binding Binding) bool {
	if r.options.Hooks.BuildSessionSpec == nil {
		return matchingSession(session, binding)
	}
	relaxed := binding
	relaxed.VaultIDs = nil
	if !matchingSession(Session{ID: session.ID, AgentID: session.AgentID, AgentVersion: session.AgentVersion, EnvironmentID: session.EnvironmentID, ModelEndpoint: session.ModelEndpoint}, relaxed) {
		return false
	}
	have := make(map[string]bool, len(session.VaultIDs))
	for _, id := range session.VaultIDs {
		have[id] = true
	}
	for _, id := range binding.VaultIDs {
		if !have[id] {
			return false
		}
	}
	return true
}

// sameSet 判断两个 ID 列表作为集合是否相等（忽略顺序与重复）。
func sameSet(a, b []string) bool {
	left := make(map[string]bool, len(a))
	for _, id := range a {
		left[id] = true
	}
	right := make(map[string]bool, len(b))
	for _, id := range b {
		right[id] = true
		if !left[id] {
			return false
		}
	}
	return len(left) == len(right)
}

func matchingCreatedResponse(session Session, binding Binding) bool {
	if binding.ModelEndpoint != "" && session.ModelEndpoint != binding.ModelEndpoint {
		return false
	}
	if mainThread(session.ID) == "" || session.AgentID != "" && session.AgentID != binding.AgentID || session.AgentVersion != "" && versionNumber(session.AgentVersion) != versionNumber(binding.AgentVersion) || session.EnvironmentID != "" && session.EnvironmentID != binding.EnvironmentID {
		return false
	}
	if session.VaultIDs != nil {
		candidate := session
		candidate.AgentID = binding.AgentID
		candidate.AgentVersion = binding.AgentVersion
		candidate.EnvironmentID = binding.EnvironmentID
		if !matchingSession(candidate, binding) {
			return false
		}
	}
	return true
}

// boundToThisTurn 判断会话是否已绑定到本轮持久化的创建意图（崩溃恢复时据此跳过创建）。
func boundToThisTurn(conversation *Conversation, j *job) bool {
	if conversation == nil {
		return false
	}
	s := conversation.Session
	return s.State == SessionActive && s.Generation == j.Session.Generation && s.CreationKey == j.Session.CreationKey && s.LookupKey == lookupKey(j.ConversationID, j.Session.Generation) && s.SessionID != "" && s.BindingVersion == j.BindingVersion
}

// unusableSession 判断 Session 状态是否已确定不再接受输入。
func unusableSession(status string) bool {
	return status == "terminated" || status == "failed"
}

func apiError(err error) (int, string) {
	var coded APIErrorCode
	if errors.As(err, &coded) {
		return coded.HTTPStatus(), coded.ErrorCode()
	}
	var status interface{ HTTPStatus() int }
	if errors.As(err, &status) {
		return status.HTTPStatus(), ""
	}
	return 0, ""
}

// sessionGoneError 判断写入错误是否证明目标 Session 已不可写（终止或删除）。
func sessionGoneError(err error) bool {
	status, code := apiError(err)
	if status == 404 && strings.HasPrefix(code, "InvalidSession") {
		return true
	}
	return status == 403 && code == "OperationDenied.SessionTerminated"
}

// requiresActionError 判断发送是否因会话停在 requires_action 被拒绝；被拒的输入不发布、不持久化。
func requiresActionError(err error) bool {
	status, code := apiError(err)
	return status == 400 && code == "RequiresActionRejected"
}

// retryableReject 是明确未受理、可在稍后安全重发的拒绝（限流、配额、会话忙）。
func retryableReject(err error) bool {
	status, _ := apiError(err)
	return status == 409 || status == 425 || status == 429
}

func definitiveReject(err error) bool {
	status, _ := apiError(err)
	return status >= 400 && status < 500 && status != 408 && !retryableReject(err)
}

func (r *Runtime) processTurn(parent context.Context, j *job) error {
	if err := turnWorkError(j.Turn); err != nil {
		return err
	}
	if handled, err := r.reconcileIdleExpiry(j); handled {
		return err
	}
	if mayBeRunning(j) && (!j.StopRequestedAt.IsZero() || r.pastDeadline(j)) {
		cleanupCtx, cancel := context.WithTimeout(parent, r.options.DeliveryTimeout)
		connection, err := r.resolveCleanup(cleanupCtx, j)
		cancel()
		if err != nil {
			if errors.Is(err, ErrLeaseLost) || isStoreFailure(err) || isUnsupportedWork(err) {
				return err
			}
			r.reportTurn(EventInterrupt, "failed", "connection_unavailable", j)
			if !r.pastDeadline(j) {
				return r.wait(j, "interrupt_connection_unavailable")
			}
			return r.failTurn(j, "interrupt_unconfirmed")
		}
		j.connection = connection
		defer releaseConnection(connection)
	}

	if !j.StopRequestedAt.IsZero() {
		return r.stopTurn(j)
	}
	if r.pastQueueDeadline(j) {
		// 排队超时只针对尚未开始执行的 Turn；开始执行后由 TurnTimeout/MessageDeadline 接管。
		return r.giveUp(j, "queue_deadline")
	}
	if r.pastDeadline(j) {
		// 沿用最后一次等待的原因，让用户看到的回复对应真实卡点。
		kind := j.WaitReason
		if j.Phase == PhaseCreate && j.Submit.WriteAttempts > 0 {
			kind = "create_unconfirmed"
		}
		if kind == "" {
			kind = "message_deadline"
		}
		return r.giveUp(j, kind)
	}
	timeout := r.options.TurnTimeout
	if j.binding.MaxTurnDuration > 0 && j.binding.MaxTurnDuration < timeout {
		timeout = j.binding.MaxTurnDuration
	}
	if remaining := j.Schedule.DeadlineAt.Sub(r.options.Clock()); j.Phase != PhaseObserve && !j.terminalObserved && remaining < timeout {
		timeout = remaining
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	isNew := strings.TrimSpace(j.msg.Text) == "/new"
	if j.Phase == PhaseNone {
		if allowed, err := r.beforeTrigger(ctx, j); !allowed || err != nil {
			return err
		}
		conversation, err := r.store.GetConversation(ctx, j.ConversationID)
		if err != nil {
			return err
		}
		current := SessionBinding{State: SessionNone}
		if conversation != nil {
			current = conversation.Session
		}
		if current.State == SessionActive && !isNew {
			// 普通追问沿用已确认的 Session；被管理员重置的预留代需要新建。
			j.Session = TurnSession{SnapshotID: current.SnapshotID, Generation: current.Generation, SessionID: current.SessionID, CreationKey: current.CreationKey}
			if err := r.advance(ctx, j, PhaseSend); err != nil {
				return err
			}
		} else {
			generation := uint64(1)
			switch current.State {
			case SessionReserved:
				generation = current.Generation
			case SessionActive:
				generation = current.Generation + 1
			}
			if conversation != nil && conversation.GenerationHighWater >= generation {
				generation = conversation.GenerationHighWater + 1
			}
			j.Session = TurnSession{Generation: generation, CreationKey: creationKey(j, generation)}
			if err := r.advance(ctx, j, PhaseCreate); err != nil {
				return err
			}
		}
	}
	if !j.stage.Accepts(j.Phase) {
		return r.releaseStage(ctx, j)
	}
	connection, err := r.resolveTurn(ctx, j)
	if err != nil {
		if errors.Is(err, ErrLeaseLost) || isStoreFailure(err) || isUnsupportedWork(err) {
			return err
		}
		if errors.Is(err, ErrUserIdentityConflict) {
			return r.rejectUserIdentity(ctx, j)
		}
		if errors.Is(err, ErrUserVaultDeleted) {
			return r.rejectUserAuthorization(ctx, j, "user_vault_deleted")
		}
		if errors.Is(err, ErrIdentityUnsupported) {
			return r.failTurn(j, "identity_unsupported")
		}
		if errors.Is(err, ErrChannelDisabled) {
			if mayBeRunning(j) {
				cleanupCtx, cancel := context.WithTimeout(ctx, r.options.DeliveryTimeout)
				cleanup, cleanupErr := r.resolveCleanup(cleanupCtx, j)
				cancel()
				if cleanupErr != nil {
					if errors.Is(cleanupErr, ErrLeaseLost) || isStoreFailure(cleanupErr) || isUnsupportedWork(cleanupErr) {
						return cleanupErr
					}
					return r.wait(j, "interrupt_connection_unavailable")
				}
				defer releaseConnection(cleanup)
				j.connection = cleanup
				return r.abortTurn(j, cleanup, "channel_disabled")
			}
			return r.failTurn(j, "channel_disabled")
		}
		if errors.Is(err, ErrConflict) {
			return r.failTurn(j, "session_snapshot_conflict")
		}
		return r.wait(j, "connection_unavailable")
	}
	defer releaseConnection(connection)
	j.connection = connection
	if j.Phase != PhaseObserve && j.Submit.WriteAttempts == 0 {
		if ready, err := r.prepareUserIdentity(ctx, j, connection); !ready || err != nil {
			return err
		}
	}
	if j.Session.SnapshotID != "" {
		if err := j.checkLease(ctx); err != nil {
			return err
		}
		if err := r.ensureBotIdentity(ctx, connection, j.msg.ChatType); err != nil {
			if isStoreFailure(err) {
				return err
			}
			if errors.Is(err, ErrIdentityUnsupported) {
				return r.failTurn(j, "identity_unsupported")
			}
			return r.wait(j, "identity_unavailable")
		}
	}
	switch j.Phase {
	case PhaseCreate:
		return r.createThenContinue(ctx, j, connection, isNew)
	case PhaseSend:
		return r.sendAndObserve(ctx, j, connection)
	case PhaseObserve:
		return r.observe(ctx, j, connection)
	}
	return r.giveUp(j, "invalid_phase")
}

func (r *Runtime) pastDeadline(j *job) bool {
	return !j.terminalObserved && !r.options.Clock().Before(j.Schedule.DeadlineAt)
}

// pastQueueDeadline 判断 Turn 是否超过排队时限。排队时限从消息 received_at 起算，
// 只在尚未产生远程副作用（阶段未进入 create/send/observe）时判断；一旦进入 PhaseCreate，
// 表示 Session 创建流程已开始，改由 TurnTimeout/MessageDeadline 接管。
func (r *Runtime) pastQueueDeadline(j *job) bool {
	if j.Phase != PhaseNone || j.QueueDeadlineAt.IsZero() {
		return false
	}
	return !r.options.Clock().Before(j.QueueDeadlineAt)
}

// stopping 判断当前调用是否因 Runtime 停机而中止；停机时 Turn 保持原状，由下次领取继续。
func (r *Runtime) stopping() bool {
	if ctx := r.runCtx.Load(); ctx != nil {
		return (*ctx).Err() != nil
	}
	return false
}

// shortContext 为结算与进度写入提供独立于轮次超时的短时上下文，避免超时后无法落盘。
func shortContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 2*time.Second)
}

// advance 持久化当前阶段、Session 与提交记录；成功后用存储返回的最新状态刷新 job。
func (r *Runtime) advance(ctx context.Context, j *job, phase TurnPhase) error {
	return r.updateTurn(ctx, j, TurnUpdate{Phase: phase, Session: j.Session, Submit: j.Submit})
}

func (r *Runtime) updateTurn(ctx context.Context, j *job, update TurnUpdate) error {
	turn, err := r.store.UpdateTurn(ctx, j.Lease, update)
	if err != nil {
		return err
	}
	j.Turn = turn
	return nil
}

// wait 释放 Worker 并稍后重新领取；会话里后续消息仍排在本轮之后。
func (r *Runtime) wait(j *job, kind string) error {
	if r.stopping() {
		return nil
	}
	if handled, err := r.reconcileIdleExpiry(j); handled {
		return err
	}
	if r.pastDeadline(j) {
		if j.Phase == PhaseCreate && j.Submit.WriteAttempts > 0 {
			kind = "create_unconfirmed"
		}
		return r.giveUp(j, kind)
	}
	ctx, cancel := shortContext()
	defer cancel()
	r.reportTurn(EventTurnWait, "waiting", kind, j)
	return r.store.WaitTurn(ctx, j.Lease, TurnWait{Reason: kind, Until: r.options.Clock().Add(r.options.PollInterval), Submit: j.Submit})
}

// textDelivery 构造本轮的文本回复；原 Session 已切换时在开头附带提示。
func (r *Runtime) textDelivery(j *job, outputKey, text string) []Delivery {
	if notice := mountSkipNotice(j.Input.Mounts); notice != "" {
		text = strings.TrimSpace(notice + "\n\n" + text)
	}
	if j.Session.Renewed {
		if text == "" {
			text = noticeSessionRenewed
		} else {
			text = noticeSessionRenewed + "\n\n" + text
		}
	}
	if text == "" {
		return nil
	}
	return []Delivery{NewDelivery(DeliverySource{Kind: SourceTurn, ID: j.ID}, 0, outputKey, j.BindingID, ReplyTargetFor(j.msg, j.binding), TextBody(text))}
}

// settle 以一条文本回复（可为空）及额外投递结算本轮；回复与终态在同一事务内提交。
func (r *Runtime) settle(j *job, status TurnStatus, kind, outputKey, text string, extra []Delivery) error {
	replies := r.textDelivery(j, outputKey, text)
	if status == TurnDone && outputKey == "answer" && len(replies) == 1 && len(j.richAnswer) > 0 {
		replies[0].Body = RichTextBody(j.richAnswer)
	} else if status == TurnDone && outputKey == "answer" && strings.TrimSpace(text) != "" && j.binding.StreamFinalAnswer && !j.disableAnswerStream && len(replies) == 1 && CanStreamText(replies[0].Body.Text) {
		replies[0].Body = StreamTextBody(replies[0].Body.Text)
		if j.answerImageFallback != "" {
			replies[0].Body.Type = BodyImageStream
			fallback := r.textDelivery(j, outputKey, j.answerImageFallback)
			replies[0].Body.ImageFallbackText = fallback[0].Body.Text
		}
		replies[0].TextStream = &TextStreamState{}
	}
	deliveries := append(replies, extra...)
	summary := ""
	if status == TurnFailed {
		summary = failureReply(kind)
		if kind == "attachment_rejected" && text != "" {
			summary = text
		}
	}
	deliveries = append(deliveries, r.progressDelivery(j, progressStateFor(status), summary)...)
	for i := range deliveries {
		deliveries[i].Snapshot = j.Snapshot
		deliveries[i].SessionSnapshotID = j.Session.SnapshotID
	}
	ctx, cancel := shortContext()
	defer cancel()
	if err := r.store.SettleTurn(ctx, j.Lease, TurnOutcome{Status: status, ErrorKind: kind}, deliveries); err != nil {
		return err
	}
	r.forgetPrompts(j.ID)
	r.reportProgress(j, nil, progressStateFor(status), summary)
	item := Observation{Name: EventTurnSettle, Status: string(status), ErrorKind: kind, BindingID: j.BindingID, TurnID: j.ID, ConversationID: j.ConversationID, SessionID: j.Session.SessionID}
	if !j.CreatedAt.IsZero() {
		item.Duration = r.options.Clock().Sub(j.CreatedAt)
	}
	r.report(ctx, item)
	return nil
}

func failureReply(kind string) string {
	switch kind {
	case "idle_timeout":
		return replyIdleTimeout
	case "create_rejected", "send_rejected", "environment_version_conflict", "preparation_rejected":
		return replyRejected
	case "turn_timeout", "message_deadline", "session_busy", "queue_deadline":
		return replyTimeout
	case "requires_action_timeout":
		return replyNeedsAction
	case "identity_unsupported":
		return "当前渠道暂不支持所配置的执行身份，请联系管理员调整。"
	case "attachment_rejected":
		return replyAttachment
	case "trigger_hook_failed", "session_hook_failed", "enrich_hook_failed":
		return replyHookDenied
	case "history_limit":
		return "执行历史过大，当前渠道未能读取完整结果，本次请求已结束。"
	case "unsupported_contract", "input_conflict", "history_conflict", "invalid_phase":
		return replyUnsupported
	case "create_unconfirmed", "send_unconfirmed", "create_proof_conflict", "mapping_conflict", "create_response_conflict",
		"create_pending", "create_unknown", "send_pending", "send_unknown", "send_proof_missing":
		return replyUnconfirmed
	}
	return replyAgentFailed
}

// stopRequested 读取持久化的停止请求；读取失败时按未请求处理，下次轮询再看。
func (r *Runtime) stopRequested(ctx context.Context, j *job) bool {
	latest, err := r.store.GetTurn(ctx, j.ID)
	return err == nil && latest != nil && !latest.StopRequestedAt.IsZero()
}

// mayBeRunning 判断输入是否可能已在远端运行，需要在结束本轮前中断。
func mayBeRunning(j *job) bool {
	return j.Phase == PhaseObserve || j.Phase == PhaseSend && j.Submit.WriteAttempts > 0
}

// stopTurn 按用户请求结束本轮：先持久化清理意图再尽力中断，持久化失败不得结算。
// 本地 stopped 不释放远端预算；同 Session 后续输入等待独立清理屏障收敛。
func (r *Runtime) stopTurn(j *job) error {
	if mayBeRunning(j) {
		if err := r.interrupt(j, r.jobConnection(j)); err != nil {
			return err
		}
	}
	return r.settle(j, TurnStopped, "", "stopped", replyStopped, nil)
}

// failTurn 以失败结算并告知用户；本地队首推进，远端屏障仍独立保留。
func (r *Runtime) failTurn(j *job, kind string) error {
	return r.failTurnWithReply(j, kind, failureReply(kind))
}

// failTurnWithReply 复用失败结算，将受控的具体诊断随终态原子写入 Outbox。
func (r *Runtime) failTurnWithReply(j *job, kind, text string) error {
	return r.settle(j, TurnFailed, kind, "failure", text, nil)
}

// giveUp 在无法继续推进时结算：若输入可能已在运行，先尽力中断远端执行，避免后续输入被排进同一轮。
func (r *Runtime) giveUp(j *job, kind string) error {
	if mayBeRunning(j) {
		if err := r.interrupt(j, r.jobConnection(j)); err != nil {
			return err
		}
	}
	return r.failTurn(j, kind)
}

// abortTurn 中断远端执行后以失败结算。
func (r *Runtime) abortTurn(j *job, connection Connection, kind string) error {
	if err := r.interrupt(j, connection); err != nil {
		return err
	}
	return r.failTurn(j, kind)
}

func (r *Runtime) interrupt(j *job, connection Connection) error {
	if connection.MAClient == nil || mainThread(j.Session.SessionID) == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(j.workContext(), r.options.DeliveryTimeout)
	defer cancel()
	if err := j.checkLease(ctx); err != nil {
		return err
	}
	if err := r.updateTurn(ctx, j, TurnUpdate{Phase: j.Phase, Session: j.Session, Submit: j.Submit, RemoteKind: RemoteInput, RemoteCleanup: true}); err != nil {
		if errors.Is(err, ErrInterruptStarted) {
			return nil
		}
		return err
	}
	// 同步回包不证明后台取消已经执行完；后续按历史与当前 idle 尽力对账。
	if _, err := connection.MAClient.SendInterrupt(ctx, j.Session.SessionID, interruptEventID(j)); err != nil {
		r.reportTurn(EventInterrupt, "failed", "ma_error", j)
		return nil
	}
	r.reportTurn(EventInterrupt, "sent", "", j)
	return nil
}

func (r *Runtime) createThenContinue(ctx context.Context, j *job, connection Connection, isNew bool) error {
	if !j.stage.Accepts(PhaseCreate) {
		return r.releaseStage(ctx, j)
	}
	conversation, err := r.store.GetConversation(ctx, j.ConversationID)
	if err != nil {
		return err
	}
	if boundToThisTurn(conversation, j) {
		j.Session.SessionID = conversation.Session.SessionID
		return r.afterCreated(ctx, j, connection, isNew)
	}
	if conversation != nil {
		current := conversation.Session
		if current.Generation > j.Session.Generation || current.Generation == j.Session.Generation && current.State == SessionActive {
			return r.giveUp(j, "mapping_conflict")
		}
	}
	if j.Submit.WriteAttempts > 0 {
		// 已记录可能发出的创建：空列表、宽限期和重启都不证明未受理，只查账到总期限。
		session, findErr := r.findCreated(ctx, j, connection.MAClient)
		switch {
		case errors.Is(findErr, ErrLeaseLost) || isStoreFailure(findErr):
			return findErr
		case errors.Is(findErr, errHistoryConflict):
			return r.giveUp(j, "create_proof_conflict")
		case findErr != nil:
			return r.wait(j, "create_lookup_error")
		case session.ID != "":
			return r.adoptSession(ctx, j, connection, session, isNew, "adopted")
		default:
			return r.wait(j, "create_unconfirmed")
		}
	}
	snapshot, err := r.creationSnapshot(ctx, j, connection)
	if err != nil {
		if errors.Is(err, ErrLeaseLost) || isStoreFailure(err) {
			return err
		}
		if errors.Is(err, ErrIdentityUnsupported) {
			return r.failTurn(j, "identity_unsupported")
		}
		if permanent, ok := preparationFailure(err); ok {
			if permanent {
				return r.failTurn(j, "preparation_rejected")
			}
			return r.wait(j, "preparation_unavailable")
		}
		switch {
		case errors.Is(err, errSnapshotHook):
			return r.failTurn(j, "session_hook_failed")
		case errors.Is(err, errSnapshotEnvironment):
			return r.failTurn(j, "environment_unsupported")
		case errors.Is(err, ErrConflict):
			return r.failTurn(j, "session_snapshot_conflict")
		default:
			return err
		}
	}
	spec := snapshot.Spec
	identityConnection := connection
	identityConnection.Binding = spec.Binding
	if err := j.checkLease(ctx); err != nil {
		return err
	}
	if err := r.ensureBotIdentity(ctx, identityConnection, j.msg.ChatType); err != nil {
		if isStoreFailure(err) {
			return err
		}
		if errors.Is(err, ErrIdentityUnsupported) {
			return r.failTurn(j, "identity_unsupported")
		}
		return r.wait(j, "identity_unavailable")
	}
	previous := j.Submit
	j.Submit = Submission{SubmittedAt: r.options.Clock(), WriteAttempts: j.Submit.WriteAttempts + 1}
	if err := r.beginRemote(ctx, j); err != nil {
		j.Submit = previous
		if errors.Is(err, ErrRemoteLimit) || errors.Is(err, ErrRemoteBarrier) || errors.Is(err, ErrUserAuthGate) {
			return r.wait(j, "remote_budget")
		}
		return err
	}
	if err := j.checkLease(ctx); err != nil {
		return r.cancelRemoteBeforeHTTP(j, previous, err)
	}
	session, err := connection.MAClient.CreateSession(ctx, spec)
	if err != nil {
		r.reportTurn(EventSessionCreate, writeStatus(err, ctx), "", j)
		if requestNotSent(err) || retryableReject(err) {
			// 仅撤销本次已证明未发出/未受理的记录，不清除更早的未知证据。
			j.Submit = previous
			if err := r.proveRemote(j, RemoteCreate, RemoteNotAccepted, "create_rejected"); err != nil {
				return err
			}
			if err := r.persistSubmission(j); err != nil {
				return err
			}
			return r.wait(j, "create_throttled")
		}
		if definitiveReject(err) {
			if err := r.proveRemote(j, RemoteCreate, RemoteNotAccepted, "create_rejected"); err != nil {
				return err
			}
			return r.failTurn(j, "create_rejected")
		}
		return r.wait(j, "create_unknown")
	}
	expected := spec.Binding
	expected.VaultIDs = append(append([]string{}, spec.Binding.VaultIDs...), spec.Extras.VaultIDs...)
	if !matchingCreatedResponse(session, expected) {
		return r.giveUp(j, "create_response_conflict")
	}
	return r.adoptSession(ctx, j, connection, session, isNew, "created")
}

// adoptSession 绑定已确认的 Session；status 为 created（本次创建回包）或 adopted（找回）。
func (r *Runtime) adoptSession(ctx context.Context, j *job, connection Connection, session Session, isNew bool, status string) error {
	j.Session.SessionID = session.ID
	r.reportTurn(EventSessionCreate, status, "", j)
	binding := SessionBinding{SnapshotID: j.Session.SnapshotID, State: SessionActive, Generation: j.Session.Generation, SessionID: session.ID, CreationKey: j.Session.CreationKey, LookupKey: lookupKey(j.ConversationID, j.Session.Generation), BindingVersion: j.BindingVersion}
	if _, err := r.store.BindSession(ctx, j.Lease, binding); err != nil {
		if errors.Is(err, ErrConflict) {
			return r.giveUp(j, "mapping_conflict")
		}
		return err
	}
	return r.afterCreated(ctx, j, connection, isNew)
}

func (r *Runtime) afterCreated(ctx context.Context, j *job, connection Connection, isNew bool) error {
	if err := r.proveRemote(j, RemoteCreate, RemoteCreated, j.Session.SessionID); err != nil {
		return err
	}
	if isNew {
		return r.settle(j, TurnDone, "", "new", replyNewSession, nil)
	}
	j.Submit = Submission{}
	if err := r.advance(ctx, j, PhaseSend); err != nil {
		return err
	}
	return r.sendAndObserve(ctx, j, connection)
}

// findCreated 按 lookup tag 找回本代 Session。
//
// 按 lookup tag 与冻结配置找回唯一、仍可执行的候选；tag 本身不作为删除归属证明。
func (r *Runtime) findCreated(ctx context.Context, j *job, client MAClient) (Session, error) {
	return r.scanCreated(ctx, j, client, false)
}

// findCreatedProof 区分创建已确认与Session可继续使用；终止的匹配创建也能释放创建预算。
func (r *Runtime) findCreatedProof(ctx context.Context, j *job, client MAClient) (*RemoteProof, error) {
	session, err := r.scanCreated(ctx, j, client, true)
	if err != nil || session.ID == "" {
		return nil, err
	}
	kind := RemoteCreated
	if closedCreation(session) {
		kind = RemoteGone
	}
	return &RemoteProof{Kind: kind, Reference: session.ID}, nil
}

func closedCreation(session Session) bool {
	return unusableSession(session.Status) || session.Status == "deleted"
}

// scanCreated 对全部分页核验唯一候选；allowClosed仅供清理对账，不用于绑定可执行Session。
func (r *Runtime) scanCreated(ctx context.Context, j *job, client MAClient, allowClosed bool) (Session, error) {
	key := lookupKey(j.ConversationID, j.Session.Generation)
	expected := j.binding
	hasSnapshot := j.Session.SnapshotID != ""
	if hasSnapshot {
		snapshot, err := r.store.GetSessionSnapshot(ctx, j.Session.SnapshotID)
		if err != nil {
			if ctx.Err() != nil {
				return Session{}, ctx.Err()
			}
			return Session{}, NewStoreFailure(err)
		}
		if snapshot == nil || !snapshot.Valid() || snapshot.Route != j.ConversationID || snapshot.Generation != j.Session.Generation || snapshot.Spec.CreationKey != j.Session.CreationKey {
			return Session{}, errHistoryConflict
		}
		expected = snapshot.Spec.Binding.clone()
		expected.VaultIDs = append(expected.VaultIDs, snapshot.Spec.Extras.VaultIDs...)
	}
	page := ""
	seen := make(map[string]bool)
	pages := map[string]bool{"": true}
	count := 0
	var matched Session
	for i := 0; i < 100; i++ {
		result, err := client.ListSessions(ctx, key, page)
		if err != nil {
			return Session{}, err
		}
		for _, listed := range result.Sessions {
			count++
			if count > 10000 {
				return Session{}, fmt.Errorf("session history limit")
			}
			if listed.ID == "" || seen[listed.ID] {
				continue
			}
			seen[listed.ID] = true
			if listed.Tags[sessionTagKey] != key {
				continue
			}
			confirmed, err := client.GetSession(ctx, listed.ID)
			if err != nil {
				return Session{}, err
			}
			matches := r.recoverableSession(confirmed, expected)
			if hasSnapshot || allowClosed {
				matches = matchingSession(confirmed, expected)
			}
			if confirmed.ID != listed.ID || confirmed.Tags[sessionTagKey] != key || !matches || !allowClosed && closedCreation(confirmed) {
				continue
			}
			if matched.ID != "" && matched.ID != confirmed.ID {
				return Session{}, errHistoryConflict
			}
			matched = confirmed
		}
		if result.NextPage == "" {
			return matched, nil
		}
		if pages[result.NextPage] {
			return Session{}, fmt.Errorf("session history page cycle")
		}
		pages[result.NextPage] = true
		page = result.NextPage
	}
	return Session{}, fmt.Errorf("session history limit")
}

// sessionTagKey 是 Channel 写在 Session 上的路由找回标签。
const sessionTagKey = "channel_session_key"

// renewSession 在原 Session 已确定不可写时切换到下一代：保留输入身份，用新代数重新创建。
func (r *Runtime) renewSession(ctx context.Context, j *job, connection Connection) error {
	conversation, err := r.store.GetConversation(ctx, j.ConversationID)
	if err != nil {
		return err
	}
	generation := j.Session.Generation + 1
	if conversation != nil && conversation.GenerationHighWater >= generation {
		generation = conversation.GenerationHighWater + 1
	}
	if conversation != nil && conversation.Session.Generation >= generation {
		generation = conversation.Session.Generation + 1
	}
	next := TurnSession{Generation: generation, CreationKey: creationKey(j, generation), Renewed: true}
	r.reportTurn(EventSessionRenew, "renewed", "", j)
	if err := r.updateTurn(ctx, j, TurnUpdate{Phase: PhaseCreate, Session: next, Rebind: true}); err != nil {
		return err
	}
	if !j.stage.Accepts(PhaseCreate) {
		return r.releaseStage(ctx, j)
	}
	fresh, err := r.resolveTurn(ctx, j)
	if err != nil {
		if errors.Is(err, ErrUserIdentityConflict) {
			return r.rejectUserIdentity(ctx, j)
		}
		if errors.Is(err, ErrUserVaultDeleted) {
			return r.rejectUserAuthorization(ctx, j, "user_vault_deleted")
		}
		return r.wait(j, "connection_unavailable")
	}
	defer releaseConnection(fresh)
	j.connection = fresh
	if ready, err := r.prepareUserIdentity(ctx, j, fresh); !ready || err != nil {
		return err
	}
	return r.createThenContinue(ctx, j, fresh, false)
}

func acceptedResponse(response SentResponse, j *job) (bool, bool) {
	accepted := false
	for _, event := range response.Events {
		if event.ID == j.InputEventID {
			if !matchingEcho(event, j) {
				return false, true
			}
			accepted = true
		}
	}
	return accepted, false
}

func (r *Runtime) sendAndObserve(ctx context.Context, j *job, connection Connection) error {
	if !j.stage.Accepts(PhaseSend) {
		return r.releaseStage(ctx, j)
	}
	release, ok := r.trySubmit(ctx)
	if !ok {
		// 已取消的调用不代表容量不足，保留原等待原因供后续截止结算使用。
		if err := ctx.Err(); err != nil {
			return err
		}
		return r.wait(j, "submit_capacity")
	}
	defer release()

	if mainThread(j.Session.SessionID) == "" {
		return r.giveUp(j, "session_missing")
	}
	if j.Submit.WriteAttempts > 0 {
		// 上一次发送结果未知：只在历史中找回输入，不重发。MA 只在 Session 消费输入时发布
		// 它，且 loop 不按 event.id 去重 user.message，重发可能导致同一输入执行两次。
		events, err := scanEvents(ctx, connection.MAClient, j.Session.SessionID, historyWindow(j))
		if err != nil {
			if errors.Is(err, errHistoryLimit) || errors.Is(err, errHistoryConflict) {
				return r.giveUp(j, historyErrorKind(err))
			}
			return r.wait(j, historyErrorKind(err))
		}
		found, conflict := inputVisible(events, j)
		switch {
		case conflict:
			return r.giveUp(j, "input_conflict")
		case found:
			return r.beginObserving(ctx, j, connection)
		case r.options.Clock().Before(j.Submit.SubmittedAt.Add(r.options.UnknownWriteGrace)):
			return r.wait(j, "send_pending")
		}
		// 宽限期后仍未消费：giveUp 先发 user.interrupt 清掉可能滞留的排队输入，再告知用户。
		return r.giveUp(j, "send_unconfirmed")
	}
	session, err := connection.MAClient.GetSession(ctx, j.Session.SessionID)
	switch {
	case err != nil && sessionGoneError(err) && ctx.Err() == nil:
		return r.renewSession(ctx, j, connection)
	case err != nil:
		return r.wait(j, "session_lookup_error")
	case unusableSession(session.Status):
		return r.renewSession(ctx, j, connection)
	case session.Status == "running" || session.Status == "rescheduling" || session.Status == "upgrading":
		// 上一轮尚在收尾（如超时中断后）：等 Session 空闲再发，避免其终态被算到本轮。
		return r.wait(j, "session_busy")
	}
	kind, err := r.freezeInput(ctx, j, connection)
	switch {
	case kind == "attachment_rejected":
		return r.failTurnWithReply(j, kind, attachmentFailureReply(err))
	case kind != "":
		return r.wait(j, kind)
	case err != nil:
		return err
	}
	if err := r.freezeContext(ctx, j, connection, ownsSession(j)); err != nil {
		if errors.Is(err, errEnrichHook) {
			return r.failTurn(j, "enrich_hook_failed")
		}
		if errors.Is(err, errChannelContextBudget) {
			return r.failTurn(j, "context_budget_exceeded")
		}
		// 冻结写入失败（失租或存储故障）：不提交，交由 Worker 按存储错误分类处理。
		return err
	}
	if err := r.freezeMounts(ctx, j, connection, session); err != nil {
		return r.handleMountError(j, err)
	}
	j.Submit = Submission{SubmittedAt: r.options.Clock(), WriteAttempts: 1}
	if err := r.beginRemote(ctx, j); err != nil {
		j.Submit = Submission{}
		if errors.Is(err, ErrRemoteLimit) || errors.Is(err, ErrRemoteBarrier) || errors.Is(err, ErrUserAuthGate) {
			return r.wait(j, "remote_budget")
		}
		return err
	}
	if err := j.checkLease(ctx); err != nil {
		return r.cancelRemoteBeforeHTTP(j, Submission{}, err)
	}
	response, err := connection.MAClient.SendMessage(ctx, j.Session.SessionID, j.InputEventID, userInput(j))
	if err != nil {
		r.reportTurn(EventTurnSubmit, writeStatus(err, ctx), "", j)
		if requestNotSent(err) {
			return r.retryUnsent(j, "send_unavailable")
		}
		if ctx.Err() == nil || retryableReject(err) || definitiveReject(err) {
			switch {
			case sessionGoneError(err):
				if err := r.proveRemote(j, RemoteInput, RemoteGone, j.Session.SessionID); err != nil {
					return err
				}
				return r.renewSession(ctx, j, connection)
			case requiresActionError(err):
				// 上一轮停在无人应答的动作上；清理意图持久化后尽力中断，未证明消费前禁止重发。
				if err := r.interrupt(j, connection); err != nil {
					return err
				}
				return r.failTurn(j, "requires_action_rejected")
			case retryableReject(err):
				return r.retryUnsent(j, "send_throttled")
			case definitiveReject(err):
				if err := r.proveRemote(j, RemoteInput, RemoteNotAccepted, "send_rejected"); err != nil {
					return err
				}
				return r.failTurn(j, "send_rejected")
			}
		}
		return r.wait(j, "send_unknown")
	}
	accepted, conflict := acceptedResponse(response, j)
	if conflict {
		return r.giveUp(j, "input_conflict")
	}
	if !accepted {
		return r.wait(j, "send_proof_missing")
	}
	r.reportTurn(EventTurnSubmit, "accepted", "", j)
	return r.beginObserving(ctx, j, connection)
}

// ownsSession 判断当前 Session 是否由本轮创建（新建或重建）：此时本轮是该 Session 的首轮。
// 追问沿用会话中的 CreationKey，与本轮按代数推导的键不同。
func ownsSession(j *job) bool {
	return j.Session.CreationKey != "" && j.Session.CreationKey == creationKey(j, j.Session.Generation)
}

func userInput(j *job) UserInput {
	text := j.msg.Text
	if notice := mountSkipNotice(j.Input.Mounts); notice != "" {
		text += "\n[" + notice + "]"
	}
	return UserInput{Context: j.Input.Context, Text: text, Blocks: presentedAttachmentBlocks(j.Input.Blocks, j.Input.Mounts), Mounts: j.Input.Mounts}
}

// handleMountError 把挂载阶段的错误映射为可重试等待、用户侧失败或向上透传。
func (r *Runtime) handleMountError(j *job, err error) error {
	var preparation PreparationError
	switch {
	case errors.As(err, &preparation) && preparation.Permanent():
		return r.failTurn(j, "mount_rejected")
	case errors.Is(err, errSessionResourceConflict):
		return r.failTurn(j, "mount_conflict")
	default:
		// 临时错误、失租或存储故障：等待重新领取，由冻结状态决定是否重复挂载。
		return r.wait(j, "mount_unavailable")
	}
}

// writeStatus 把远程写错误归类为观测状态。
func writeStatus(err error, ctx context.Context) string {
	switch {
	case ctx.Err() != nil:
		return "unknown"
	case retryableReject(err):
		return "throttled"
	case definitiveReject(err):
		return "rejected"
	}
	return "unknown"
}

// retryUnsent 记录一次明确未受理的发送，稍后从头发送。
func (r *Runtime) retryUnsent(j *job, kind string) error {
	j.Submit = Submission{}
	if err := r.proveRemote(j, RemoteInput, RemoteNotAccepted, kind); err != nil {
		return err
	}
	if err := r.persistSubmission(j); err != nil {
		return err
	}
	return r.wait(j, kind)
}

// persistSubmission 在停机时也持久化明确未受理证明，避免重启后把本地失败当成未知写入。
func (r *Runtime) persistSubmission(j *job) error {
	ctx, cancel := shortContext()
	defer cancel()
	return r.advance(ctx, j, j.Phase)
}

func (r *Runtime) beginObserving(ctx context.Context, j *job, connection Connection) error {
	if err := r.advance(ctx, j, PhaseObserve); err != nil {
		return err
	}
	return r.observe(ctx, j, connection)
}
