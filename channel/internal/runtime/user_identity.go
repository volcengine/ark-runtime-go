// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"time"
)

const userAuthTimeout = 10 * time.Minute

// prepareUserIdentity 仅在新的远端执行前准备本人授权；返回 false 表示已持久等待或结算。
func (r *Runtime) prepareUserIdentity(ctx context.Context, j *job, c Connection) (bool, error) {
	j.userCredential = nil
	if effectiveIdentity(c.Binding, j.msg.ChatType) != IdentityUser {
		return true, nil
	}
	auth, ok := r.store.(UserAuthStore)
	if !ok || c.UserAuthorizer == nil {
		return false, r.failTurn(j, "identity_unsupported")
	}
	p := userPrincipal(c.Binding, j.msg)
	if !p.Valid() {
		return false, r.finishUserFailure(j, "invalid_subject")
	}
	scopes := userScopes(j.binding.UserAuthScopes, nil)
	var credential *UserCredentialSpec
	if j.Session.SnapshotID != "" {
		snapshot, err := r.store.GetSessionSnapshot(ctx, j.Session.SnapshotID)
		if err != nil {
			return false, err
		}
		if snapshot == nil || snapshot.Spec.UserCredential == nil {
			return false, r.finishUserFailure(j, "invalid_subject")
		}
		credential = snapshot.Spec.UserCredential
		if credential.Principal != p {
			return false, r.finishUserFailure(j, "invalid_subject")
		}
	}
	record, err := auth.GetUserAuth(ctx, p)
	if err != nil {
		return false, err
	}
	if credential == nil && record != nil {
		if handled, recoverErr := r.reauthorizeDeletedUserCredentialForNew(ctx, j, c, *record); handled || recoverErr != nil {
			return false, recoverErr
		}
		if handled, recoverErr := r.replaceDeletedUserVaultForNew(ctx, j, c, *record); handled || recoverErr != nil {
			return false, recoverErr
		}
	}
	vaultID := ""
	if credential != nil {
		vaultID = credential.VaultID
	} else if record != nil {
		vaultID = record.VaultID
	}
	if vaultID == "" {
		vaultID, err = r.userVault(ctx, c, p)
		if err != nil {
			if errors.Is(err, ErrVaultPrincipalMismatch) {
				return false, r.finishUserFailure(j, "invalid_subject")
			}
			return false, r.wait(j, "user_vault_unavailable")
		}
	}
	query := UserGrantQuery{Principal: p, VaultID: vaultID, RequiredScopes: scopes}
	if credential != nil {
		query.CredentialID = credential.CredentialID
		query.FlowID = credential.FlowID
	}
	if record != nil {
		if record.VaultID != vaultID {
			return false, r.finishUserFailure(j, "invalid_subject")
		}
		if query.CredentialID == "" {
			query.CredentialID = record.CredentialID
			if query.CredentialID == "" {
				query.CredentialID = record.Grant.CredentialID
			}
		}
		if record.CredentialID != "" && record.CredentialID != query.CredentialID || record.Grant.CredentialID != "" && record.Grant.CredentialID != query.CredentialID {
			return false, r.finishUserFailure(j, "invalid_subject")
		}
		if record.Grant.FlowID != "" {
			query.FlowID = record.Grant.FlowID
		}
		if j.AuthWait != nil && record.State == UserAuthReady && !r.options.Clock().Before(j.AuthWait.DeadlineAt) {
			return false, r.finishUserFailure(j, "user_auth_timeout")
		}
		if j.AuthWait != nil && record.Attempt != j.AuthWait.Attempt && j.AuthWait.Attempt != 0 {
			return false, r.finishUserFailure(j, "user_auth_failed")
		}
		if record.Operation == UserAuthRefresh && record.State == UserAuthVerifying {
			if record.Lease.Live(r.options.Clock()) {
				return false, r.wait(j, "user_auth_busy")
			}
			return false, r.failUserAuth(j, *record, "user_auth_uncertain")
		}
		if record.State == UserAuthUncertain || record.State == UserAuthFailed || record.State == UserAuthInvalidSubject {
			if handled, noticeErr := r.noticeDeletedUserCredential(ctx, j, c, record); handled || noticeErr != nil {
				return false, noticeErr
			}
		}
		if record.State == UserAuthUncertain {
			return false, r.failUserAuth(j, *record, "user_auth_uncertain")
		}
		if j.AuthWait != nil && (record.State == UserAuthFailed || record.State == UserAuthInvalidSubject) {
			return false, r.finishUserFailure(j, record.State)
		}
	}
	if record != nil && record.Batch > 0 && record.State != UserAuthReady {
		scopes = cloneStrings(record.RequiredScopes)
		query.RequiredScopes = scopes
	}
	observedRevision := uint64(0)
	if record != nil {
		observedRevision = record.Revision
	}
	// pending/verifying 的恢复只查询原 flow，不重新 Lookup 后绕过本次授权。
	if record == nil || record.State == UserAuthReady {
		grant, lookupErr := c.UserAuthorizer.Lookup(ctx, query)
		if lookupErr != nil || grant.Status != AuthReady {
			if handled, noticeErr := r.noticeDeletedUserCredential(ctx, j, c, record); handled || noticeErr != nil {
				return false, noticeErr
			}
		}
		if lookupErr != nil {
			return false, r.wait(j, "user_auth_unavailable")
		}
		if grant.Status == AuthReady && record != nil && record.State == UserAuthReady && r.userReadyExpired(j, record) {
			return false, r.finishUserFailure(j, "user_auth_timeout")
		}
		if grant.Status == AuthReady && validUserGrant(grant, query) && record != nil && record.State == UserAuthReady && sameUserGrant(record.Grant, grant) {
			j.userCredential = credentialFor(p, grant)
			return true, nil
		}
		if grant.Status == AuthUnknown {
			latest, e := auth.GetUserAuth(ctx, p)
			if e != nil {
				return false, e
			}
			if latest != nil && latest.State == UserAuthUncertain {
				return false, r.failUserAuth(j, *latest, "user_auth_uncertain")
			}
		}
		// 只有取得持久 gate 后，才能变更授权状态或开始重授。
		held, beginErr := auth.BeginUserAuth(ctx, UserAuthBegin{ExpectedRevision: &observedRevision, Principal: p, VaultID: vaultID, Owner: r.instanceID + ":" + j.ID, TTL: r.options.LeaseTTL})
		if beginErr != nil {
			return false, r.userAuthBusy(j, beginErr)
		}
		if grant.Status == AuthUnknown {
			return false, r.failUserAuth(j, held, "user_auth_uncertain")
		}
		if grant.Status == AuthSubjectMismatch || grant.Status == UserAuthInvalidSubject {
			return false, r.failUserAuth(j, held, "invalid_subject")
		}
		if grant.Status == AuthReady && validUserGrant(grant, query) {
			return r.acceptUserGrant(ctx, j, held, grant)
		}
		return false, r.startUserAuth(ctx, j, c, held, query)
	}
	held, err := auth.BeginUserAuth(ctx, UserAuthBegin{ExpectedRevision: &observedRevision, Principal: p, VaultID: vaultID, Owner: r.instanceID + ":" + j.ID, TTL: r.options.LeaseTTL})
	if err != nil {
		return false, r.userAuthBusy(j, err)
	}
	if held.FlowID == "" {
		if held.State == UserAuthVerifying {
			return false, r.failUserAuth(j, held, "user_auth_start_unconfirmed")
		}
		return false, r.startUserAuth(ctx, j, c, held, query)
	}
	return r.pollUserAuth(ctx, j, c, held, query)
}

func (r *Runtime) userAuthBusy(j *job, err error) error {
	if errors.Is(err, ErrUserAuthGate) || errors.Is(err, ErrRemoteBarrier) || errors.Is(err, ErrConflict) {
		return r.wait(j, "user_auth_busy")
	}
	return err
}

func userScopes(a, b []string) []string {
	out := append(cloneStrings(a), b...)
	slices.Sort(out)
	return slices.Compact(out)
}
func sameUserGrant(a, b UserGrant) bool {
	return a.VaultID == b.VaultID && a.CredentialID == b.CredentialID && a.GrantVersion == b.GrantVersion
}
func validUserGrant(g UserGrant, q UserGrantQuery) bool {
	if g.Status != AuthReady || g.VaultID != q.VaultID || g.CredentialID == "" || g.GrantVersion == "" || q.CredentialID != "" && q.CredentialID != g.CredentialID {
		return false
	}
	for _, scope := range q.RequiredScopes {
		if !slices.Contains(g.GrantedScopes, scope) {
			return false
		}
	}
	return true
}
func credentialFor(p UserPrincipal, g UserGrant) *UserCredentialSpec {
	return &UserCredentialSpec{Principal: p, VaultID: g.VaultID, CredentialID: g.CredentialID, GrantVersion: g.GrantVersion, FlowID: g.FlowID, ExpiresAt: g.ExpiresAt}
}

func (r *Runtime) acceptUserGrant(ctx context.Context, j *job, held UserAuthRecord, g UserGrant) (bool, error) {
	update := UserAuthUpdate{Principal: held.Principal, Lease: held.Lease, Revision: held.Revision, State: UserAuthReady, FlowID: g.FlowID, Grant: g}
	if r.userReadyExpired(j, &held) {
		finishCtx, cancel := shortContext()
		defer cancel()
		err := r.store.(UserAuthStore).FailUserAuth(finishCtx, j.Lease, UserAuthFailure{Update: update, Outcome: TurnOutcome{Status: TurnFailed, ErrorKind: "user_auth_timeout"}, Deliveries: r.textDelivery(j, "user-auth-failed", userFailureText(j, "user_auth_timeout"))})
		return false, err
	}
	_, err := r.store.(UserAuthStore).UpdateUserAuth(ctx, update)
	if err != nil {
		return false, err
	}
	if r.userReadyExpired(j, &held) {
		return false, r.finishUserFailure(j, "user_auth_timeout")
	}
	j.userCredential = credentialFor(held.Principal, g)
	return true, nil
}

// userReadyExpired 在外部调用返回后复核当前请求期限；旧 ready flow 的期限不限制后续新请求。
func (r *Runtime) userReadyExpired(j *job, held *UserAuthRecord) bool {
	now := r.options.Clock()
	expired := func(at time.Time) bool { return !at.IsZero() && !now.Before(at) }
	if j.Phase != PhaseObserve && expired(j.Schedule.DeadlineAt) {
		return true
	}
	if j.AuthWait != nil && expired(j.AuthWait.DeadlineAt) {
		return true
	}
	return held != nil && (held.State != UserAuthReady || j.AuthWait != nil) && expired(held.ExpiresAt)
}

// latestUserAuth 接受 adapter checkpoint 推进的 revision，但绝不能借用别人的 gate lease。
func (r *Runtime) latestUserAuth(ctx context.Context, held UserAuthRecord) (UserAuthRecord, error) {
	latest, err := r.store.(UserAuthStore).GetUserAuth(ctx, held.Principal)
	if err != nil {
		return held, err
	}
	if latest == nil || latest.Lease.Owner != held.Lease.Owner || latest.Lease.Fence != held.Lease.Fence {
		return held, ErrLeaseLost
	}
	return *latest, nil
}

func (r *Runtime) startUserAuth(ctx context.Context, j *job, c Connection, held UserAuthRecord, q UserGrantQuery) error {
	deadline := r.options.Clock().Add(userAuthTimeout)
	if !held.ExpiresAt.IsZero() {
		deadline = held.ExpiresAt
	}
	var err error
	if held.Batch == 0 && len(q.RequiredScopes) > 0 {
		batch := cloneStrings(q.RequiredScopes)
		if len(batch) > UserAuthScopeBatchLimit {
			batch = batch[:UserAuthScopeBatchLimit]
		}
		held, err = r.store.(UserAuthStore).UpdateUserAuth(ctx, UserAuthUpdate{Principal: held.Principal, Lease: held.Lease, Revision: held.Revision, State: UserAuthVerifying, BeginBatch: true, RequiredScopes: q.RequiredScopes, BatchScopes: batch, ExpiresAt: deadline})
		if err != nil {
			return err
		}
	}
	if held.Batch > 0 {
		q.RequiredScopes = cloneStrings(held.BatchScopes)
	}
	if held.ExpiresAt.IsZero() {
		held, err = r.store.(UserAuthStore).UpdateUserAuth(ctx, UserAuthUpdate{Principal: held.Principal, Lease: held.Lease, Revision: held.Revision, State: UserAuthVerifying, ExpiresAt: deadline})
		if err != nil {
			return err
		}
	}
	callCtx, cancel := context.WithTimeout(ctx, r.options.DeliveryTimeout)
	flow, startErr := c.UserAuthorizer.Start(callCtx, held.Principal, AuthRequest{VaultID: q.VaultID, ExistingCredentialID: q.CredentialID, Attempt: held.Attempt, Lease: held.Lease, Revision: held.Revision, RequiredScopes: q.RequiredScopes})
	cancel()
	held, err = r.latestUserAuth(ctx, held)
	if err != nil {
		return err
	}
	if startErr != nil {
		return r.failUserAuth(j, held, "user_auth_start_unconfirmed")
	}
	if flow.ID == "" {
		return r.failUserAuth(j, held, "user_auth_start_unconfirmed")
	}
	if held.ExpiresAt.Before(deadline) {
		deadline = held.ExpiresAt
	}
	if !flow.ExpiresAt.IsZero() && flow.ExpiresAt.Before(deadline) {
		deadline = flow.ExpiresAt
	}
	// Hosted flow 首次登记必须与提示同事务；Device 已提前加密持久化，可用只读端口恢复。
	held.FlowID, held.ExpiresAt = flow.ID, deadline
	prompt := r.userAuthPrompt(j, held, flow)
	return r.waitUserAuthFlow(j, held, time.Time{}, &prompt, true)
}

func (r *Runtime) pollUserAuth(ctx context.Context, j *job, c Connection, held UserAuthRecord, q UserGrantQuery) (bool, error) {
	if !r.options.Clock().Before(held.ExpiresAt) {
		if held.State == UserAuthVerifying && held.Secret != nil {
			return false, r.failUserAuth(j, held, "user_auth_uncertain")
		}
		return false, r.failUserAuth(j, held, "user_auth_timeout")
	}
	if j.AuthWait == nil || j.AuthWait.Batch < held.Batch {
		if reader, ok := c.UserAuthorizer.(UserAuthPromptReader); ok {
			callCtx, cancel := context.WithTimeout(ctx, r.options.DeliveryTimeout)
			flow, promptErr := reader.AuthPrompt(callCtx, AuthFlowRef{Principal: held.Principal, VaultID: held.VaultID, FlowID: held.FlowID, Lease: held.Lease, Revision: held.Revision})
			cancel()
			if promptErr != nil {
				if errors.Is(promptErr, ErrLeaseLost) || isStoreFailure(promptErr) || isUnsupportedWork(promptErr) {
					return false, promptErr
				}
				// 尚未恢复提示，不能创建 AuthWait 后直接转入 Status；只归还原 gate 并退避重试。
				_, err := r.store.(UserAuthStore).UpdateUserAuth(ctx, UserAuthUpdate{Principal: held.Principal, Lease: held.Lease, Revision: held.Revision, State: held.State, Release: true})
				if err != nil {
					return false, err
				}
				return false, r.wait(j, "user_auth_prompt_unavailable")
			}
			if flow.ID != held.FlowID || flow.URL == "" {
				return false, r.failUserAuth(j, held, "user_auth_failed")
			}
			prompt := r.userAuthPrompt(j, held, flow)
			return false, r.waitUserAuthFlow(j, held, time.Time{}, &prompt, true)
		}
	}
	callCtx, cancel := context.WithTimeout(ctx, r.options.DeliveryTimeout)
	status, statusErr := c.UserAuthorizer.Status(callCtx, AuthFlowRef{Principal: held.Principal, VaultID: held.VaultID, FlowID: held.FlowID, Lease: held.Lease, Revision: held.Revision, RequiredScopes: currentBatchScopes(held, q.RequiredScopes)})
	cancel()
	var err error
	held, err = r.latestUserAuth(ctx, held)
	if err != nil {
		return false, err
	}
	if statusErr != nil {
		return false, r.waitUserAuth(j, held, time.Time{}, nil)
	}
	switch status.Status {
	case AuthReady:
		grant := status.Grant
		if grant.FlowID != held.FlowID || !validUserGrant(grant, UserGrantQuery{VaultID: q.VaultID, CredentialID: q.CredentialID, RequiredScopes: userScopes(held.VerifiedScopes, currentBatchScopes(held, q.RequiredScopes))}) {
			return false, r.failUserAuth(j, held, "user_auth_failed")
		}
		if held.Batch > 0 && !validUserGrant(grant, UserGrantQuery{VaultID: q.VaultID, CredentialID: q.CredentialID, RequiredScopes: held.RequiredScopes}) {
			if r.userReadyExpired(j, &held) {
				return false, r.failUserAuth(j, held, "user_auth_timeout")
			}
			missing := []string{}
			for _, scope := range held.RequiredScopes {
				if !slices.Contains(grant.GrantedScopes, scope) {
					missing = append(missing, scope)
				}
			}
			if len(missing) > UserAuthScopeBatchLimit {
				missing = missing[:UserAuthScopeBatchLimit]
			}
			next, e := r.store.(UserAuthStore).UpdateUserAuth(ctx, UserAuthUpdate{Principal: held.Principal, Lease: held.Lease, Revision: held.Revision, State: UserAuthVerifying, BeginBatch: true, RequiredScopes: held.RequiredScopes, BatchScopes: missing, BatchGrant: grant})
			if e != nil {
				return false, e
			}
			q.CredentialID = grant.CredentialID
			return false, r.startUserAuth(ctx, j, c, next, q)
		}
		return r.acceptUserGrant(ctx, j, held, grant)
	case AuthPending:
		return false, r.waitUserAuth(j, held, status.NextPollAt, nil)
	case AuthSubjectMismatch, UserAuthInvalidSubject:
		return false, r.failUserAuth(j, held, "invalid_subject")
	case AuthExpired:
		return false, r.failUserAuth(j, held, "user_auth_timeout")
	case AuthDenied:
		return false, r.failUserAuth(j, held, "user_auth_denied")
	case AuthFailed, AuthReauthRequired:
		return false, r.failUserAuth(j, held, "user_auth_failed")
	default:
		return false, r.failUserAuth(j, held, "user_auth_uncertain")
	}
}

func (r *Runtime) userAuthPrompt(j *job, held UserAuthRecord, flow AuthFlow) Delivery {
	text := "请使用发送本条消息的飞书账号完成授权，授权后将继续处理原请求：\n[点击授权](" + flow.URL + ")"
	if flow.UserCode != "" {
		text += "\n授权码：" + flow.UserCode
	}
	return NewDelivery(DeliverySource{Kind: SourceProgress, ID: j.ID}, int(held.Attempt), fmt.Sprintf("user-auth-prompt-%d", held.Batch), j.BindingID, ReplyTargetFor(j.msg, j.binding), TextBody(text))
}
func (r *Runtime) waitUserAuth(j *job, held UserAuthRecord, next time.Time, prompt *Delivery) error {
	return r.waitUserAuthFlow(j, held, next, prompt, false)
}

// maxUserAuthPollDelay 是授权复查间隔的上限，退避与抖动都不能越过。
const maxUserAuthPollDelay = 15 * time.Second

// userAuthPollDelay 返回第 poll 次复查前的等待：从 base 起逐次翻倍，乘以 jitter 后截断到上限。
func userAuthPollDelay(base time.Duration, poll uint64, jitter float64) time.Duration {
	delay := base
	for n := uint64(0); n < poll && delay < maxUserAuthPollDelay; n++ {
		delay *= 2
	}
	delay = min(delay, maxUserAuthPollDelay)
	return min(time.Duration(float64(delay)*jitter), maxUserAuthPollDelay)
}

func (r *Runtime) waitUserAuthFlow(j *job, held UserAuthRecord, next time.Time, prompt *Delivery, register bool) error {
	poll := uint64(0)
	if j.AuthWait != nil {
		poll = j.AuthWait.PollAttempt + 1
	}
	delay := userAuthPollDelay(r.options.UserAuthPollInterval, poll, 0.8+rand.Float64()*0.4)
	until := r.options.Clock().Add(delay)
	if next.After(until) {
		until = next
	}
	deadline := held.ExpiresAt
	if j.AuthWait != nil {
		deadline = j.AuthWait.DeadlineAt
	}
	ctx, cancel := shortContext()
	defer cancel()
	return r.store.(UserAuthStore).WaitForUserAuth(ctx, j.Lease, UserAuthWait{Batch: held.Batch, RegisterFlow: register, Principal: held.Principal, Lease: held.Lease, Revision: held.Revision, Attempt: held.Attempt, PollAttempt: poll, FlowID: held.FlowID, DeadlineAt: deadline, Until: until, Prompt: prompt})
}

func userFailureText(j *job, kind string) string {
	text := "授权未完成，本次请求未继续执行。请重新发送请求并完成授权。"
	if kind == "user_vault_deleted" || kind == "user_credential_deleted" {
		text = "个人授权凭据已失效或被删除，本次请求未继续执行。请发送 /new 重新授权并新建会话。"
	}
	if kind == "invalid_subject" {
		text = "授权账号与发消息的账号不符，本次请求未继续执行。请重新发送请求，并使用发送消息的飞书账号完成授权。"
	}
	if kind == "user_auth_start_unconfirmed" {
		text = "授权发起未确认，本次请求未继续执行。请重新发送请求并完成授权。"
	}
	if kind == "user_auth_timeout" {
		text = "授权已超时，本次请求未继续执行。请重新发送请求并完成授权。"
	}
	if kind == "user_auth_uncertain" {
		text = "无法确认授权结果，本次请求未继续执行。请联系管理员确认授权状态后重新发送请求。"
	}
	if kind == "user_auth_denied" {
		text = "本次授权已被拒绝，请重新发送请求并完成授权。"
	}
	if j.Session.SessionID != "" {
		text += "原会话已保留。"
	}
	return text
}
func (r *Runtime) finishUserFailure(j *job, kind string) error {
	return r.settle(j, TurnFailed, kind, "", "", r.textDelivery(j, "user-auth-failed", userFailureText(j, kind)))
}
func (r *Runtime) failUserAuth(j *job, held UserAuthRecord, kind string) error {
	state := UserAuthFailed
	if kind == "invalid_subject" {
		state = UserAuthInvalidSubject
	}
	if kind == "user_auth_uncertain" {
		state = UserAuthUncertain
	}
	return r.settleUserAuthFailure(j, held, state, kind)
}

func (r *Runtime) settleUserAuthFailure(j *job, held UserAuthRecord, state, kind string) error {
	if held.State == UserAuthUncertain && held.Lease.Owner == "" {
		held.Lease = Lease{}
	}
	ctx, cancel := shortContext()
	defer cancel()
	deliveries := r.textDelivery(j, "user-auth-failed", userFailureText(j, kind))
	for i := range deliveries {
		deliveries[i].Snapshot = j.Snapshot
		deliveries[i].SessionSnapshotID = j.Session.SnapshotID
	}
	return r.store.(UserAuthStore).FailUserAuth(ctx, j.Lease, UserAuthFailure{Update: UserAuthUpdate{Principal: held.Principal, Lease: held.Lease, Revision: held.Revision, State: state}, Outcome: TurnOutcome{Status: TurnFailed, ErrorKind: kind}, Deliveries: deliveries})
}

// checkUserConfirmation 只让仍为 ready 的原 Session 本人凭据执行确认；Bot 和清理独立运行。
func (r *Runtime) checkUserConfirmation(ctx context.Context, turn Turn, c Connection) error {
	if effectiveIdentity(c.Binding, "p2p") != IdentityUser {
		return nil
	}
	snapshot, err := r.store.GetSessionSnapshot(ctx, turn.Session.SnapshotID)
	if err != nil {
		return err
	}
	if snapshot == nil {
		return ErrUserAuthGate
	}
	if effectiveIdentity(snapshot.Spec.Binding, snapshot.Spec.ChatType) != IdentityUser {
		return nil
	}
	auth, ok := r.store.(UserAuthStore)
	if !ok || c.UserAuthorizer == nil || snapshot.Spec.UserCredential == nil {
		return ErrUserAuthGate
	}
	credential := snapshot.Spec.UserCredential
	gate, err := auth.GetUserAuth(ctx, credential.Principal)
	if err != nil {
		return err
	}
	if gate == nil || gate.Grant.VaultID != credential.VaultID || gate.Grant.CredentialID != credential.CredentialID {
		return ErrUserAuthGate
	}
	return gate.CheckIntent(c.Binding.Owner, RemoteIntent{UserAuthKey: credential.Principal.Key(), GrantVersion: gate.Grant.GrantVersion})
}

func currentBatchScopes(r UserAuthRecord, fallback []string) []string {
	if r.Batch > 0 {
		return cloneStrings(r.BatchScopes)
	}
	return fallback
}
