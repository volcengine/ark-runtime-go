// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package userauth

import (
	"context"
	"slices"
	"time"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

const userTokenRefreshWindow = 30 * time.Second

func userTokenFresh(expiresAt time.Time, hasRefreshToken bool, now time.Time) bool {
	if !now.Before(expiresAt) {
		return false
	}
	// A token without refresh credentials remains usable until its actual expiry.
	return !hasRefreshToken || now.Add(userTokenRefreshWindow).Before(expiresAt)
}

// Lookup 仅在需要 refresh 时取得维护 lease；刷新保留原 flow/授权代次及绝对期限。
func (a *Authorizer) Lookup(ctx context.Context, q core.UserGrantQuery) (core.UserGrant, error) {
	r, err := a.record(ctx, q.Principal, q.VaultID)
	if err != nil {
		return core.UserGrant{}, err
	}
	if r == nil {
		return core.UserGrant{Status: core.AuthReauthRequired}, nil
	}
	if r.State == core.UserAuthUncertain {
		return core.UserGrant{Status: core.AuthUnknown}, nil
	}
	if r.State != core.UserAuthReady {
		return core.UserGrant{Status: core.AuthPending}, nil
	}
	if q.CredentialID != "" && q.CredentialID != stableID(*r) || q.FlowID != "" && q.FlowID != r.FlowID {
		return core.UserGrant{}, core.ErrConflict
	}
	secret, err := a.decrypt(ctx, *r)
	if err != nil {
		return core.UserGrant{}, err
	}
	if uncertainPhase(secret.Phase) {
		return core.UserGrant{Status: core.AuthUnknown}, nil
	}
	if secret.Phase != phaseSynced || !hasScopes(secret.Tokens, q.RequiredScopes) {
		return core.UserGrant{Status: core.AuthReauthRequired}, nil
	}
	if userTokenFresh(secret.Tokens.ExpiresAt, secret.Tokens.RefreshToken != "", a.config.Clock()) {
		return r.Grant, nil
	}
	if secret.Tokens.RefreshToken == "" {
		return core.UserGrant{Status: core.AuthReauthRequired}, nil
	}
	if ctx.Err() != nil {
		return core.UserGrant{}, errState
	}
	held, err := a.config.Store.BeginUserAuthRefresh(ctx, core.UserAuthBegin{Principal: q.Principal, VaultID: q.VaultID, Owner: a.owner, TTL: 2 * time.Minute})
	if err != nil {
		return core.UserGrant{Status: core.AuthPending}, nil
	}
	// lease 只互斥当前持有者；等待 acquisition 期间可能已有别的调用完成轮换。
	// 必须使用 acquisition 返回的最新密文，不能把预读 secret 用新 revision 覆盖回去。
	secret, err = a.decrypt(ctx, held)
	if err != nil || uncertainPhase(secret.Phase) {
		return a.releaseUnchangedRefresh(held, core.AuthUnknown)
	}
	if q.CredentialID != "" && q.CredentialID != stableID(held) || q.FlowID != "" && q.FlowID != held.FlowID {
		_, releaseErr := a.releaseUnchangedRefresh(held, core.AuthReady)
		if releaseErr != nil {
			return core.UserGrant{}, releaseErr
		}
		return core.UserGrant{}, core.ErrConflict
	}
	if secret.Phase != phaseSynced || !hasScopes(secret.Tokens, q.RequiredScopes) {
		return a.releaseUnchangedRefresh(held, core.AuthReauthRequired)
	}
	if userTokenFresh(secret.Tokens.ExpiresAt, secret.Tokens.RefreshToken != "", a.config.Clock()) {
		return a.releaseUnchangedRefresh(held, core.AuthReady)
	}
	if secret.Tokens.RefreshToken == "" {
		return a.releaseUnchangedRefresh(held, core.AuthReauthRequired)
	}
	refreshCtx, cancelRefresh := context.WithTimeout(ctx, 90*time.Second)
	defer cancelRefresh()
	ctx = refreshCtx
	secret.Phase = phaseRefreshIntent
	held, err = a.checkpoint(held, secret, core.UserAuthVerifying, "")
	if err != nil {
		return core.UserGrant{}, err
	}
	tokens, refreshErr := a.config.OAuth.Refresh(ctx, secret.Tokens.RefreshToken)
	var status core.AuthStatus
	if refreshErr != nil {
		status, err = a.exchangeError(held, secret, refreshErr)
	} else {
		// 省略 scope 仅继承同一已确认 grant；显式空 scope 必须保持空集。
		if !tokens.ScopeKnown && secret.Tokens.ScopeKnown {
			tokens.ScopeKnown = true
			tokens.GrantedScopes = slices.Clone(secret.Tokens.GrantedScopes)
		}
		secret.Tokens = tokens
		secret.Phase = phaseTokens
		held, err = a.checkpoint(held, secret, core.UserAuthVerifying, "")
		if err == nil {
			status, err = a.verifyAndSync(ctx, held, secret, q.RequiredScopes)
		}
	}
	// Lookup 自己拥有 refresh lease；任何未完成的维护都关闭 gate，不能以旧 grant 放行。
	latest, getErr := a.config.Store.GetUserAuth(context.Background(), q.Principal)
	if getErr != nil || latest == nil || latest.Lease.Owner != held.Lease.Owner || latest.Lease.Fence != held.Lease.Fence {
		return core.UserGrant{}, errState
	}
	state := core.UserAuthUncertain
	if err == nil {
		switch status.Status {
		case core.AuthReady:
			state = core.UserAuthReady
		case core.AuthSubjectMismatch:
			state = core.UserAuthInvalidSubject
		case core.AuthReauthRequired, core.AuthFailed, core.AuthDenied:
			state = core.UserAuthFailed
		}
	}
	saveCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, saveErr := a.config.Store.UpdateUserAuth(saveCtx, core.UserAuthUpdate{Principal: q.Principal, Lease: latest.Lease, Revision: latest.Revision, State: state, Grant: status.Grant})
	if saveErr != nil {
		return core.UserGrant{}, errState
	}
	if state == core.UserAuthReady {
		return status.Grant, nil
	}
	if state == core.UserAuthUncertain {
		return core.UserGrant{Status: core.AuthUnknown}, nil
	}
	return core.UserGrant{Status: status.Status}, nil
}

// releaseUnchangedRefresh 在尚未发出维护 HTTP 时释放 lease，保留 acquisition 的最新 grant/密文。
// query 过旧或缺 scope 不能破坏其他调用刚完成的授权；不能解密的证据仍保守关闭 gate。
func (a *Authorizer) releaseUnchangedRefresh(held core.UserAuthRecord, status string) (core.UserGrant, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	update := core.UserAuthUpdate{Principal: held.Principal, Lease: held.Lease, Revision: held.Revision, State: core.UserAuthReady, Grant: held.Grant}
	if status == core.AuthUnknown {
		update.State = core.UserAuthUncertain
		update.Grant = core.UserGrant{}
	}
	if _, err := a.config.Store.UpdateUserAuth(ctx, update); err != nil {
		return core.UserGrant{}, errState
	}
	if status == core.AuthReady {
		return held.Grant, nil
	}
	return core.UserGrant{Status: status}, nil
}
