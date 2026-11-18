// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"slices"
	"time"
)

// Begin 开始或接管授权，过期接管保留原 flow 与绝对过期时间。
func (r UserAuthRecord) Begin(b UserAuthBegin, now time.Time) (UserAuthRecord, error) {
	if !b.Principal.Valid() || b.VaultID == "" || b.Owner == "" || b.TTL <= 0 {
		return r, ErrInvalid
	}
	if r.IsOpaqueStored() {
		return r, ErrUnsupportedWork
	}
	if b.ExpectedRevision != nil && *b.ExpectedRevision != r.Revision {
		return r, ErrConflict
	}
	if r.Revision > 0 && (r.Principal != b.Principal || r.VaultID != b.VaultID) {
		return r, ErrConflict
	}
	if b.DeletedCredentialID != "" {
		if err := r.checkDeletedCredentialRecovery(b); err != nil {
			return r, err
		}
	} else if r.State == UserAuthUncertain || r.Operation == UserAuthRefresh && r.State != UserAuthReady && r.State != UserAuthFailed && r.State != UserAuthInvalidSubject {
		return r, ErrUserAuthGate
	}
	if r.Lease.Live(now) {
		return r, ErrConflict
	}
	if b.DeletedCredentialID != "" {
		r.CredentialID = ""
		r.Grant = UserGrant{}
		r.State = UserAuthFailed
	}
	if r.Revision == 0 || r.State == UserAuthReady || r.State == UserAuthFailed || r.State == UserAuthInvalidSubject || r.State == UserAuthDraining {
		r.State = UserAuthPending
		r.Attempt++
		r.FlowID = ""
		r.ExpiresAt = time.Time{}
		r.Secret = nil
		r.RequiredScopes, r.BatchScopes, r.VerifiedScopes, r.Batch = nil, nil, nil, 0
	}
	if r.CredentialID == "" {
		r.CredentialID = r.Grant.CredentialID
	}
	r.Operation = UserAuthAuthorize
	r.Principal = b.Principal
	r.VaultID = b.VaultID
	r.WorkKind = WorkKindUserAuth
	r.Lease = Lease{ID: b.Principal.Key(), Owner: b.Owner, Fence: r.Lease.Fence + 1, ExpiresAt: now.Add(b.TTL)}
	r.Revision++
	return r, nil
}

// Apply 校验授权 fence、状态与不可延长的本次授权截止时间。
func (r UserAuthRecord) Apply(u UserAuthUpdate, now time.Time) (UserAuthRecord, error) {
	if r.IsOpaqueStored() {
		return r, ErrUnsupportedWork
	}
	// 过期 refresh 可能已经发出远端写入，只允许同观察 fence/revision 收紧为 uncertain。
	if r.Operation == UserAuthRefresh && r.State == UserAuthVerifying && r.Principal == u.Principal && r.Lease == u.Lease && r.Revision == u.Revision && !r.Lease.Live(now) && u.State == UserAuthUncertain && unchangedAuthEvidence(u) {
		r.State = UserAuthUncertain
		r.Lease.Owner = ""
		r.Lease.ExpiresAt = time.Time{}
		r.Revision++
		return r, nil
	}
	// 已闭锁的 unknown 只允许在 FailUserAuth 事务中观察原 revision 并结算回复。
	if r.State == UserAuthUncertain && u.State == UserAuthUncertain && r.Principal == u.Principal && u.Lease == (Lease{}) && !r.Lease.Live(now) && u.Revision == r.Revision && unchangedAuthEvidence(u) {
		return r, nil
	}
	if r.Principal != u.Principal || u.Lease.ID != r.Principal.Key() || u.Lease.Owner == "" || u.Lease.Owner != r.Lease.Owner || u.Lease.Fence != r.Lease.Fence || !r.Lease.Live(now) {
		return r, ErrLeaseLost
	}
	if u.Revision != r.Revision {
		return r, ErrConflict
	}
	if u.ReplaceVault != nil {
		return r.replaceVault(u)
	}
	if u.BeginBatch {
		if r.Operation != UserAuthAuthorize || u.State != UserAuthVerifying || len(u.BatchScopes) == 0 || len(u.BatchScopes) > UserAuthScopeBatchLimit || !r.ExpiresAt.IsZero() && !now.Before(r.ExpiresAt) {
			return r, ErrInvalid
		}
		required := userScopes(u.RequiredScopes, nil)
		batch := userScopes(u.BatchScopes, nil)
		verified := cloneStrings(r.VerifiedScopes)
		if r.Batch > 0 {
			q := UserGrantQuery{VaultID: r.VaultID, CredentialID: r.CredentialID, RequiredScopes: userScopes(r.VerifiedScopes, r.BatchScopes)}
			if r.FlowID == "" || u.BatchGrant.FlowID != r.FlowID || !validUserGrant(u.BatchGrant, q) || !slices.Equal(required, r.RequiredScopes) {
				return r, ErrConflict
			}
			verified = userScopes(r.VerifiedScopes, r.BatchScopes)
			r.CredentialID = u.BatchGrant.CredentialID
		} else if r.FlowID != "" {
			return r, ErrConflict
		}
		for _, scope := range batch {
			if !slices.Contains(required, scope) || slices.Contains(verified, scope) {
				return r, ErrConflict
			}
		}
		r.RequiredScopes, r.BatchScopes, r.VerifiedScopes = required, batch, verified
		r.Batch++
		r.FlowID = ""
		r.Secret = nil
		r.State = UserAuthVerifying
		r.Revision++
		if !u.ExpiresAt.IsZero() {
			if !r.ExpiresAt.IsZero() && u.ExpiresAt.After(r.ExpiresAt) {
				return r, ErrConflict
			}
			r.ExpiresAt = u.ExpiresAt
		}
		return r, nil
	}
	switch u.State {
	case UserAuthDraining, UserAuthPending, UserAuthVerifying, UserAuthReady, UserAuthInvalidSubject, UserAuthFailed, UserAuthUncertain:
	default:
		return r, ErrInvalid
	}
	if r.State == UserAuthUncertain || r.Operation == UserAuthRefresh && (u.State == UserAuthPending || u.State == UserAuthDraining) {
		return r, ErrUserAuthGate
	}
	if r.Operation == UserAuthRefresh && u.State == UserAuthReady && (u.Grant.GrantVersion != r.Grant.GrantVersion || u.Grant.VaultID != r.Grant.VaultID || u.Grant.CredentialID != r.Grant.CredentialID || u.Grant.FlowID != r.Grant.FlowID) {
		return r, ErrConflict
	}
	if u.State == UserAuthReady && r.Batch > 0 && !validUserGrant(u.Grant, UserGrantQuery{VaultID: r.VaultID, CredentialID: r.CredentialID, RequiredScopes: r.RequiredScopes}) {
		return r, ErrConflict
	}
	if u.State == UserAuthReady && (u.Grant.Status != UserAuthReady || u.Grant.VaultID != r.VaultID || u.Grant.CredentialID == "" || u.Grant.GrantVersion == "") {
		return r, ErrInvalid
	}
	target := r.CredentialID
	if target == "" {
		target = r.Grant.CredentialID
	}
	requested := u.CredentialID
	if u.State == UserAuthReady {
		if requested != "" && requested != u.Grant.CredentialID {
			return r, ErrConflict
		}
		requested = u.Grant.CredentialID
	}
	if target != "" && requested != "" && target != requested {
		return r, ErrConflict
	}
	if target == "" && requested != "" && u.State != UserAuthReady && r.FlowID == "" && u.FlowID == "" {
		return r, ErrInvalid
	}
	if u.State == UserAuthReady && r.FlowID != "" && u.Grant.FlowID != r.FlowID {
		return r, ErrConflict
	}
	if requested != "" {
		target = requested
	}
	if u.Secret != nil && (u.Secret.Version == 0 || u.Secret.KeyID == "" || len(u.Secret.Ciphertext) == 0) {
		return r, ErrInvalid
	}
	if !r.ExpiresAt.IsZero() && !u.ExpiresAt.IsZero() && u.ExpiresAt.After(r.ExpiresAt) {
		return r, ErrConflict
	}
	r.State = u.State
	r.CredentialID = target
	if u.FlowID != "" {
		if r.FlowID != "" && r.FlowID != u.FlowID {
			return r, ErrConflict
		}
		r.FlowID = u.FlowID
	}
	if !u.ExpiresAt.IsZero() {
		r.ExpiresAt = u.ExpiresAt.UTC()
	}
	if u.Secret != nil {
		v := *u.Secret
		v.Ciphertext = append([]byte(nil), v.Ciphertext...)
		r.Secret = &v
	}
	if u.State == UserAuthReady {
		r.Grant = u.Grant
		r.Grant.GrantedScopes = cloneStrings(u.Grant.GrantedScopes)
	}
	if u.TTL > 0 {
		r.Lease.ExpiresAt = now.Add(u.TTL)
	}
	if u.Release || u.State == UserAuthReady || u.State == UserAuthFailed || u.State == UserAuthInvalidSubject || u.State == UserAuthUncertain {
		r.Lease.Owner = ""
		r.Lease.ExpiresAt = time.Time{}
		if u.State == UserAuthFailed || u.State == UserAuthInvalidSubject {
			r.Secret = nil
		}
	}
	r.Revision++
	return r, nil
}

// CheckIntent 只有同账号、同授权代次的 ready 门闩允许新远端写。
func (r UserAuthRecord) CheckIntent(owner string, i RemoteIntent) error {
	if r.IsOpaqueStored() || r.Principal.Owner != owner || r.Principal.Key() != i.UserAuthKey || r.State != UserAuthReady || i.GrantVersion == "" || r.Grant.GrantVersion != i.GrantVersion {
		return ErrUserAuthGate
	}
	return nil
}

// WaitUserAuth 冻结等待恢复点，并保留原 Turn 的执行绝对时限。
func (t Turn) WaitUserAuth(w UserAuthWait) (Turn, error) {
	if !w.Principal.Valid() || w.FlowID == "" || w.DeadlineAt.IsZero() || w.Until.IsZero() {
		return t, ErrInvalid
	}
	if old := t.AuthWait; old != nil {
		if old.Principal != w.Principal || !old.DeadlineAt.Equal(w.DeadlineAt) || old.Batch > 0 && old.Attempt != w.Attempt {
			return t, ErrConflict
		}
		if w.Batch == old.Batch {
			if old.FlowID != w.FlowID {
				return t, ErrConflict
			}
		} else if w.Batch != old.Batch+1 || !w.RegisterFlow || w.Prompt == nil {
			return t, ErrConflict
		}
	}
	if w.Until.After(w.DeadlineAt) {
		w.Until = w.DeadlineAt
	}
	w.Prompt = nil
	w.RegisterFlow = false
	w.Lease = Lease{}
	w.Revision = 0
	t.AuthWait = &w
	t.WorkKind = WorkKindUserAuth
	return t.Wait(TurnWait{Reason: "user_auth", Until: w.Until, Submit: t.Submit})
}

// BeginRefresh 取得不改变授权身份的维护租约，保留密文/flow/授权期限，关闭新输入门闩。
// 已进入 verifying 的刷新不能被自动重试；丢失响应必须按 uncertain 处理。
func (r UserAuthRecord) BeginRefresh(b UserAuthBegin, now time.Time) (UserAuthRecord, error) {
	if b.DeletedCredentialID != "" {
		return r, ErrInvalid
	}
	if !b.Principal.Valid() || b.Owner == "" || b.VaultID == "" || b.TTL <= 0 {
		return r, ErrInvalid
	}
	if r.IsOpaqueStored() {
		return r, ErrUnsupportedWork
	}
	if b.ExpectedRevision != nil && *b.ExpectedRevision != r.Revision {
		return r, ErrConflict
	}
	if r.State != UserAuthReady {
		return r, ErrUserAuthGate
	}
	if r.Principal != b.Principal || r.VaultID != b.VaultID {
		return r, ErrConflict
	}
	if r.Lease.Live(now) {
		return r, ErrConflict
	}
	r.Operation = UserAuthRefresh
	r.State = UserAuthVerifying
	r.WorkKind = WorkKindUserAuth
	r.Lease = Lease{ID: b.Principal.Key(), Owner: b.Owner, Fence: r.Lease.Fence + 1, ExpiresAt: now.Add(b.TTL)}
	r.Revision++
	return r, nil
}

// Drain 先持久关闭新输入门闩，保留既有授权与密文；它不授予开始远端授权的 lease。
func (r UserAuthRecord) Drain(b UserAuthBegin, now time.Time) (UserAuthRecord, error) {
	if !b.Principal.Valid() || b.VaultID == "" || b.Owner == "" || b.TTL <= 0 {
		return r, ErrInvalid
	}
	if r.IsOpaqueStored() {
		return r, ErrUnsupportedWork
	}
	if b.ExpectedRevision != nil && *b.ExpectedRevision != r.Revision {
		return r, ErrConflict
	}
	if r.Revision > 0 && (r.Principal != b.Principal || r.VaultID != b.VaultID) {
		return r, ErrConflict
	}
	if b.DeletedCredentialID != "" {
		if err := r.checkDeletedCredentialRecovery(b); err != nil {
			return r, err
		}
	} else if r.State == UserAuthUncertain || r.Operation == UserAuthRefresh && r.State != UserAuthReady && r.State != UserAuthFailed && r.State != UserAuthInvalidSubject {
		return r, ErrUserAuthGate
	}
	if r.Lease.Live(now) {
		return r, ErrConflict
	}
	// uncertain 本已关闭执行入口，遇屏障时保留它，不能降级成普通消息可重开的 draining。
	if b.DeletedCredentialID != "" && r.State == UserAuthUncertain {
		return r, nil
	}
	r.Principal = b.Principal
	r.VaultID = b.VaultID
	r.Operation = UserAuthAuthorize
	r.WorkKind = WorkKindUserAuth
	r.State = UserAuthDraining
	r.Lease.ID = b.Principal.Key()
	r.Lease.Owner = ""
	r.Lease.ExpiresAt = time.Time{}
	r.Revision++
	return r, nil
}

// WithSessionIdentity 按已经持久的 Session 身份选择执行门闩，而非按本轮新建默认值。
// Store 必须在同一事务从 SnapshotID 读取 snapshot；不能使用调用者提供的身份覆盖。
func (t Turn) WithSessionIdentity(snapshot *SessionSnapshot) (Turn, error) {
	if snapshot == nil || snapshot.ID != t.Session.SnapshotID || snapshot.Route != t.ConversationID || snapshot.Generation != t.Session.Generation || snapshot.IsOpaqueStored() {
		return t, ErrConflict
	}
	if effectiveIdentity(snapshot.Spec.Binding, snapshot.Spec.ChatType) == IdentityUser {
		t.WorkKind = WorkKindUserAuth
	} else if t.WorkKind == WorkKindUserAuth {
		t.WorkKind = WorkKindLegacy
	}
	return t, nil
}

func unchangedAuthEvidence(u UserAuthUpdate) bool {
	return u.ReplaceVault == nil && !u.BeginBatch && len(u.RequiredScopes) == 0 && len(u.BatchScopes) == 0 && digestJSON(u.BatchGrant) == digestJSON(UserGrant{}) && u.CredentialID == "" && u.Secret == nil && u.FlowID == "" && u.TTL == 0 && u.ExpiresAt.IsZero() && !u.Release && digestJSON(u.Grant) == digestJSON(UserGrant{})
}

// ApplyWait 在同一 gate CAS 中登记首次 flow 并释放 lease，授权提示必须随事务提交。
func (r UserAuthRecord) ApplyWait(w UserAuthWait, now time.Time) (UserAuthRecord, error) {
	if w.Batch != r.Batch || r.Batch > 0 && w.Attempt != r.Attempt {
		return r, ErrConflict
	}
	if (r.FlowID != w.FlowID && (!w.RegisterFlow || r.FlowID != "")) || (r.State != UserAuthPending && r.State != UserAuthVerifying && r.State != UserAuthDraining) {
		return r, ErrConflict
	}
	if !r.ExpiresAt.IsZero() && w.DeadlineAt.After(r.ExpiresAt) {
		return r, ErrConflict
	}
	update := UserAuthUpdate{Principal: w.Principal, Lease: w.Lease, Revision: w.Revision, State: r.State, Release: true}
	if w.RegisterFlow {
		if w.Prompt == nil || w.FlowID == "" || w.DeadlineAt.IsZero() {
			return r, ErrInvalid
		}
		update.State = UserAuthPending
		update.FlowID = w.FlowID
		update.ExpiresAt = w.DeadlineAt
	}
	return r.Apply(update, now)
}

// replaceVault 仅在已关闭的授权入口内迁移引用，原 flow/grant 不能跨 Vault 延续。
func (r UserAuthRecord) replaceVault(u UserAuthUpdate) (UserAuthRecord, error) {
	if r.Operation != UserAuthAuthorize || r.Secret != nil || (r.State != UserAuthPending && r.State != UserAuthVerifying) {
		return r, ErrUserAuthGate
	}
	replacement := *u.ReplaceVault
	if replacement.PreviousVaultID != r.VaultID {
		return r, ErrConflict
	}
	if replacement.VaultID == "" || replacement.VaultID == r.VaultID {
		return r, ErrInvalid
	}
	plain := u
	plain.ReplaceVault = nil
	if u.State != UserAuthPending || !unchangedAuthEvidence(plain) {
		return r, ErrInvalid
	}
	r.VaultID = replacement.VaultID
	r.CredentialID, r.FlowID = "", ""
	r.Grant, r.Secret = UserGrant{}, nil
	r.RequiredScopes, r.BatchScopes, r.VerifiedScopes, r.Batch = nil, nil, nil, 0
	r.ExpiresAt = time.Time{}
	r.State = UserAuthPending
	r.Attempt++
	r.Revision++
	return r, nil
}

// checkDeletedCredentialRecovery 限定删除恢复只能操作已观察的旧引用，不能抹除未知授权写入。
func (r UserAuthRecord) checkDeletedCredentialRecovery(b UserAuthBegin) error {
	if b.ExpectedRevision == nil || r.Revision == 0 {
		return ErrInvalid
	}
	target := r.CredentialID
	if target == "" {
		target = r.Grant.CredentialID
	}
	if target == "" || target != b.DeletedCredentialID || r.Grant.CredentialID != "" && r.Grant.CredentialID != target {
		return ErrConflict
	}
	if r.Secret != nil || r.Operation == UserAuthRefresh && r.State != UserAuthReady {
		return ErrUserAuthGate
	}
	switch r.State {
	case UserAuthReady, UserAuthFailed, UserAuthInvalidSubject, UserAuthUncertain, UserAuthDraining:
		return nil
	default:
		return ErrUserAuthGate
	}
}
