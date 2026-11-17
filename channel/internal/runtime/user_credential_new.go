// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"strings"
)

// reauthorizeDeletedUserCredentialForNew 只在显式新建中恢复已确认删除的旧凭据，不替换 Vault。
func (r *Runtime) reauthorizeDeletedUserCredentialForNew(ctx context.Context, j *job, c Connection, record UserAuthRecord) (bool, error) {
	if strings.TrimSpace(j.msg.Text) != "/new" || j.Session.SnapshotID != "" || j.Submit.WriteAttempts != 0 || j.AuthWait != nil {
		return false, nil
	}
	target := record.CredentialID
	if target == "" {
		target = record.Grant.CredentialID
	}
	begin := UserAuthBegin{Principal: record.Principal, VaultID: record.VaultID, Owner: r.instanceID + ":" + j.ID, TTL: r.options.LeaseTTL, ExpectedRevision: &record.Revision, DeletedCredentialID: target}
	if target == "" || record.checkDeletedCredentialRecovery(begin) != nil || record.Lease.Live(r.options.Clock()) {
		return false, nil
	}
	missing, err := userCredentialMissing(ctx, c, record, target)
	if err != nil {
		if errors.Is(err, ErrUserIdentityConflict) {
			return true, r.finishUserFailure(j, "invalid_subject")
		}
		if deletedUserVault(err) {
			return false, nil
		}
		return true, r.wait(j, "user_credential_unavailable")
	}
	if !missing {
		return false, nil
	}
	held, err := r.store.(UserAuthStore).BeginUserAuth(ctx, begin)
	if err != nil {
		return true, r.userAuthBusy(j, err)
	}
	if err = j.checkLease(ctx); err != nil {
		return true, err
	}
	return true, r.startUserAuth(ctx, j, c, held, UserGrantQuery{Principal: held.Principal, VaultID: held.VaultID, RequiredScopes: userScopes(j.binding.UserAuthScopes, nil)})
}

// userCredentialMissing 只读确认本人 Vault 中的旧凭据缺失；不支持端口时不推断删除。
func userCredentialMissing(ctx context.Context, c Connection, record UserAuthRecord, target string) (bool, error) {
	reader, ok := c.UserAuthorizer.(UserCredentialReader)
	if !ok {
		return false, nil
	}
	vaultReader, ok := c.MAClient.(interface {
		GetVault(context.Context, string) (VaultRef, error)
	})
	if !ok {
		return false, nil
	}
	vault, err := vaultReader.GetVault(ctx, record.VaultID)
	if err != nil {
		return false, err
	}
	if vault.ID != record.VaultID || vault.Principal != record.Principal.Key() {
		return false, ErrUserIdentityConflict
	}
	exists, err := reader.UserCredentialExists(ctx, UserGrantQuery{Principal: record.Principal, VaultID: record.VaultID, CredentialID: target})
	return !exists && err == nil, err
}

// noticeDeletedUserCredential 为尚未提交的普通消息提供恢复入口，不重置授权或旧会话。
func (r *Runtime) noticeDeletedUserCredential(ctx context.Context, j *job, c Connection, record *UserAuthRecord) (bool, error) {
	if record == nil || strings.TrimSpace(j.msg.Text) == "/new" || j.Submit.WriteAttempts != 0 || j.AuthWait != nil {
		return false, nil
	}
	target := record.CredentialID
	if target == "" {
		target = record.Grant.CredentialID
	}
	observed := UserAuthBegin{ExpectedRevision: &record.Revision, DeletedCredentialID: target}
	if record.checkDeletedCredentialRecovery(observed) != nil || record.Lease.Live(r.options.Clock()) {
		return false, nil
	}
	missing, err := userCredentialMissing(ctx, c, *record, target)
	if err != nil || !missing {
		return false, nil
	}
	// 探测期间其他会话可能完成 /new；旧观察已变化时重试，不发送过时的删除诊断。
	latest, err := r.store.(UserAuthStore).GetUserAuth(ctx, record.Principal)
	if err != nil {
		return false, err
	}
	if latest == nil || latest.Revision != record.Revision || latest.Principal != record.Principal || latest.VaultID != record.VaultID {
		return true, r.userAuthBusy(j, ErrConflict)
	}
	return true, r.finishUserFailure(j, "user_credential_deleted")
}
