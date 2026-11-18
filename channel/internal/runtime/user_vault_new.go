// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// replaceDeletedUserVaultForNew 仅在显式 /new 中恢复已确认丢失的个人授权容器。
// 普通消息、旧 Session 和已开始的授权等待均不迁移引用。
func (r *Runtime) replaceDeletedUserVaultForNew(ctx context.Context, j *job, c Connection, record UserAuthRecord) (bool, error) {
	if strings.TrimSpace(j.msg.Text) != "/new" || j.Session.SnapshotID != "" || j.Submit.WriteAttempts != 0 || j.AuthWait != nil {
		return false, nil
	}
	if record.State == UserAuthUncertain || record.Operation == UserAuthRefresh && record.State == UserAuthVerifying {
		// 不确定授权和刷新仍由原流程诊断，不能通过新建绕过。
		return false, nil
	}
	reader, ok := c.MAClient.(interface {
		GetVault(context.Context, string) (VaultRef, error)
	})
	if !ok {
		return false, nil
	}
	vault, err := reader.GetVault(ctx, record.VaultID)
	if err == nil {
		if vault.ID != record.VaultID || vault.Principal != record.Principal.Key() {
			return true, r.finishUserFailure(j, "invalid_subject")
		}
		return false, nil
	}
	if !deletedUserVault(err) {
		return true, r.wait(j, "user_vault_unavailable")
	}
	if record.Secret != nil && record.State != UserAuthReady {
		// 尚未确认的 Device intent 可能已有远端效果；只诊断当前 /new，不改变原门闩和密文。
		return true, r.finishUserFailure(j, "user_auth_uncertain")
	}
	auth := r.store.(UserAuthStore)
	held, err := auth.BeginUserAuth(ctx, UserAuthBegin{ExpectedRevision: &record.Revision, Principal: record.Principal, VaultID: record.VaultID, Owner: r.instanceID + ":" + j.ID, TTL: r.options.LeaseTTL})
	if err != nil {
		return true, r.userAuthBusy(j, err)
	}
	if err := j.checkLease(ctx); err != nil {
		return true, err
	}
	// 名称绑定原 Vault；创建结果未知时由既有 reservation 查账，不能重复 POST。
	store, sok := r.store.(BotVaultStore)
	provisioner, pok := c.MAClient.(BotVaultProvisioner)
	if !sok || !pok {
		return true, r.failUserAuth(j, held, "identity_unsupported")
	}
	owner := fmt.Sprintf("%s:%d", held.Lease.Owner, held.Lease.Fence)
	coordinator := NewBotVaultCoordinator(store, provisioner, owner)
	name := "channel-user-" + digestJSON([]string{held.Principal.Key(), held.VaultID})
	ref, err := coordinator.EnsureVault(ctx, held.Principal.Key(), BotVaultCreate{Name: name, DisplayName: userVaultDisplayName(held.Principal)}, time.Minute)
	if err != nil {
		if errors.Is(err, ErrVaultPrincipalMismatch) {
			return true, r.failUserAuth(j, held, "invalid_subject")
		}
		if _, releaseErr := auth.UpdateUserAuth(ctx, UserAuthUpdate{Principal: held.Principal, Lease: held.Lease, Revision: held.Revision, State: held.State, Release: true}); releaseErr != nil {
			return true, releaseErr
		}
		return true, r.wait(j, "user_vault_unavailable")
	}
	if err := j.checkLease(ctx); err != nil {
		return true, err
	}
	next, err := auth.UpdateUserAuth(ctx, UserAuthUpdate{Principal: held.Principal, Lease: held.Lease, Revision: held.Revision, State: UserAuthPending, ReplaceVault: &UserVaultReplacement{PreviousVaultID: held.VaultID, VaultID: ref.ID}})
	if err != nil {
		return true, err
	}
	query := UserGrantQuery{Principal: next.Principal, VaultID: next.VaultID, RequiredScopes: userScopes(j.binding.UserAuthScopes, nil)}
	return true, r.startUserAuth(ctx, j, c, next, query)
}

func deletedUserVault(err error) bool {
	var coded APIErrorCode
	return errors.Is(err, ErrUserVaultDeleted) || errors.As(err, &coded) && coded.HTTPStatus() == http.StatusNotFound && coded.ErrorCode() == "InvalidVault.NotFound"
}
