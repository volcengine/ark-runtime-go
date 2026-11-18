// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
)

// rejectUserIdentity 将已证实的归属冲突映射为可靠失败，并先关闭该主体的执行门闩。
// 没有授权记录或已有他人持有的关闭门闩时，仅结算当前 Turn，绝不冒用别人的 lease。
func (r *Runtime) rejectUserIdentity(ctx context.Context, j *job) error {
	return r.rejectUserAuthorization(ctx, j, "invalid_subject")
}

// rejectUserAuthorization 关闭已证实失效的个人授权并可靠结算，不替换原 Vault 或释放远端屏障。
func (r *Runtime) rejectUserAuthorization(ctx context.Context, j *job, kind string) error {
	auth, ok := r.store.(UserAuthStore)
	if !ok {
		return r.finishUserFailure(j, kind)
	}
	binding := j.binding
	vaultID := ""
	if j.Session.SnapshotID != "" {
		snapshot, err := r.store.GetSessionSnapshot(ctx, j.Session.SnapshotID)
		if err != nil {
			return err
		}
		if snapshot != nil {
			binding = snapshot.Spec.Binding
			if snapshot.Spec.UserCredential != nil {
				vaultID = snapshot.Spec.UserCredential.VaultID
			}
		}
	}
	p := userPrincipal(binding, j.msg)
	if !p.Valid() {
		return r.finishUserFailure(j, kind)
	}
	record, err := auth.GetUserAuth(ctx, p)
	if err != nil {
		return err
	}
	if record != nil {
		// 旧 Session 的冻结 Vault 已删除，不代表另一代的新 Vault 授权失效。
		if kind == "user_vault_deleted" && vaultID != "" && vaultID != record.VaultID {
			return r.finishUserFailure(j, kind)
		}
		vaultID = record.VaultID
		if record.State == UserAuthUncertain {
			return r.settleUserAuthFailure(j, *record, UserAuthUncertain, kind)
		}
		if record.State != UserAuthReady {
			return r.finishUserFailure(j, kind)
		}
	}
	if vaultID == "" {
		return r.finishUserFailure(j, kind)
	}
	held, err := auth.BeginUserAuth(ctx, UserAuthBegin{Principal: p, VaultID: vaultID, Owner: r.instanceID + ":" + j.ID, TTL: r.options.LeaseTTL})
	if errors.Is(err, ErrRemoteBarrier) || errors.Is(err, ErrUserAuthGate) || errors.Is(err, ErrConflict) {
		return r.finishUserFailure(j, kind)
	}
	if err != nil {
		return err
	}
	return r.failUserAuth(j, held, kind)
}
