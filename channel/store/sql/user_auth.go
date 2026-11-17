// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// userAuthDDL 是显式 expand 制品；不是普通 open 的必要结构。
func userAuthDDL(d Dialect, prefix string) []string {
	return []string{"CREATE TABLE IF NOT EXISTS " + prefix + "channel_user_auth (id VARCHAR(128) NOT NULL PRIMARY KEY, data " + d.TextType + " NOT NULL)"}
}

// expandUserAuth 显式安装 User 可选表，调用方负责环境变更审批。
func expandUserAuth(ctx context.Context, db *sql.DB, d Dialect, prefix string) error {
	if !validPrefix(prefix) {
		return core.ErrInvalid
	}
	for _, stmt := range userAuthDDL(d, prefix) {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}
func (s *store) userAuth(ctx context.Context, q queryer, key string, lock bool) (*core.UserAuthRecord, error) {
	suffix := ""
	if lock {
		suffix = s.dialect.RowLockSuffix
	}
	return loadOne[core.UserAuthRecord](ctx, q, "SELECT data FROM "+s.payloadTable("user_auth")+" WHERE id=?"+suffix, key)
}
func (s *store) saveUserAuth(ctx context.Context, tx *sql.Tx, r core.UserAuthRecord, insert bool) error {
	if r.IsOpaqueStored() {
		return core.ErrUnsupportedWork
	}
	if insert {
		_, e := tx.ExecContext(ctx, "INSERT INTO "+s.payloadTable("user_auth")+" (id,data) VALUES (?,?)", r.Principal.Key(), mustEncode(r))
		return e
	}
	_, e := tx.ExecContext(ctx, "UPDATE "+s.payloadTable("user_auth")+" SET data=? WHERE id=?", mustEncode(r), r.Principal.Key())
	return e
}

// GetUserAuth 只读取非密元信息及不透明密文，不触发解密。
func (s *store) GetUserAuth(ctx context.Context, p core.UserPrincipal) (*core.UserAuthRecord, error) {
	if !p.Valid() {
		return nil, core.ErrInvalid
	}
	return s.userAuth(ctx, s.db, p.Key(), false)
}

// BeginUserAuth 以 account→principal→remote 当前读锁序开始重授。
func (s *store) BeginUserAuth(ctx context.Context, b core.UserAuthBegin) (core.UserAuthRecord, error) {
	var out core.UserAuthRecord
	var blocked bool
	if !b.Principal.Valid() {
		return out, core.ErrInvalid
	}
	e := s.runTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		blocked = false
		if _, e := s.lockQuota(ctx, tx, b.Principal.Owner, false); e != nil {
			return e
		}
		r, e := s.userAuth(ctx, tx, b.Principal.Key(), true)
		if e != nil {
			return e
		}
		insert := r == nil
		if r == nil {
			r = &core.UserAuthRecord{}
		}
		// FOR UPDATE 是当前读，避免 RR 快照看不到刚提交的 intent；账号锁阻止新的 intent 插入。
		rows, e := tx.QueryContext(ctx, "SELECT resolved,data FROM "+s.t.remoteOperations+" WHERE owner=?"+s.dialect.RowLockSuffix, b.Principal.Owner)
		if e != nil {
			return e
		}
		for rows.Next() {
			var raw []byte
			var resolved bool
			if e = rows.Scan(&resolved, &raw); e != nil {
				break
			}
			var op struct {
				UserAuthKey string          `json:"user_auth_key"`
				WorkKind    string          `json:"_work_kind"`
				Kind        core.RemoteKind `json:"kind"`
				Resolved    bool            `json:"resolved"`
			}
			if e = json.Unmarshal(raw, &op); e != nil {
				break
			}
			if op.WorkKind != "" && op.WorkKind != core.WorkKindLegacy && op.WorkKind != core.WorkKindUserAuth || op.Kind != core.RemoteCreate && op.Kind != core.RemoteInput || resolved != op.Resolved || !resolved && op.UserAuthKey == b.Principal.Key() {
				blocked = true
				break
			}
		}
		rowErr := rows.Err()
		_ = rows.Close()
		if e != nil {
			return e
		}
		if rowErr != nil {
			return rowErr
		}
		if blocked {
			out, e = r.Drain(b, s.now(ctx))
		} else {
			out, e = r.Begin(b, s.now(ctx))
		}
		if e != nil {
			return e
		}
		return s.saveUserAuth(ctx, tx, out, insert)
	})
	if e == nil && blocked {
		return out, core.ErrRemoteBarrier
	}
	return out, e
}
func (s *store) updateUserAuth(ctx context.Context, tx *sql.Tx, u core.UserAuthUpdate) (core.UserAuthRecord, error) {
	if _, e := s.lockQuota(ctx, tx, u.Principal.Owner, false); e != nil {
		return core.UserAuthRecord{}, e
	}
	r, e := s.userAuth(ctx, tx, u.Principal.Key(), true)
	if e != nil {
		return core.UserAuthRecord{}, e
	}
	if r == nil {
		return core.UserAuthRecord{}, core.ErrLeaseLost
	}
	next, e := r.Apply(u, s.now(ctx))
	if e != nil {
		return next, e
	}
	return next, s.saveUserAuth(ctx, tx, next, false)
}

// UpdateUserAuth 在同一事务校验并更新 fence/revision。
func (s *store) UpdateUserAuth(ctx context.Context, u core.UserAuthUpdate) (core.UserAuthRecord, error) {
	var out core.UserAuthRecord
	e := s.runTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var e error
		out, e = s.updateUserAuth(ctx, tx, u)
		return e
	})
	return out, e
}

// WaitForUserAuth 原子保存恢复点、提示 Outbox 和 Turn lease 释放。
func (s *store) WaitForUserAuth(ctx context.Context, l core.Lease, w core.UserAuthWait) error {
	if s.payloadCodec == nil {
		return core.ErrUserAuthEncryption
	}
	return s.runTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		owner, _, e := s.lockRemoteQuota(ctx, tx, l.ID)
		if e != nil {
			return e
		}
		if owner != w.Principal.Owner {
			return core.ErrConflict
		}
		r, e := s.userAuth(ctx, tx, w.Principal.Key(), true)
		if e != nil {
			return e
		}
		if r == nil {
			return core.ErrConflict
		}

		released, e := r.ApplyWait(w, s.now(ctx))
		if e != nil {
			return e
		}
		if e = s.saveUserAuth(ctx, tx, released, false); e != nil {
			return e
		}
		if !r.ExpiresAt.IsZero() && w.DeadlineAt.After(r.ExpiresAt) {
			return core.ErrConflict
		}
		t, e := s.leasedTurn(ctx, tx, l)
		if e != nil {
			return e
		}
		next, e := t.WaitUserAuth(w)
		if e != nil {
			return e
		}
		if w.Prompt != nil && (t.AuthWait == nil || w.Batch > t.AuthWait.Batch) {
			prompt := *w.Prompt
			prompt.Snapshot = t.Snapshot
			if w.Prompt.BindingID != t.BindingID {
				return core.ErrConflict
			}
			if e = s.insertDeliveries(ctx, tx, core.DeliverySource{Kind: core.SourceProgress, ID: t.ID}, t.ConversationID, []core.Delivery{prompt}); e != nil {
				return e
			}
		}
		return s.putTurn(ctx, tx, next, false, false)
	})
}

// FailUserAuth 将失败授权与 Turn/回复一并结算，任一步失败全部回滚。
func (s *store) FailUserAuth(ctx context.Context, l core.Lease, f core.UserAuthFailure) error {
	if s.payloadCodec == nil {
		return core.ErrUserAuthEncryption
	}
	validState := f.Update.State == core.UserAuthFailed || f.Update.State == core.UserAuthInvalidSubject || f.Update.State == core.UserAuthUncertain || f.Update.State == core.UserAuthReady && f.Outcome.ErrorKind == "user_auth_timeout"
	if f.Outcome.Status != core.TurnFailed || !validState {
		return core.ErrInvalid
	}
	return s.runTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		owner, _, e := s.lockRemoteQuota(ctx, tx, l.ID)
		if e != nil {
			return e
		}
		if owner != f.Update.Principal.Owner {
			return core.ErrConflict
		}
		if _, e = s.updateUserAuth(ctx, tx, f.Update); e != nil {
			return e
		}
		return s.settleUserTurn(ctx, tx, l, f.Outcome, f.Deliveries)
	})
}
func (s *store) checkUserIntent(ctx context.Context, tx *sql.Tx, owner string, i *core.RemoteIntent) error {
	if i == nil || i.UserAuthKey == "" {
		return nil
	}
	r, e := s.userAuth(ctx, tx, i.UserAuthKey, true)
	if e != nil {
		return e
	}
	if r == nil {
		return core.ErrUserAuthGate
	}
	return r.CheckIntent(owner, *i)
}

var _ core.UserAuthStore = (*store)(nil)

// BeginUserAuthRefresh 使用同账号/principal 锁取得维护租约，但不要求已有 remote 收敛。
func (s *store) BeginUserAuthRefresh(ctx context.Context, b core.UserAuthBegin) (core.UserAuthRecord, error) {
	var out core.UserAuthRecord
	if !b.Principal.Valid() {
		return out, core.ErrInvalid
	}
	e := s.runTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if _, e := s.lockQuota(ctx, tx, b.Principal.Owner, false); e != nil {
			return e
		}
		r, e := s.userAuth(ctx, tx, b.Principal.Key(), true)
		if e != nil {
			return e
		}
		if r == nil {
			return core.ErrUserAuthGate
		}
		out, e = r.BeginRefresh(b, s.now(ctx))
		if e != nil {
			return e
		}
		return s.saveUserAuth(ctx, tx, out, false)
	})
	return out, e
}

// expandUserAuth 显式安装当前 namespace 的可选授权表，不修改既有记录。
func (s *store) ExpandUserAuth(ctx context.Context) error {
	return expandUserAuth(ctx, s.db, s.dialect, strings.TrimSuffix(s.t.meta, "channel_meta"))
}
