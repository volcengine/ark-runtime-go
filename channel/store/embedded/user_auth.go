// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

import (
	"context"
	"encoding/json"

	"go.etcd.io/bbolt"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

var bucketUserAuth = []byte("user_auth")

// ExpandUserAuth 在已打开文件上显式安装可选授权 bucket；不改变全局 marker。
func (s *store) ExpandUserAuth(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.db.Update(func(tx *bbolt.Tx) error { _, e := tx.CreateBucketIfNotExists(bucketUserAuth); return e })
}
func userAuth(tx storeTx, k string) (*core.UserAuthRecord, error) {
	if tx.Bucket(bucketUserAuth) == nil {
		return nil, errSchema
	}
	return getJSON[core.UserAuthRecord](tx, bucketUserAuth, []byte(k))
}
func saveUserAuth(tx storeTx, r core.UserAuthRecord) error {
	if r.IsOpaqueStored() {
		return core.ErrUnsupportedWork
	}
	return putJSON(tx, bucketUserAuth, []byte(r.Principal.Key()), r)
}

// GetUserAuth 只读取授权 metadata 和不透明密文，不触发 KMS。
func (s *store) GetUserAuth(ctx context.Context, p core.UserPrincipal) (*core.UserAuthRecord, error) {
	var out *core.UserAuthRecord
	if !p.Valid() {
		return nil, core.ErrInvalid
	}
	e := s.view(ctx, func(tx storeTx) error { var e error; out, e = userAuth(tx, p.Key()); return e })
	return out, e
}

// BeginUserAuth 在 bbolt 写事务中与同主体所有 remote intent 原子竞争。
func (s *store) BeginUserAuth(ctx context.Context, b core.UserAuthBegin) (core.UserAuthRecord, error) {
	var out core.UserAuthRecord
	var blocked bool
	e := s.update(ctx, func(tx storeTx) error {
		blocked = false
		r, e := userAuth(tx, b.Principal.Key())
		if e != nil {
			return e
		}
		if r == nil {
			r = &core.UserAuthRecord{}
		}
		e = tx.Bucket(bucketRemoteOperations).ForEach(func(k, v []byte) error {
			var op struct {
				Owner       string          `json:"owner"`
				UserAuthKey string          `json:"user_auth_key"`
				Resolved    bool            `json:"resolved"`
				WorkKind    string          `json:"_work_kind"`
				Kind        core.RemoteKind `json:"kind"`
			}
			if e := json.Unmarshal(v, &op); e != nil {
				return e
			}
			if op.Owner == b.Principal.Owner && (!op.Resolved && op.UserAuthKey == b.Principal.Key() || op.WorkKind != "" && op.WorkKind != core.WorkKindLegacy && op.WorkKind != core.WorkKindUserAuth || op.Kind != core.RemoteCreate && op.Kind != core.RemoteInput) {
				blocked = true
			}
			return nil
		})
		if e != nil {
			return e
		}
		if blocked {
			out, e = r.Drain(b, s.now())
		} else {
			out, e = r.Begin(b, s.now())
		}
		if e != nil {
			return e
		}
		return saveUserAuth(tx, out)
	})
	if e == nil && blocked {
		return out, core.ErrRemoteBarrier
	}
	return out, e
}
func (s *store) updateUserAuth(tx storeTx, u core.UserAuthUpdate) (core.UserAuthRecord, error) {
	r, e := userAuth(tx, u.Principal.Key())
	if e != nil {
		return core.UserAuthRecord{}, e
	}
	if r == nil {
		return core.UserAuthRecord{}, core.ErrLeaseLost
	}
	next, e := r.Apply(u, s.now())
	if e != nil {
		return next, e
	}
	return next, saveUserAuth(tx, next)
}

// UpdateUserAuth 以持久 fence/revision 校验写入。
func (s *store) UpdateUserAuth(ctx context.Context, u core.UserAuthUpdate) (core.UserAuthRecord, error) {
	var out core.UserAuthRecord
	e := s.update(ctx, func(tx storeTx) error { var e error; out, e = s.updateUserAuth(tx, u); return e })
	return out, e
}

// WaitForUserAuth 同事务写入等待、密文提示 Outbox 并释放 Turn lease。
func (s *store) WaitForUserAuth(ctx context.Context, l core.Lease, w core.UserAuthWait) error {
	if s.payloadCodec == nil {
		return core.ErrUserAuthEncryption
	}
	return s.update(ctx, func(tx storeTx) error {
		r, e := userAuth(tx, w.Principal.Key())
		if e != nil {
			return e
		}
		if r == nil {
			return core.ErrConflict
		}

		released, e := r.ApplyWait(w, s.now())
		if e != nil {
			return e
		}
		if e = saveUserAuth(tx, released); e != nil {
			return e
		}
		if !r.ExpiresAt.IsZero() && w.DeadlineAt.After(r.ExpiresAt) {
			return core.ErrConflict
		}
		t, e := s.leasedTurn(tx, l)
		if e != nil {
			return e
		}
		owner, _, e := s.remoteQuota(tx, t.Turn)
		if e != nil {
			return e
		}
		if owner != w.Principal.Owner {
			return core.ErrConflict
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
			if e = s.insertDeliveries(tx, core.DeliverySource{Kind: core.SourceProgress, ID: t.ID}, []core.Delivery{prompt}); e != nil {
				return e
			}
		}
		old := *t
		t.Turn = next
		return s.writeTurn(tx, &old, *t)
	})
}

// FailUserAuth 将授权失败、Turn 结算、回复提交为单个写事务。
func (s *store) FailUserAuth(ctx context.Context, l core.Lease, f core.UserAuthFailure) error {
	if s.payloadCodec == nil {
		return core.ErrUserAuthEncryption
	}
	validState := f.Update.State == core.UserAuthFailed || f.Update.State == core.UserAuthInvalidSubject || f.Update.State == core.UserAuthUncertain || f.Update.State == core.UserAuthReady && f.Outcome.ErrorKind == "user_auth_timeout"
	if f.Outcome.Status != core.TurnFailed || !validState {
		return core.ErrInvalid
	}
	return s.update(ctx, func(tx storeTx) error {
		t, e := s.leasedTurn(tx, l)
		if e != nil {
			return e
		}
		owner, _, e := s.remoteQuota(tx, t.Turn)
		if e != nil {
			return e
		}
		if owner != f.Update.Principal.Owner {
			return core.ErrConflict
		}
		if _, e = s.updateUserAuth(tx, f.Update); e != nil {
			return e
		}
		return s.settleUserTurn(tx, l, f.Outcome, f.Deliveries)
	})
}
func checkUserIntent(tx storeTx, owner string, i *core.RemoteIntent) error {
	if i == nil || i.UserAuthKey == "" {
		return nil
	}
	r, e := userAuth(tx, i.UserAuthKey)
	if e != nil {
		return e
	}
	if r == nil {
		return core.ErrUserAuthGate
	}
	return r.CheckIntent(owner, *i)
}

var _ core.UserAuthStore = (*store)(nil)

// BeginUserAuthRefresh 在写事务中保存维护租约，不阻断已存在的远端操作。
func (s *store) BeginUserAuthRefresh(ctx context.Context, b core.UserAuthBegin) (core.UserAuthRecord, error) {
	var out core.UserAuthRecord
	e := s.update(ctx, func(tx storeTx) error {
		r, e := userAuth(tx, b.Principal.Key())
		if e != nil {
			return e
		}
		if r == nil {
			return core.ErrUserAuthGate
		}
		out, e = r.BeginRefresh(b, s.now())
		if e != nil {
			return e
		}
		return saveUserAuth(tx, out)
	})
	return out, e
}
