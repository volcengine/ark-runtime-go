// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"context"
	"database/sql"
	"errors"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func (s *store) lockRemoteQuota(ctx context.Context, tx *sql.Tx, id string) (string, *accountQuota, error) {
	t, e := s.turn(ctx, tx, id)
	if e != nil {
		return "", nil, e
	}
	if t == nil {
		return "", nil, core.ErrLeaseLost
	}
	owner, e := s.quotaOwner(ctx, tx, *t)
	if e != nil {
		return "", nil, e
	}
	if owner == "" {
		owner = "binding:" + t.BindingID
	}
	q, e := s.lockQuota(ctx, tx, owner, false)
	if e == nil && q.Remote == nil {
		q.Remote = map[string]core.RemoteKind{}
	}
	return owner, q, e
}
func (s *store) putRemote(ctx context.Context, tx *sql.Tx, o core.RemoteOperation, insert bool) error {
	if o.IsOpaqueStored() {
		return core.ErrUnsupportedWork
	}
	if insert {
		_, e := tx.ExecContext(ctx, "INSERT INTO "+s.t.remoteOperations+" (id,owner,turn_id,session_id,resolved,eligible_at,data) VALUES (?,?,?,?,?,?,?)", o.ID, o.Owner, o.TurnID, o.SessionID, o.Resolved, remoteEligible(o), mustEncode(o))
		return e
	}
	_, e := tx.ExecContext(ctx, "UPDATE "+s.t.remoteOperations+" SET session_id=?,resolved=?,eligible_at=?,data=? WHERE id=?", o.SessionID, o.Resolved, remoteEligible(o), mustEncode(o), o.ID)
	return e
}
func (s *store) applyRemoteTurn(ctx context.Context, tx *sql.Tx, t core.Turn, u core.TurnUpdate, owner string, q *accountQuota) error {
	kind := u.RemoteKind
	if u.RemoteIntent != nil {
		kind = u.RemoteIntent.Kind
	}
	if kind == "" {
		return nil
	}
	id := core.RemoteOperationID(t, kind)
	o, e := loadOne[core.RemoteOperation](ctx, tx, "SELECT data FROM "+s.t.remoteOperations+" WHERE id=?"+s.dialect.RowLockSuffix, id)
	if e != nil {
		return e
	}
	insert := o == nil
	if u.RemoteIntent != nil {
		if o != nil && !o.Resolved && (o.UserAuthKey != u.RemoteIntent.UserAuthKey || o.GrantVersion != u.RemoteIntent.GrantVersion) {
			return core.ErrConflict
		}
		if kind == core.RemoteCreate {
			q.CreationLimit = u.RemoteIntent.Limit
		} else {
			q.Limit = u.RemoteIntent.Limit
		}
		if kind != core.RemoteInput && kind != core.RemoteCreate || u.RemoteIntent.Limit < 0 || t.Submit.WriteAttempts == 0 || kind == core.RemoteInput && t.Session.SessionID == "" {
			return core.ErrInvalid
		}
		if o == nil || o.Resolved {
			if kind == core.RemoteInput {
				var n int
				e = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+s.t.remoteOperations+" WHERE owner=? AND session_id=? AND resolved=0 AND id<>?", owner, t.Session.SessionID, id).Scan(&n)
				if e != nil {
					return e
				}
				if n > 0 {
					return core.ErrRemoteBarrier
				}
			}
			count := 0
			for _, k := range q.Remote {
				if k == kind {
					count++
				}
			}
			if u.RemoteIntent.Limit > 0 && count >= u.RemoteIntent.Limit {
				return core.ErrRemoteLimit
			}
			next := core.NewRemoteOperation(t, owner, kind)
			if u.RemoteIntent.UserAuthKey != "" {
				next.WorkKind = core.WorkKindUserAuth
			}
			next.UserAuthKey = u.RemoteIntent.UserAuthKey
			next.GrantVersion = u.RemoteIntent.GrantVersion
			o = &next
			q.Remote[id] = kind
		}
	}
	if o == nil {
		return nil
	}
	if o.Lease.Owner != "" {
		return core.ErrLeaseLost
	}
	if u.RemoteProof != nil && u.RemoteProof.Kind == core.RemoteInterrupted && !o.CleanupStarted {
		return core.ErrRemoteBarrier
	}
	if u.RemoteCleanup {
		if o.Resolved || o.CleanupStarted {
			return core.ErrInterruptStarted
		}
		o.State = "closing"
		o.CleanupStarted = true
		o.AttentionRequired = true
		o.Revision++
	}
	if u.RemoteProof != nil {
		next, e := o.Resolve(*u.RemoteProof, s.now(ctx))
		if e != nil {
			return e
		}
		o = &next
		delete(q.Remote, id)
	}
	if t.Session.SessionID != "" {
		o.SessionID = t.Session.SessionID
	}
	if e = s.putRemote(ctx, tx, *o, insert); e != nil {
		return e
	}
	return s.saveQuota(ctx, tx, owner, q)
}

// ListRemoteOperations 按稳定 ID 分页读取远端操作。
func (s *store) ListRemoteOperations(ctx context.Context, q core.RemoteOperationQuery) ([]core.RemoteOperation, error) {
	limit, e := core.PageLimit(q.Limit)
	if e != nil {
		return nil, e
	}
	query := "SELECT data FROM " + s.t.remoteOperations + " WHERE id>?"
	args := []any{q.After}
	if q.OpenOnly {
		query += " AND resolved=0"
	}
	if q.TurnID != "" {
		query += " AND turn_id=?"
		args = append(args, q.TurnID)
	}
	query += " ORDER BY id LIMIT ?"
	args = append(args, limit)
	return loadMany[core.RemoteOperation](ctx, s.db, query, args...)
}

// LeaseRemoteOperation 仅领取已本地终结的操作，转移独立清理 fence。
func (s *store) LeaseRemoteOperation(ctx context.Context, c core.RemoteClaim) (*core.RemoteOperation, error) {
	if c.Owner == "" || c.TTL <= 0 {
		return nil, core.ErrInvalid
	}
	var result *core.RemoteOperation
	e := s.runTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		candidates, e := loadIDs(ctx, tx, "SELECT r.id FROM "+s.t.remoteOperations+" r JOIN "+s.t.turns+" t ON t.id=r.turn_id WHERE r.resolved=0 AND r.eligible_at<=? AND t.status IN ('done','failed','stopped') ORDER BY r.id LIMIT 32 OFFSET ?", nanos(s.now(ctx)), s.remoteScanOffset.Load())
		if e != nil {
			return e
		}
		for _, id := range candidates {
			candidate, err := loadOne[core.RemoteOperation](ctx, tx, "SELECT data FROM "+s.t.remoteOperations+" WHERE id=?", id)
			if storedFormatError(err) {
				continue
			}
			if err != nil {
				return err
			}
			if candidate == nil || candidate.IsOpaqueStored() || candidate.WorkKind == core.WorkKindUserAuth && !c.UserAuth || candidate.Kind != core.RemoteCreate && candidate.Kind != core.RemoteInput {
				continue
			}
			if candidate.Lease.Live(s.now(ctx)) || s.now(ctx).Before(candidate.NextReconcileAt) {
				continue
			}
			// 账号先于 Turn/operation，避免和意图事务锁序逆转。
			if _, e = s.lockQuota(ctx, tx, candidate.Owner, true); errors.Is(e, core.ErrNotFound) {
				continue
			} else if e != nil {
				return e
			}
			turn, err := s.lockedTurn(ctx, tx, candidate.TurnID)
			if storedFormatError(err) {
				continue
			}
			if err != nil {
				return err
			}
			if turn == nil || !supportedTurn(*turn, c.UserAuth) || !turn.Status.Terminal() {
				continue
			}
			o, e := loadOne[core.RemoteOperation](ctx, tx, "SELECT data FROM "+s.t.remoteOperations+" WHERE id=?"+s.dialect.RowLockSuffix, candidate.ID)
			if e != nil {
				return e
			}
			if o == nil || o.IsOpaqueStored() || o.WorkKind == core.WorkKindUserAuth && !c.UserAuth || o.Kind != core.RemoteCreate && o.Kind != core.RemoteInput || o.Resolved || o.Lease.Live(s.now(ctx)) || s.now(ctx).Before(o.NextReconcileAt) {
				continue
			}
			o.Lease = core.Lease{ID: o.ID, Owner: c.Owner, Fence: o.Lease.Fence + 1, ExpiresAt: s.now(ctx).Add(c.TTL)}
			o.Revision++
			if e = s.putRemote(ctx, tx, *o, false); e != nil {
				return e
			}
			result = o
			s.remoteScanOffset.Store(0)
			return nil
		}
		if len(candidates) == 32 {
			s.remoteScanOffset.Add(32)
		} else {
			s.remoteScanOffset.Store(0)
		}
		return nil
	})
	return result, e
}

// UpdateRemoteOperation 以持久 fence 更新对账结果；未知不释放预算。
func (s *store) UpdateRemoteOperation(ctx context.Context, l core.Lease, u core.RemoteUpdate) (core.RemoteOperation, error) {
	var result core.RemoteOperation
	e := s.runTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		candidate, e := loadOne[core.RemoteOperation](ctx, tx, "SELECT data FROM "+s.t.remoteOperations+" WHERE id=?", l.ID)
		if e != nil {
			return e
		}
		if candidate == nil {
			return core.ErrLeaseLost
		}
		owner, q, e := s.lockRemoteQuota(ctx, tx, candidate.TurnID)
		if e != nil {
			return e
		}
		o, e := loadOne[core.RemoteOperation](ctx, tx, "SELECT data FROM "+s.t.remoteOperations+" WHERE id=?"+s.dialect.RowLockSuffix, l.ID)
		if e != nil {
			return e
		}
		if o == nil || o.Resolved || o.Lease.Owner != l.Owner || l.Owner == "" || o.Lease.Fence != l.Fence || !o.Lease.Live(s.now(ctx)) {
			return core.ErrLeaseLost
		}
		next, e := o.Apply(u, s.now(ctx))
		if e != nil {
			return e
		}
		o = &next
		if o.Resolved {
			delete(q.Remote, o.ID)
		}
		if e = s.putRemote(ctx, tx, *o, false); e != nil {
			return e
		}
		result = *o
		return s.saveQuota(ctx, tx, owner, q)
	})
	return result, e
}

// remoteDDL 定义远端意图与恢复索引。
func remoteDDL(d Dialect, prefix string) []string {
	t := tableNames(prefix).remoteOperations
	return []string{
		"CREATE TABLE IF NOT EXISTS " + t + " (id VARCHAR(255) NOT NULL PRIMARY KEY, owner VARCHAR(255) NOT NULL, turn_id VARCHAR(255) NOT NULL, session_id VARCHAR(255) NOT NULL, resolved BIGINT NOT NULL, eligible_at BIGINT NOT NULL, data " + d.TextType + " NOT NULL)",
		"CREATE INDEX " + t + "_turn ON " + t + " (turn_id, resolved)",
		"CREATE INDEX " + t + "_session ON " + t + " (session_id, resolved)",
		"CREATE INDEX " + t + "_owner ON " + t + " (owner, resolved)",
		"CREATE INDEX " + t + "_ready ON " + t + " (resolved, eligible_at)",
	}
}

// GetRemoteOperationContext 读取被未解远端操作固定保留的原始输入上下文。
func (s *store) GetRemoteOperationContext(ctx context.Context, id string) (*core.TurnLease, error) {
	o, e := loadOne[core.RemoteOperation](ctx, s.db, "SELECT data FROM "+s.t.remoteOperations+" WHERE id=?", id)
	if e != nil {
		return nil, e
	}
	if o == nil {
		return nil, core.ErrNotFound
	}
	t, e := s.turn(ctx, s.db, o.TurnID)
	if e != nil {
		return nil, e
	}
	if t == nil {
		return nil, core.ErrNotFound
	}
	m, e := loadOne[core.InboundMessage](ctx, s.db, "SELECT data FROM "+s.t.messages+" WHERE id=?", t.MessageID)
	if e != nil {
		return nil, e
	}
	v, e := loadOne[core.BindingVersion](ctx, s.db, "SELECT data FROM "+s.t.versions+" WHERE binding_id=? AND version=?", t.BindingID, t.BindingVersion)
	if e != nil {
		return nil, e
	}
	if m == nil || v == nil {
		return nil, core.ErrConflict
	}
	result := &core.TurnLease{Turn: *t, Message: *m, Binding: *v}
	if e = s.hydrateTurnLease(ctx, result); e != nil {
		return nil, e
	}
	return result, nil
}

func remoteEligible(o core.RemoteOperation) int64 {
	at := o.NextReconcileAt
	if o.Lease.ExpiresAt.After(at) {
		at = o.Lease.ExpiresAt
	}
	return nanos(at)
}
