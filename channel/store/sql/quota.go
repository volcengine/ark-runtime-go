// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"context"
	"database/sql"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

type accountQuota struct {
	CreationLimit int                        `json:"creation_limit"`
	Revision      uint64                     `json:"revision"`
	Remote        map[string]core.RemoteKind `json:"remote,omitempty"`
	Held          map[string]bool            `json:"held"`
	Limit         int                        `json:"limit"`
}

func (s *store) quotaOwner(ctx context.Context, tx *sql.Tx, turn core.Turn) (string, error) {
	if turn.Snapshot != nil {
		return turn.Snapshot.Binding.Owner, nil
	}
	v, err := loadOne[core.BindingVersion](ctx, tx, "SELECT data FROM "+s.t.versions+" WHERE binding_id = ? AND version = ?", turn.BindingID, turn.BindingVersion)
	if err != nil {
		return "", err
	}
	if v == nil {
		return "", core.ErrConflict
	}
	return v.Content.Owner, nil
}

// lockQuota 只锁当前账号，始终先于会话锁获取。
func (s *store) lockQuota(ctx context.Context, tx *sql.Tx, owner string, skipLocked bool) (*accountQuota, error) {
	suffix := s.dialect.RowLockSuffix
	if skipLocked {
		suffix += s.dialect.SkipLockedSuffix
	}
	q, err := loadOne[accountQuota](ctx, tx, "SELECT data FROM "+s.t.accountQuotas+" WHERE owner = ?"+suffix, owner)
	if err != nil {
		return nil, err
	}
	if q != nil {
		return q, nil
	}
	if skipLocked && s.dialect.SkipLockedSuffix != "" {
		var count int
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+s.t.accountQuotas+" WHERE owner = ?", owner).Scan(&count); err != nil {
			return nil, err
		}
		if count > 0 {
			return nil, core.ErrNotFound
		}
	}
	q = &accountQuota{Held: map[string]bool{}}
	suffixInsert := ""
	if s.dialect.Name == mysqlName {
		suffixInsert = " ON DUPLICATE KEY UPDATE owner=VALUES(owner)"
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO "+s.t.accountQuotas+" (owner,open_count,quota,updated_at,data) VALUES (?,0,0,?,?)"+suffixInsert, owner, nanos(s.now(ctx)), mustEncode(q))
	if err != nil {
		return nil, err
	}
	return loadOne[accountQuota](ctx, tx, "SELECT data FROM "+s.t.accountQuotas+" WHERE owner = ?"+s.dialect.RowLockSuffix, owner)
}
func (s *store) saveQuota(ctx context.Context, tx *sql.Tx, owner string, q *accountQuota) error {
	creation, execution := 0, 0
	for _, kind := range q.Remote {
		if kind == core.RemoteCreate {
			creation++
		} else {
			execution++
		}
	}
	q.Revision++
	_, err := tx.ExecContext(ctx, "UPDATE "+s.t.accountQuotas+" SET open_count=?,quota=?,creation_count=?,creation_limit=?,execution_count=?,execution_limit=?,revision=?,updated_at=?,data=? WHERE owner=?", execution, q.Limit, creation, q.CreationLimit, execution, q.Limit, q.Revision, nanos(s.now(ctx)), mustEncode(q), owner)
	return err
}
