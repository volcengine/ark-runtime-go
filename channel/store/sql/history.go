// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"context"
	"database/sql"
	"strconv"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// ListSessionHistory 按代读取长期保留的 Session 绑定。
func (s *store) ListSessionHistory(ctx context.Context, route core.RouteKey, after uint64, limit int) ([]core.SessionHistory, error) {
	n, err := core.PageLimit(limit)
	if err != nil {
		return nil, err
	}
	return loadMany[core.SessionHistory](ctx, s.db, "SELECT data FROM "+s.t.sessionHistory+" WHERE route = ? AND generation > ? ORDER BY generation LIMIT ?", string(route), after, n)
}

func (s *store) recordHistory(ctx context.Context, tx *sql.Tx, route core.RouteKey, binding core.SessionBinding) error {
	if binding.SnapshotID == "" {
		snap, err := loadOne[core.SessionSnapshot](ctx, tx, "SELECT data FROM "+s.t.sessionSnapshots+" WHERE route=? AND generation=?", string(route), binding.Generation)
		if err != nil {
			return err
		}
		if snap != nil {
			binding.SnapshotID = snap.ID
		}
	}
	id := string(route) + ":" + strconv.FormatUint(binding.Generation, 10)
	old, err := loadOne[core.SessionHistory](ctx, tx, "SELECT data FROM "+s.t.sessionHistory+" WHERE id = ?", id)
	if err != nil {
		return err
	}
	if old != nil {
		if old.Binding != binding {
			check := old.Binding
			check.SnapshotID = binding.SnapshotID
			if old.Binding.SnapshotID != "" || check != binding {
				return core.ErrConflict
			}
			old.Binding = binding
			data, err := encode(old)
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, "UPDATE "+s.t.sessionHistory+" SET snapshot_id=?,data=? WHERE id=?", binding.SnapshotID, data, id)
			return err
		}
		return nil
	}
	row := core.SessionHistory{Route: route, Binding: binding, CreatedAt: s.now(ctx)}
	data, err := encode(row)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO "+s.t.sessionHistory+" (id,route,generation,snapshot_id,session_id,created_at,data) VALUES (?,?,?,?,?,?,?)", id, string(route), binding.Generation, binding.SnapshotID, binding.SessionID, nanos(row.CreatedAt), data)
	return err
}
