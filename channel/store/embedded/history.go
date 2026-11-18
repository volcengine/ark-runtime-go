// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

import (
	"bytes"
	"context"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

var bucketHistory = []byte("session_history")

// ListSessionHistory 按代读取长期保留的 Session 绑定。
func (s *store) ListSessionHistory(ctx context.Context, route core.RouteKey, after uint64, limit int) ([]core.SessionHistory, error) {
	n, err := core.PageLimit(limit)
	if err != nil {
		return nil, err
	}
	result := []core.SessionHistory{}
	err = s.view(ctx, func(tx storeTx) error {
		prefix := append([]byte(route), 0)
		c := tx.Bucket(bucketHistory).Cursor()
		for k, _ := c.Seek(convKey(route, after)); k != nil && bytes.HasPrefix(k, prefix) && len(result) < n; k, _ = c.Next() {
			row, e := getJSON[core.SessionHistory](tx, bucketHistory, k)
			if e != nil {
				return e
			}
			if row.Binding.Generation > after {
				result = append(result, *row)
			}
		}
		return nil
	})
	return result, err
}
func (s *store) recordHistory(tx storeTx, route core.RouteKey, binding core.SessionBinding) error {
	if binding.SnapshotID == "" {
		snap, err := getJSON[core.SessionSnapshot](tx, bucketSnapshots, []byte(core.NewSessionSnapshot(route, binding.Generation, core.CreateSessionSpec{}).ID))
		if err != nil {
			return err
		}
		if snap != nil {
			binding.SnapshotID = snap.ID
		}
	}
	k := convKey(route, binding.Generation)
	old, err := getJSON[core.SessionHistory](tx, bucketHistory, k)
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
			return putJSON(tx, bucketHistory, k, old)
		}
		return nil
	}
	return putJSON(tx, bucketHistory, k, core.SessionHistory{Route: route, Binding: binding, CreatedAt: s.now()})
}
