// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"context"
	"database/sql"
	"errors"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/payload"
)

// Writers take the shared gate before any business locks; maintenance takes the
// exclusive gate before scanning roots. SQLite immediate transactions already
// serialize writers. MySQL readers and ordinary concurrent writers stay parallel.
func (s *store) lockPayloadMaintenance(ctx context.Context, tx *sql.Tx, exclusive bool) error {
	if s.dialect.Name != mysqlName {
		return nil
	}
	suffix := " LOCK IN SHARE MODE"
	if exclusive {
		suffix = s.dialect.RowLockSuffix
	}
	var mode string
	return tx.QueryRowContext(ctx, "SELECT v FROM "+s.t.meta+" WHERE k='payload_mode'"+suffix).Scan(&mode)
}

// prunePayloads rebuilds strong links from current raw roots in one transaction.
// Session snapshots remain roots independently of short-lived turns; opaque
// records retain every reference in their storage envelope without decryption.
func (s *store) prunePayloads(ctx context.Context) error {
	err := s.runTxOnce(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if err := s.lockPayloadMaintenance(ctx, tx, true); err != nil {
			return err
		}
		refs := s.payloadTable("payload_refs")
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+refs); err != nil {
			return err
		}
		for _, root := range []struct{ kind, table string }{
			{"message", s.t.messages}, {"turn", s.t.turns},
			{"snapshot", s.t.sessionSnapshots}, {"delivery", s.t.deliveries},
			{"interaction", s.t.interactions},
		} {
			after := ""
			for {
				rows, err := tx.QueryContext(ctx, "SELECT id,data FROM "+root.table+" WHERE id>? ORDER BY id LIMIT 100", after)
				if err != nil {
					return err
				}
				type record struct{ id, raw string }
				var batch []record
				for rows.Next() {
					var item record
					if err = rows.Scan(&item.id, &item.raw); err != nil {
						break
					}
					batch = append(batch, item)
				}
				if err == nil {
					err = rows.Err()
				}
				_ = rows.Close()
				if err != nil {
					return err
				}
				for _, item := range batch {
					links, err := payload.References(s.payloadNamespace, root.kind, item.id, []byte(item.raw))
					if err != nil {
						return err
					}
					for field, ref := range links {
						if _, err = tx.ExecContext(ctx, "INSERT INTO "+refs+" (record_kind,record_id,field,payload_id) VALUES (?,?,?,?)", root.kind, item.id, field, ref); err != nil {
							return err
						}
					}
					after = item.id
				}
				if len(batch) < 100 {
					break
				}
			}
		}
		_, err := tx.ExecContext(ctx, "DELETE FROM "+s.payloadTable("sensitive_payloads")+" WHERE id NOT IN (SELECT payload_id FROM "+refs+")")
		return err
	})
	if errors.Is(err, core.ErrUnsupportedWork) {
		return nil // Transaction rollback preserves all links/ciphertext for opaque recovery.
	}
	return err
}
