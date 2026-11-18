// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// budgetDDL 是 schema 6 受理容量与持久拒绝的附加表；槽位主键提供精确预算，无全局计数锁。
func budgetDDL(d Dialect, prefix string) []string {
	return []string{
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %schannel_ingest_locks (id VARCHAR(128) NOT NULL PRIMARY KEY)", prefix),
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %schannel_ingest_slots (slot BIGINT NOT NULL PRIMARY KEY, message_id VARCHAR(128) NOT NULL UNIQUE)", prefix),
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %schannel_rejected_messages (slot BIGINT NOT NULL PRIMARY KEY, message_id VARCHAR(128) NOT NULL, source_id VARCHAR(128) NOT NULL, delivery_id VARCHAR(128) NOT NULL, created_at BIGINT NOT NULL)", prefix),
		fmt.Sprintf("CREATE INDEX %schannel_rejected_messages_message ON %schannel_rejected_messages (message_id)", prefix, prefix),
		fmt.Sprintf("CREATE INDEX %schannel_rejected_messages_source ON %schannel_rejected_messages (source_id, created_at)", prefix, prefix),
	}
}
func (s *store) insertIgnore() string {
	if s.dialect.Name == mysqlName {
		return "INSERT IGNORE INTO "
	}
	return "INSERT OR IGNORE INTO "
}
func (s *store) lockIngestChannel(ctx context.Context, tx *sql.Tx, b core.Binding) error {
	id := core.IngestChannelScope(b)
	statement := "INSERT INTO " + s.t.ingestLocks + " (id) VALUES (?) ON CONFLICT(id) DO UPDATE SET id=excluded.id"
	if s.dialect.Name == mysqlName {
		statement = "INSERT INTO " + s.t.ingestLocks + " (id) VALUES (?) ON DUPLICATE KEY UPDATE id=VALUES(id)"
	}
	if _, err := tx.ExecContext(ctx, statement, id); err != nil {
		return err
	}
	var got string
	return tx.QueryRowContext(ctx, "SELECT id FROM "+s.t.ingestLocks+" WHERE id=?"+s.dialect.RowLockSuffix, id).Scan(&got)
}

// reserveIngestSlot 使用唯一主键竞争空槽；不对全局计数或其他账号加锁。
func (s *store) reserveIngestSlot(ctx context.Context, tx *sql.Tx, table string, limit int, columns string, values []any) error {
	if limit <= 0 {
		return core.ErrQueueFull
	}
	rows, err := tx.QueryContext(ctx, "SELECT slot FROM "+table+" ORDER BY slot")
	if err != nil {
		return err
	}
	used := make(map[int]bool)
	for rows.Next() {
		var slot int
		if err = rows.Scan(&slot); err != nil {
			_ = rows.Close()
			return err
		}
		used[slot] = true
	}
	err = rows.Err()
	_ = rows.Close()
	if err != nil {
		return err
	}
	marks := "?"
	for range values {
		marks += ",?"
	}
	available := limit - len(used)
	if available <= 0 {
		return core.ErrQueueFull
	}
	for slot := 0; slot < limit && available > 0; slot++ {
		if used[slot] {
			continue
		}
		available--
		args := append([]any{slot}, values...)
		r, e := tx.ExecContext(ctx, s.insertIgnore()+table+" (slot,"+columns+") VALUES ("+marks+")", args...)
		if e != nil {
			return e
		}
		n, e := r.RowsAffected()
		if e != nil {
			return e
		}
		if n == 1 {
			return nil
		}
	}
	return core.ErrQueueFull
}
func (s *store) channelAtCapacity(ctx context.Context, tx *sql.Tx, b core.Binding) (bool, error) {
	if s.maxChannelOpenTurns == 0 {
		return false, nil
	}
	turns, err := loadMany[core.Turn](ctx, tx, "SELECT data FROM "+s.t.turns+" WHERE status IN ('queued','running','waiting')")
	if err != nil {
		return false, err
	}
	count := 0
	versions := map[string]*core.BindingVersion{}
	for _, t := range turns {
		k := t.BindingID + "|" + t.BindingVersion
		v := versions[k]
		var err error
		if v == nil {
			v, err = loadOne[core.BindingVersion](ctx, tx, "SELECT data FROM "+s.t.versions+" WHERE binding_id=? AND version=?", t.BindingID, t.BindingVersion)
			versions[k] = v
		}
		if err != nil {
			return false, err
		}
		if v == nil {
			return false, core.ErrConflict
		}
		if core.IngestChannelScope(v.Content) == core.IngestChannelScope(b) {
			count++
			if count >= s.maxChannelOpenTurns {
				return true, nil
			}
		}
	}
	return false, nil
}
func (s *store) rejectedResult(ctx context.Context, tx *sql.Tx, message core.InboundMessage) (*core.IngestResult, error) {
	var source, id string
	err := tx.QueryRowContext(ctx, "SELECT source_id,delivery_id FROM "+s.t.rejected+" WHERE message_id=? LIMIT 1", message.ID).Scan(&source, &id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	d, err := s.delivery(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	if d == nil {
		return nil, core.ErrConflict
	}
	c, err := loadOne[core.ControlRequest](ctx, tx, "SELECT data FROM "+s.t.controls+" WHERE id=?", source)
	if err != nil {
		return nil, err
	}
	if c == nil {
		return nil, core.ErrConflict
	}
	return &core.IngestResult{Message: message, Duplicate: true, Rejected: true, Control: c, Reply: d}, nil
}
func (s *store) reserveRejection(ctx context.Context, tx *sql.Tx, r core.RejectedMessage) error {
	return s.reserveIngestSlot(ctx, tx, s.t.rejected, s.maxRejectedMessages, "message_id,source_id,delivery_id,created_at", []any{r.MessageID, r.ControlID, r.DeliveryID, r.CreatedAt})
}
func (s *store) persistRejected(ctx context.Context, tx *sql.Tx, message core.InboundMessage, v core.BindingVersion, now time.Time, result *core.IngestResult) error {
	if s.maxRejectedMessages == 0 {
		return core.ErrQueueFull
	}
	controls, err := loadMany[core.ControlRequest](ctx, tx, "SELECT data FROM "+s.t.controls+" WHERE conversation_id=? AND created_at>? ORDER BY created_at DESC", string(message.ConversationID), nanos(now.Add(-s.rejectNoticeInterval)))
	if err != nil {
		return err
	}
	var control *core.ControlRequest
	var reply *core.Delivery
	for _, c := range controls {
		if c.Kind != core.RejectControlKind {
			continue
		}
		ds, err := loadMany[core.Delivery](ctx, tx, "SELECT data FROM "+s.t.deliveries+" WHERE source_kind=? AND source_id=? LIMIT 1", string(core.SourceControl), c.ID)
		if err != nil {
			return err
		}
		if len(ds) == 1 {
			control = &c
			reply = &ds[0]
			break
		}
	}
	if control == nil {
		c, d := core.NewRejectControl(message, v)
		c.CreatedAt = now
		data, err := encode(c)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO "+s.t.controls+" (id,seq,message_id,conversation_id,created_at,data) VALUES (?,?,?,?,?,?)", c.ID, c.Seq, c.MessageID, string(c.ConversationID), nanos(now), data); err != nil {
			return err
		}
		if err = s.insertDeliveries(ctx, tx, d.Source, message.ConversationID, []core.Delivery{d}); err != nil {
			return err
		}
		reply, err = s.delivery(ctx, tx, d.ID)
		if err != nil {
			return err
		}
		control = &c
	}
	if err = s.reserveRejection(ctx, tx, core.RejectedMessage{MessageID: message.ID, ControlID: control.ID, DeliveryID: reply.ID, CreatedAt: nanos(now)}); err != nil {
		return err
	}
	result.Rejected = true
	result.Control = control
	result.Reply = reply
	return nil
}
func (s *store) pruneRejectedSource(ctx context.Context, tx *sql.Tx, id string, cutoff int64, removed *core.PruneResult) (bool, error) {
	var undelivered int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+s.t.deliveries+" WHERE source_kind=? AND source_id=? AND status<>?", string(core.SourceControl), id, string(core.DeliveryDone)).Scan(&undelivered); err != nil {
		return false, err
	}
	if undelivered > 0 {
		return false, nil
	}
	var retained int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+s.t.rejected+" WHERE source_id=? AND created_at>=?", id, cutoff).Scan(&retained); err != nil {
		return false, err
	}
	if retained > 0 {
		return false, nil
	}
	ids, err := loadIDs(ctx, tx, "SELECT DISTINCT message_id FROM "+s.t.rejected+" WHERE source_id=?", id)
	if err != nil {
		return false, err
	}
	for _, messageID := range ids {
		if supported, err := s.pruneMessageSupported(ctx, tx, messageID); err != nil || !supported {
			return false, err
		}
	}
	for _, messageID := range ids {
		if _, err = tx.ExecContext(ctx, "DELETE FROM "+s.t.eventIDs+" WHERE message_id=?", messageID); err != nil {
			return false, err
		}
		if _, err = tx.ExecContext(ctx, "DELETE FROM "+s.t.messages+" WHERE id=?", messageID); err != nil {
			return false, err
		}
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM "+s.t.rejected+" WHERE source_id=?", id); err != nil {
		return false, err
	}
	if len(ids) > 1 {
		removed.Messages += len(ids) - 1
	}
	return true, nil
}

// pruneRejectedSources 按真实诊断来源独立提交清理，与 Ingest 使用同一 Channel 锁。
// 每次只锁一个 Channel，避免批量清理时跨账号的锁顺序和长事务。
func (s *store) pruneRejectedSources(ctx context.Context, cutoff int64, removed *core.PruneResult) error {
	const batch = 100
	after := ""
	for {
		controls, err := loadMany[core.ControlRequest](ctx, s.db, "SELECT c.data FROM "+s.t.controls+" c WHERE c.id>? AND c.created_at<? AND EXISTS (SELECT 1 FROM "+s.t.rejected+" r WHERE r.source_id=c.id) AND NOT EXISTS (SELECT 1 FROM "+s.t.rejected+" r WHERE r.source_id=c.id AND r.created_at>=?) AND NOT EXISTS (SELECT 1 FROM "+s.t.deliveries+" d WHERE d.source_kind=? AND d.source_id=c.id AND d.status<>?) ORDER BY c.id LIMIT ?", after, cutoff, cutoff, string(core.SourceControl), string(core.DeliveryDone), batch)
		if err != nil {
			return err
		}
		if len(controls) == 0 {
			return nil
		}
		for _, control := range controls {
			after = control.ID
			var proof pruneSourceProof
			if s.payloadCodec != nil {
				var err error
				proof, err = s.preparePruneSource(ctx, core.DeliverySource{Kind: core.SourceControl, ID: control.ID})
				if err != nil {
					return err
				}
				if !proof.supported || !proof.terminal {
					continue
				}
			}
			var delta core.PruneResult
			err = s.runTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
				blocked, err := s.sourceHasUnsupportedDelivery(ctx, tx, core.DeliverySource{Kind: core.SourceControl, ID: control.ID}, false)
				if err != nil {
					return err
				}
				if blocked {
					return nil
				}
				deliveries, err := loadMany[core.Delivery](ctx, tx, "SELECT data FROM "+s.t.deliveries+" WHERE source_kind=? AND source_id=? LIMIT 1", string(core.SourceControl), control.ID)
				if err != nil {
					return err
				}
				if len(deliveries) == 0 {
					return nil
				}
				if deliveries[0].Snapshot == nil {
					return core.ErrConflict
				}
				if err = s.lockIngestChannel(ctx, tx, deliveries[0].Snapshot.Binding); err != nil {
					return err
				}
				// 保持 Channel 锁→delivery 行锁，与 Ingest 一致；锁内再次核验可清理状态。
				blocked, err = s.sourceHasUnsupportedDelivery(ctx, tx, core.DeliverySource{Kind: core.SourceControl, ID: control.ID}, true)
				if err != nil || blocked {
					return err
				}
				if s.payloadCodec != nil {
					valid, err := s.verifyPruneSource(ctx, tx, proof)
					if err != nil || !valid {
						return err
					}
				}
				// 等锁期间来源可能已由另一个 Prune 清理；不存在时保持幂等。
				current, err := loadOne[core.ControlRequest](ctx, tx, "SELECT data FROM "+s.t.controls+" WHERE id=?", control.ID)
				if err != nil {
					return err
				}
				if current == nil {
					return nil
				}
				ok, err := s.pruneRejectedSource(ctx, tx, control.ID, cutoff, &delta)
				if err != nil {
					return err
				}
				if !ok {
					return nil
				}
				n, err := s.deleteSource(ctx, tx, core.DeliverySource{Kind: core.SourceControl, ID: control.ID}, control.MessageID)
				if err != nil {
					return err
				}
				if _, err = tx.ExecContext(ctx, "DELETE FROM "+s.t.controls+" WHERE id=?", control.ID); err != nil {
					return err
				}
				delta.Deliveries += n
				delta.Controls++
				delta.Messages++
				return nil
			})
			if err != nil {
				return err
			}
			removed.Deliveries += delta.Deliveries
			removed.Controls += delta.Controls
			removed.Messages += delta.Messages
		}
		if len(controls) < batch {
			return nil
		}
	}
}
