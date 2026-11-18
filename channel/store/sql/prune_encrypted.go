// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"time"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/payload"
)

type pruneSourceProof struct {
	source    core.DeliverySource
	records   map[string]string
	supported bool
	terminal  bool
}

// preparePruneSource 只在显式 Prune 的事务外解密实际删除候选，不在 metadata 更新中调用。
func (s *store) preparePruneSource(ctx context.Context, source core.DeliverySource) (pruneSourceProof, error) {
	proof := pruneSourceProof{source: source, records: map[string]string{}, supported: true, terminal: true}
	rows, err := s.db.QueryContext(ctx, "SELECT id,status,data FROM "+s.t.deliveries+" WHERE source_kind=? AND source_id=?", string(source.Kind), source.ID)
	if err != nil {
		return proof, err
	}
	type item struct{ id, status, raw string }
	var records []item
	for rows.Next() {
		var record item
		if err = rows.Scan(&record.id, &record.status, &record.raw); err != nil {
			break
		}
		records = append(records, record)
	}
	if err == nil {
		err = rows.Err()
	}
	_ = rows.Close()
	if err != nil {
		return proof, err
	}
	for _, record := range records {
		proof.records[record.id] = record.raw
		if !core.DeliveryStatus(record.status).Terminal() {
			proof.terminal = false
		}

		var delivery core.Delivery
		if err = core.DecodeStored([]byte(record.raw), &delivery); err != nil {
			proof.supported = false
			continue
		}
		if !delivery.Status.Terminal() || string(delivery.Status) != record.status {
			proof.terminal = false
		}
		if !supportedDeliveryMetadata(delivery) {
			proof.supported = false
			continue
		}
		if err = s.hydrate(ctx, &delivery); err != nil {
			return proof, err
		}
		if !supportedDelivery(delivery) {
			proof.supported = false
		}
	}
	return proof, nil
}
func (s *store) verifyPruneSource(ctx context.Context, tx *sql.Tx, proof pruneSourceProof) (bool, error) {
	if !proof.supported || !proof.terminal {
		return false, nil
	}
	rows, err := tx.QueryContext(ctx, "SELECT id,status,data FROM "+s.t.deliveries+" WHERE source_kind=? AND source_id=?"+s.dialect.RowLockSuffix, string(proof.source.Kind), proof.source.ID)
	if err != nil {
		return false, err
	}
	defer func() { _ = rows.Close() }()
	count := 0
	for rows.Next() {
		var id, status, raw string
		if err = rows.Scan(&id, &status, &raw); err != nil {
			return false, err
		}
		if previous, ok := proof.records[id]; !ok || previous != raw || !core.DeliveryStatus(status).Terminal() {
			return false, nil
		}
		count++
	}
	return count == len(proof.records), rows.Err()
}

type pruneRecordCheck struct {
	id, raw string
	cutoff  int64
}

func (s *store) pruneTurnEligible(ctx context.Context, tx *sql.Tx, check pruneRecordCheck) (bool, error) {
	var raw string
	err := tx.QueryRowContext(ctx, "SELECT data FROM "+s.t.turns+" WHERE id=? AND status IN ('done','failed','stopped') AND settled_at>0 AND settled_at<?"+s.dialect.RowLockSuffix, check.id, check.cutoff).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil || raw != check.raw {
		return false, err
	}
	kind, err := core.StoredWorkKind([]byte(raw))
	if err != nil {
		return false, err
	}
	if kind != core.WorkKindLegacy {
		return false, nil
	}
	var turn core.Turn
	if err = core.DecodeStored([]byte(raw), &turn); err != nil {
		return false, err
	}
	if !supportedTurn(turn) || !turn.Status.Terminal() {
		return false, nil
	}
	if _, err := payload.References(s.payloadNamespace, "turn", check.id, []byte(raw)); err != nil {
		return false, err
	}
	return s.pruneChildrenSupported(ctx, tx, turn.MessageID, turn.ID)
}
func (s *store) pruneControlEligible(ctx context.Context, tx *sql.Tx, check pruneRecordCheck) (bool, error) {
	var raw string
	err := tx.QueryRowContext(ctx, "SELECT data FROM "+s.t.controls+" WHERE id=? AND created_at<? AND id NOT IN (SELECT source_id FROM "+s.t.rejected+")"+s.dialect.RowLockSuffix, check.id, check.cutoff).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil || raw != check.raw {
		return false, err
	}
	kind, err := core.StoredWorkKind([]byte(raw))
	if err != nil || kind != core.WorkKindLegacy {
		return false, err
	}
	var control core.ControlRequest
	if err = core.DecodeStored([]byte(raw), &control); err != nil {
		return false, err
	}
	return s.pruneChildrenSupported(ctx, tx, control.MessageID, "")
}

// A supported parent does not grant permission to destroy future child records.
// Inspect raw headers before any cascading deletion, then verify known encrypted
// envelopes. Row locks keep that proof valid until the deletion commits.
func (s *store) pruneMessageSupported(ctx context.Context, tx *sql.Tx, id string) (bool, error) {
	if id == "" {
		return true, nil
	}
	var raw string
	err := tx.QueryRowContext(ctx, "SELECT data FROM "+s.t.messages+" WHERE id=?"+s.dialect.RowLockSuffix, id).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	kind, err := core.StoredWorkKind([]byte(raw))
	if err != nil || kind != core.WorkKindLegacy {
		return false, err
	}
	if s.payloadCodec != nil {
		_, err = payload.References(s.payloadNamespace, "message", id, []byte(raw))
	}
	return err == nil, err
}

func (s *store) pruneChildrenSupported(ctx context.Context, tx *sql.Tx, messageID, turnID string) (bool, error) {
	if ok, err := s.pruneMessageSupported(ctx, tx, messageID); err != nil || !ok {
		return ok, err
	}
	if turnID == "" {
		return true, nil
	}
	rows, err := tx.QueryContext(ctx, "SELECT id,data FROM "+s.t.interactions+" WHERE turn_id=?"+s.dialect.RowLockSuffix, turnID)
	if err != nil {
		return false, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id string
		var raw []byte
		if err = rows.Scan(&id, &raw); err != nil {
			return false, err
		}
		kind, err := core.StoredWorkKind(raw)
		if err != nil || kind != core.WorkKindLegacy {
			return false, err
		}
		if s.payloadCodec != nil {
			if _, err = payload.References(s.payloadNamespace, "interaction", id, raw); err != nil {
				return false, err
			}
		}
	}
	return true, rows.Err()
}

func (s *store) pruneEncrypted(ctx context.Context, before time.Time) (core.PruneResult, error) {
	var removed core.PruneResult
	cutoff := nanos(before)
	const batch = 100
	if err := s.pruneRejectedSources(ctx, cutoff, &removed); err != nil {
		return removed, err
	}
	after := ""
	for {
		turns, err := loadMany[core.Turn](ctx, s.db, "SELECT data FROM "+s.t.turns+" WHERE id>? AND status IN ('done','failed','stopped') AND settled_at>0 AND settled_at<? AND id NOT IN (SELECT turn_id FROM "+s.t.remoteOperations+" WHERE resolved=0) AND id NOT IN (SELECT source_id FROM "+s.t.deliveries+" WHERE source_kind IN ('turn','progress','reaction') AND status IN ('pending','uncertain')) ORDER BY id LIMIT ?", after, cutoff, batch)
		if err != nil {
			return removed, err
		}
		for _, turn := range turns {
			after = turn.ID
			if !supportedTurn(turn) {
				continue
			}
			var raw string
			if err = s.db.QueryRowContext(ctx, "SELECT data FROM "+s.t.turns+" WHERE id=?", turn.ID).Scan(&raw); err != nil {
				return removed, err
			}
			answer, err := s.preparePruneSource(ctx, core.DeliverySource{Kind: core.SourceTurn, ID: turn.ID})
			if err != nil {
				return removed, err
			}
			progress, err := s.preparePruneSource(ctx, core.DeliverySource{Kind: core.SourceProgress, ID: turn.ID})
			if err != nil {
				return removed, err
			}
			reaction, err := s.preparePruneSource(ctx, core.DeliverySource{Kind: core.SourceReaction, ID: turn.ID})
			if err != nil {
				return removed, err
			}
			if !reaction.supported || !reaction.terminal {
				continue
			}
			if !answer.supported || !answer.terminal || !progress.supported || !progress.terminal {
				continue
			}
			var delta core.PruneResult
			err = s.runTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
				if err := s.lockConversation(ctx, tx, turn.ConversationID, false); err != nil {
					return err
				}
				same, err := s.pruneTurnEligible(ctx, tx, pruneRecordCheck{id: turn.ID, raw: raw, cutoff: cutoff})
				if err != nil || !same {
					return err
				}

				valid, err := s.verifyPruneSource(ctx, tx, answer)
				if err != nil || !valid {
					return err
				}
				valid, err = s.verifyPruneSource(ctx, tx, progress)
				if err != nil || !valid {
					return err
				}
				valid, err = s.verifyPruneSource(ctx, tx, reaction)
				if err != nil || !valid {
					return err
				}
				blocked, err := s.turnHasUnsupportedRemote(ctx, tx, turn.ID)
				if err != nil || blocked {
					return err
				}

				deleted, err := s.deleteSource(ctx, tx, answer.source, turn.MessageID)
				if err != nil {
					return err
				}
				reactionDeleted, err := s.deleteSource(ctx, tx, reaction.source, "")
				if err != nil {
					return err
				}
				progressDeleted, err := s.deleteSource(ctx, tx, progress.source, "")
				if err != nil {
					return err
				}
				if _, err = tx.ExecContext(ctx, "DELETE FROM "+s.t.interactions+" WHERE turn_id=?", turn.ID); err != nil {
					return err
				}
				if _, err = tx.ExecContext(ctx, "DELETE FROM "+s.t.turns+" WHERE id=? AND data=?", turn.ID, raw); err != nil {
					return err
				}
				delta = core.PruneResult{Turns: 1, Messages: 1, Deliveries: deleted + progressDeleted + reactionDeleted}
				return nil
			})
			if err != nil {
				return removed, err
			}
			removed.Turns += delta.Turns
			removed.Messages += delta.Messages
			removed.Deliveries += delta.Deliveries
		}
		if len(turns) < batch {
			break
		}
	}
	after = ""
	for {
		controls, err := loadMany[core.ControlRequest](ctx, s.db, "SELECT data FROM "+s.t.controls+" WHERE id>? AND created_at<? AND id NOT IN (SELECT source_id FROM "+s.t.rejected+") AND id NOT IN (SELECT source_id FROM "+s.t.deliveries+" WHERE source_kind='control' AND status IN ('pending','uncertain')) ORDER BY id LIMIT ?", after, cutoff, batch)
		if err != nil {
			return removed, err
		}
		for _, control := range controls {
			after = control.ID
			var raw string
			if err = s.db.QueryRowContext(ctx, "SELECT data FROM "+s.t.controls+" WHERE id=?", control.ID).Scan(&raw); err != nil {
				return removed, err
			}
			proof, err := s.preparePruneSource(ctx, core.DeliverySource{Kind: core.SourceControl, ID: control.ID})
			if err != nil {
				return removed, err
			}
			if !proof.supported || !proof.terminal {
				continue
			}
			var delta core.PruneResult
			err = s.runTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
				if err := s.lockConversation(ctx, tx, control.ConversationID, false); err != nil {
					return err
				}
				same, err := s.pruneControlEligible(ctx, tx, pruneRecordCheck{id: control.ID, raw: raw, cutoff: cutoff})
				if err != nil || !same {
					return err
				}
				pinned, err := s.recallSourcePinned(ctx, tx, control.MessageID)
				if err != nil || pinned {
					return err
				}
				valid, err := s.verifyPruneSource(ctx, tx, proof)
				if err != nil || !valid {
					return err
				}
				deleted, err := s.deleteSource(ctx, tx, proof.source, control.MessageID)
				if err != nil {
					return err
				}
				if _, err = tx.ExecContext(ctx, "DELETE FROM "+s.t.controls+" WHERE id=? AND data=?", control.ID, raw); err != nil {
					return err
				}
				delta = core.PruneResult{Controls: 1, Messages: 1, Deliveries: deleted}
				return nil
			})
			if err != nil {
				return removed, err
			}
			removed.Controls += delta.Controls
			removed.Messages += delta.Messages
			removed.Deliveries += delta.Deliveries
		}
		if len(controls) < batch {
			break
		}
	}
	return removed, s.prunePayloads(ctx)
}
