// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// deliveryOrder 让持续状态投递排在普通答案之后，状态来源之间按到期时间公平领取。
const deliveryOrder = "CASE WHEN source_kind='reaction' THEN 1 ELSE 0 END,CASE WHEN source_kind='reaction' THEN eligible_at ELSE 0 END,inbox_seq,conversation_id,id"

func supportedTurn(turn core.Turn, userAuth ...bool) bool {
	kind := core.TurnWorkKind(turn)
	supported := kind == core.WorkKindLegacy || kind == core.WorkKindUserAuth && len(userAuth) > 0 && userAuth[0]
	if turn.IsOpaqueStored() || !supported || turn.Input.IsOpaqueStored() {
		return false
	}
	for _, block := range turn.Input.Blocks {
		if block.IsOpaqueStored() {
			return false
		}
	}
	for _, mount := range turn.Input.Mounts {
		if mount.IsOpaqueStored() {
			return false
		}
	}
	if turn.Snapshot != nil && (turn.Snapshot.SchemaVersion != 1 || turn.Snapshot.IsOpaqueStored() || turn.Snapshot.StoredWorkKind() != core.WorkKindLegacy) {
		return false
	}
	switch turn.Phase {
	case core.PhaseNone, core.PhaseCreate, core.PhaseSend, core.PhaseObserve:
		return true
	}
	return false
}
func supportedDelivery(delivery core.Delivery) bool {
	if delivery.IsOpaqueStored() {
		return false
	}
	if (delivery.Source.Kind == core.SourceReaction) != (delivery.Body.Type == core.BodyReaction) {
		return false
	}
	switch delivery.Source.Kind {
	case core.SourceTurn, core.SourceControl, core.SourceProgress, core.SourceReaction:
		return core.SupportsDeliveryBody(delivery.Body)
	}
	return false
}

// supportedDeliveryMetadata 允许保护层的空正文占位，真实 source/body 配对在解密后验证。
func supportedDeliveryMetadata(delivery core.Delivery) bool {
	if delivery.IsOpaqueStored() {
		return false
	}
	if delivery.Payloads != nil && delivery.Payloads.Refs["body"] != "" && delivery.Body.Type == core.BodyText && delivery.Body.Text == "" {
		switch delivery.Source.Kind {
		case core.SourceTurn, core.SourceControl, core.SourceProgress, core.SourceReaction:
			return true
		}
		return false
	}
	return supportedDelivery(delivery)
}

func storedFormatError(err error) bool {
	var syntax *json.SyntaxError
	var field *json.UnmarshalTypeError
	return errors.Is(err, core.ErrUnknownBody) || errors.As(err, &syntax) || errors.As(err, &field)
}

// sourceHasUnsupportedDelivery 在任何删除之前保守保留未知任务及其关联事实。
func (s *store) sourceHasUnsupportedDelivery(ctx context.Context, tx *sql.Tx, source core.DeliverySource, lock bool) (bool, error) {
	suffix := ""
	if lock {
		suffix = s.dialect.RowLockSuffix
	}
	rows, err := tx.QueryContext(ctx, "SELECT status,data FROM "+s.t.deliveries+" WHERE source_kind=? AND source_id=?"+suffix, string(source.Kind), source.ID)
	if err != nil {
		return false, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var status string
		var raw []byte
		if err = rows.Scan(&status, &raw); err != nil {
			return false, err
		}
		if !core.DeliveryStatus(status).Terminal() {
			return true, nil
		}
		var delivery core.Delivery
		if err = core.DecodeStored(raw, &delivery); err != nil {
			return true, nil
		}
		if !supportedDelivery(delivery) || !delivery.Status.Terminal() || string(delivery.Status) != status {
			return true, nil
		}
	}
	return false, rows.Err()
}

// leaseEncryptedDelivery 在锁外检查密文内的 Body；未知 Body 从未进入领取事务。
// 短事务重新核验完整原始 metadata，避免解密期间的续租/结算被旧视图覆盖。
func (s *store) leaseEncryptedDelivery(ctx context.Context, owner string, ttl time.Duration) (*core.Delivery, error) {
	offset := s.deliveryScanOffset.Load()
	type candidate struct{ id, raw string }
	query := "SELECT id,data FROM " + s.t.deliveries + " WHERE queue_head=1 AND status IN ('pending','uncertain') AND eligible_at<=? ORDER BY " + deliveryOrder + " LIMIT ? OFFSET ?"
	var batch []candidate
	for attempt := 0; attempt < 2; attempt++ {
		now := s.clock().UTC().UnixNano()
		if s.databaseClock {
			var micros int64
			if err := s.db.QueryRowContext(ctx, "SELECT CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(6))*1000000 AS UNSIGNED)").Scan(&micros); err != nil {
				return nil, err
			}
			now = time.UnixMicro(micros).UnixNano()
		}
		rows, err := s.db.QueryContext(ctx, query, now, leaseCandidateLimit, offset)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var item candidate
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
			return nil, err
		}
		if len(batch) > 0 || offset == 0 {
			break
		}
		offset = 0
		s.deliveryScanOffset.Store(0)
	}
	for _, item := range batch {
		var hydrated core.Delivery
		if err := core.DecodeStored([]byte(item.raw), &hydrated); err != nil {
			if storedFormatError(err) {
				continue
			}
			return nil, err
		}
		if !supportedDeliveryMetadata(hydrated) {
			continue
		}
		if err := s.hydrate(ctx, &hydrated); err != nil {
			return nil, err
		}
		if !supportedDelivery(hydrated) {
			continue
		}
		var claimed *core.Delivery
		err := s.runTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
			now := s.now(ctx)
			var raw string
			err := tx.QueryRowContext(ctx, "SELECT data FROM "+s.t.deliveries+" WHERE id=? AND queue_head=1 AND status IN ('pending','uncertain') AND eligible_at<=?"+s.dialect.RowLockSuffix+s.dialect.SkipLockedSuffix, item.id, now.UnixNano()).Scan(&raw)
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			if err != nil {
				return err
			}
			if raw != item.raw {
				return nil
			}
			var redacted core.Delivery
			if err = core.DecodeStored([]byte(raw), &redacted); err != nil {
				return err
			}
			if redacted.Status.Terminal() || now.Before(redacted.EligibleAt()) {
				return nil
			}
			next := redacted.Claim(owner, now, ttl)
			if err = s.putDelivery(ctx, tx, next, false); err != nil {
				return err
			}
			next.Body = hydrated.Body
			claimed = &next
			return nil
		})
		if err != nil {
			return nil, err
		}
		if claimed != nil {
			s.deliveryScanOffset.Store(0)
			return claimed, nil
		}
	}
	if len(batch) == leaseCandidateLimit {
		s.deliveryScanOffset.Store(offset + uint64(len(batch)))
	} else {
		s.deliveryScanOffset.Store(0)
	}
	return nil, nil
}

// turnHasUnsupportedPredecessor 在会话锁内复核前序记录，过期队尾也不能跨过未知协议。
func (s *store) turnHasUnsupportedPredecessor(ctx context.Context, tx *sql.Tx, rowID string, userAuth bool) (bool, error) {
	previous, err := loadMany[core.Turn](ctx, tx, "SELECT data FROM "+s.t.turns+" WHERE conversation_id=(SELECT conversation_id FROM "+s.t.turns+" WHERE id=?) AND seq<(SELECT seq FROM "+s.t.turns+" WHERE id=?) AND status IN ('queued','running','waiting')"+s.dialect.RowLockSuffix, rowID, rowID)
	if storedFormatError(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	for _, item := range previous {
		if !supportedTurn(item, userAuth) {
			return true, nil
		}
	}
	return false, nil
}

// turnHasUnsupportedRemote 对已终结的未知远端记录同样保守保留来源。
func (s *store) turnHasUnsupportedRemote(ctx context.Context, tx *sql.Tx, turnID string) (bool, error) {
	rows, err := tx.QueryContext(ctx, "SELECT data FROM "+s.t.remoteOperations+" WHERE turn_id=?"+s.dialect.RowLockSuffix, turnID)
	if err != nil {
		return false, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			return false, err
		}
		var operation core.RemoteOperation
		if err = core.DecodeStored(raw, &operation); err != nil {
			return true, nil
		}
		if operation.IsOpaqueStored() || !operation.Resolved || (operation.Kind != core.RemoteCreate && operation.Kind != core.RemoteInput) {
			return true, nil
		}
	}
	return false, rows.Err()
}
