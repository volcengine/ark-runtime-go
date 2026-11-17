// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// GetSessionSnapshot 返回已冻结的创建请求。
func (s *store) GetSessionSnapshot(ctx context.Context, id string) (*core.SessionSnapshot, error) {
	v, err := loadOne[core.SessionSnapshot](ctx, s.db, "SELECT data FROM "+s.t.sessionSnapshots+" WHERE id = ?", id)
	if err == nil && v != nil {
		err = s.hydrate(ctx, v)
	}
	return v, err
}

// FreezeSession 在同一事务中保存请求并关联当前 Turn。
func (s *store) FreezeSession(ctx context.Context, lease core.Lease, snapshot core.SessionSnapshot) (core.SessionSnapshot, error) {
	var result core.SessionSnapshot
	requested := snapshot
	err := s.runTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		snapshot = requested
		candidate, err := s.turn(ctx, tx, lease.ID)
		if err != nil {
			return err
		}
		if candidate == nil {
			return core.ErrLeaseLost
		}
		if err := s.lockConversation(ctx, tx, candidate.ConversationID, false); err != nil {
			return err
		}
		turn, err := s.leasedTurn(ctx, tx, lease)
		if err != nil {
			return err
		}
		if !snapshot.CanFreeze(turn) {
			return core.ErrConflict
		}
		old, err := loadOne[core.SessionSnapshot](ctx, tx, "SELECT data FROM "+s.t.sessionSnapshots+" WHERE id = ?", snapshot.ID)
		if err != nil {
			return err
		}
		if old != nil {
			if old.RequestHash != snapshot.RequestHash {
				return core.ErrConflict
			}
			snapshot = *old
		} else {
			snapshot.Payloads = nil
			snapshot.CreatedAt = s.now(ctx)
			data, err := s.protectedData(ctx, tx, snapshot, snapshot.Spec.Binding)
			if err != nil {
				return err
			}

			if s.payloadCodec != nil {
				var meta core.SessionSnapshot
				if err = json.Unmarshal([]byte(data), &meta); err != nil {
					return err
				}
				snapshot = meta
			}
			_, err = tx.ExecContext(ctx, "INSERT INTO "+s.t.sessionSnapshots+" (id,route,generation,owner,creation_key,request_hash,created_at,updated_at,data) VALUES (?,?,?,?,?,?,?,?,?)", snapshot.ID, string(snapshot.Route), snapshot.Generation, snapshot.Spec.Binding.Owner, snapshot.Spec.CreationKey, snapshot.RequestHash, nanos(snapshot.CreatedAt), nanos(snapshot.CreatedAt), data)
			if err != nil {
				if isDuplicateRecord(err) {
					return core.ErrConflict
				}
				return err
			}
		}
		conversation, err := s.conversation(ctx, tx, turn.ConversationID)
		if err != nil {
			return err
		}
		if conversation == nil {
			return core.ErrConflict
		}
		if conversation.GenerationHighWater < snapshot.Generation {
			conversation.GenerationHighWater = snapshot.Generation
		}
		conversation.Revision++
		conversation.UpdatedAt = s.now(ctx)
		if err := s.putConversation(ctx, tx, *conversation, false); err != nil {
			return err
		}
		turn.Session.SnapshotID = snapshot.ID
		turn.Revision++
		if err := s.putTurn(ctx, tx, turn, false, false); err != nil {
			return err
		}
		result = snapshot
		return nil
	})
	if err == nil && s.payloadCodec != nil {
		err = s.hydrate(ctx, &result)
	}
	return result, err
}
