// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

import (
	"context"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

var bucketSnapshots = []byte("session_snapshots")

// GetSessionSnapshot 返回已冻结的创建请求。
func (s *store) GetSessionSnapshot(ctx context.Context, id string) (*core.SessionSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var result *core.SessionSnapshot
	err := s.view(ctx, func(tx storeTx) error {
		var err error
		result, err = getJSON[core.SessionSnapshot](tx, bucketSnapshots, []byte(id))
		return err
	})
	if err == nil {
		err = s.hydrate(ctx, result)
	}
	return result, err
}

// FreezeSession 在同一事务中保存请求并关联当前 Turn。
func (s *store) FreezeSession(ctx context.Context, lease core.Lease, snapshot core.SessionSnapshot) (core.SessionSnapshot, error) {
	snapshot.Payloads = nil
	var result core.SessionSnapshot
	err := s.update(ctx, func(tx storeTx) error {
		record, err := s.leasedTurn(tx, lease)
		if err != nil {
			return err
		}
		if !snapshot.CanFreeze(record.Turn) {
			return core.ErrConflict
		}
		old, err := getJSON[core.SessionSnapshot](tx, bucketSnapshots, []byte(snapshot.ID))
		if err != nil {
			return err
		}
		if old != nil {
			if old.RequestHash != snapshot.RequestHash {
				return core.ErrConflict
			}
			snapshot = *old
		} else {
			snapshot.CreatedAt = s.now()
			if err := putJSON(tx, bucketSnapshots, []byte(snapshot.ID), snapshot); err != nil {
				return err
			}
		}
		conversation, err := getJSON[core.Conversation](tx, bucketConversations, []byte(record.ConversationID))
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
		conversation.UpdatedAt = s.now()
		if err := putJSON(tx, bucketConversations, []byte(conversation.ID), conversation); err != nil {
			return err
		}
		before := *record
		record.Session.SnapshotID = snapshot.ID
		record.Revision++
		if err := s.writeTurn(tx, &before, *record); err != nil {
			return err
		}
		result = snapshot
		if s.payloadCodec != nil {
			stored, err := getJSON[core.SessionSnapshot](tx, bucketSnapshots, []byte(result.ID))
			if err != nil {
				return err
			}
			if stored == nil {
				return core.ErrConflict
			}
			result = *stored
		}
		return nil
	})
	if err == nil {
		err = s.hydrate(ctx, &result)
	}
	return result, err
}
