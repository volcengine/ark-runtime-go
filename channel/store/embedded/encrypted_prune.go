// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

import (
	"bytes"
	"context"
	"time"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/payload"
)

type pruneSnapshot struct {
	rows    map[string][]byte
	blocked map[string]bool
}

func (s *store) pruneMessageSupported(tx storeTx, id string) (bool, error) {
	if ok, err := legacyRecord(tx, bucketMessages, []byte(id)); err != nil || !ok {
		return ok, err
	}
	if raw := tx.Bucket(bucketMessages).Get([]byte(id)); s.payloadCodec != nil && raw != nil {
		if _, err := payload.References(s.payloadNamespace, "message", id, raw); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (s *store) pruneChildrenSupported(tx storeTx, messageID, turnID string) (bool, error) {
	if ok, err := s.pruneMessageSupported(tx, messageID); err != nil || !ok {
		return ok, err
	}
	if turnID == "" {
		return true, nil
	}
	prefix := append([]byte(turnID), 0)
	c := tx.Bucket(bucketInteractionTurn).Cursor()
	for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
		id := k[len(prefix):]
		if ok, err := legacyRecord(tx, bucketInteractions, id); err != nil || !ok {
			return ok, err
		}
		if raw := tx.Bucket(bucketInteractions).Get(id); s.payloadCodec != nil && raw != nil {
			if _, err := payload.References(s.payloadNamespace, "interaction", string(id), raw); err != nil {
				return false, err
			}
		}
	}
	return true, nil
}

type pruneCheckedTx struct {
	storeTx
	proof *pruneSnapshot
}

// inspectPrune 解密只发生在显式清理的事务外，实际删除前仍核对原文快照。
func (s *store) inspectPrune(ctx context.Context, before time.Time) (*pruneSnapshot, error) {
	if s.payloadCodec == nil {
		return nil, nil
	}
	proof := &pruneSnapshot{rows: map[string][]byte{}, blocked: map[string]bool{}}
	var rows []core.Delivery
	err := s.view(ctx, func(tx storeTx) error {
		c := tx.Bucket(bucketDeliveries).Cursor()
		for id, raw := c.First(); id != nil; id, raw = c.Next() {
			var r deliveryRecord
			if e := core.DecodeStored(raw, &r); e != nil {
				return e
			}
			if !r.Status.Terminal() || !r.CreatedAt.Before(before) || r.Payloads == nil {
				continue
			}
			proof.rows[r.ID] = bytes.Clone(raw)
			rows = append(rows, r.Delivery)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		kind, e := core.StoredWorkKind(proof.rows[row.ID])
		if e != nil {
			return nil, e
		}
		if !core.SupportsDeliveryWorkKind(kind) {
			proof.blocked[row.ID] = true
			continue
		}
		if e = s.hydrate(ctx, &row); e != nil {
			return nil, e
		}
		if !supportedDeliveryContent(row) {
			proof.blocked[row.ID] = true
		}
	}
	return proof, nil
}

func (t *pruneCheckedTx) pinsSource(source core.DeliverySource) (bool, error) {
	prefix := sourcePrefix(source)
	c := t.Bucket(bucketDeliveryBySrc).Cursor()
	for k, id := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, id = c.Next() {
		r, err := getDelivery(t, string(id))
		if err != nil {
			return false, err
		}
		if r == nil || r.Payloads == nil {
			continue
		}
		if t.proof.blocked[r.ID] || !bytes.Equal(t.proof.rows[r.ID], t.Bucket(bucketDeliveries).Get(id)) {
			return true, nil
		}
	}
	return false, nil
}
