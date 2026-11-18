// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

import (
	"bytes"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func legacyRecord(tx storeTx, bucket, id []byte) (bool, error) {
	raw := tx.Bucket(bucket).Get(id)
	if raw == nil {
		return true, nil
	}
	kind, err := core.StoredWorkKind(raw)
	return kind == core.WorkKindLegacy, err
}

func supportedTurn(tx storeTx, id string, userAuth ...bool) (bool, error) {
	ok, err := legacyRecord(tx, bucketTurns, []byte(id))
	if !ok && err == nil && len(userAuth) > 0 && userAuth[0] {
		kind, e := core.StoredWorkKind(tx.Bucket(bucketTurns).Get([]byte(id)))
		ok = kind == core.WorkKindUserAuth
		err = e
	}
	if err != nil || !ok {
		return ok, err
	}
	r, err := getTurn(tx, id)
	if err != nil || r == nil {
		return false, err
	}
	// Bolt key 是可信实体身份；未知输入协议必须保留原文并继续阻塞本会话。
	if r.ID != id || r.Input.IsOpaqueStored() {
		return false, nil
	}
	for _, block := range r.Input.Blocks {
		if block.IsOpaqueStored() {
			return false, nil
		}
	}
	for _, mount := range r.Input.Mounts {
		if mount.IsOpaqueStored() {
			return false, nil
		}
	}
	switch r.Phase {
	case core.PhaseNone, core.PhaseCreate, core.PhaseSend, core.PhaseObserve:
	default:
		return false, nil
	}
	return r.Snapshot == nil || r.Snapshot.SchemaVersion == 1, nil
}

func supportedDelivery(tx storeTx, id string) (bool, error) {
	raw := tx.Bucket(bucketDeliveries).Get([]byte(id))
	kind, err := core.StoredWorkKind(raw)
	ok := core.SupportsDeliveryWorkKind(kind)
	if err != nil || !ok {
		return ok, err
	}
	r, err := getDelivery(tx, id)
	if err != nil || r == nil {
		return false, err
	}
	switch r.Source.Kind {
	case core.SourceTurn, core.SourceControl, core.SourceProgress, core.SourceReaction:
	default:
		return false, nil
	}
	if r.Payloads == nil {
		return supportedDeliveryContent(r.Delivery), nil
	}
	return core.SupportsDeliveryBody(r.Body), nil
}

func supportedDeliveryContent(delivery core.Delivery) bool {
	return core.SupportsDeliveryBody(delivery.Body) && (delivery.Source.Kind == core.SourceReaction) == (delivery.Body.Type == core.BodyReaction)
}

func unsupportedSource(tx storeTx, source core.DeliverySource) (bool, error) {
	prefix := sourcePrefix(source)
	c := tx.Bucket(bucketDeliveryBySrc).Cursor()
	for k, id := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, id = c.Next() {
		ok, err := supportedDelivery(tx, string(id))
		if err != nil {
			return false, err
		}
		if !ok {
			return true, nil
		}
	}
	return false, nil
}
