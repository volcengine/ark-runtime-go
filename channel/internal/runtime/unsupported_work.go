// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
)

func isUnsupportedWork(err error) bool {
	return !isStoreFailure(err) && (errors.Is(err, ErrUnsupportedWork) || errors.Is(err, ErrUnknownBody))
}

func storedWorkError(record StoredRecord) error {
	if record.IsOpaqueStored() {
		return ErrUnsupportedWork
	}
	if record.stored == nil {
		return nil
	}
	kind, err := StoredWorkKind(record.stored.raw)
	if err != nil {
		return err
	}
	if kind != WorkKindLegacy {
		return ErrUnsupportedWork
	}
	return nil
}

func turnWorkError(turn Turn) error {
	if err := storedWorkError(turn.StoredRecord); err != nil && (turn.IsOpaqueStored() || turn.WorkKind != WorkKindUserAuth || turn.StoredWorkKind() != turn.WorkKind) {
		return err
	}
	if turn.Input.IsOpaqueStored() {
		return ErrUnsupportedWork
	}
	for _, block := range turn.Input.Blocks {
		if block.IsOpaqueStored() {
			return ErrUnsupportedWork
		}
	}
	for _, mount := range turn.Input.Mounts {
		if mount.IsOpaqueStored() {
			return ErrUnsupportedWork
		}
	}
	switch turn.Phase {
	case PhaseNone, PhaseCreate, PhaseSend, PhaseObserve:
	default:
		return ErrUnsupportedWork
	}
	if turn.Snapshot != nil {
		if turn.Snapshot.SchemaVersion != 1 {
			return ErrUnsupportedWork
		}
		return storedWorkError(turn.Snapshot.StoredRecord)
	}
	return nil
}

func sessionWorkError(snapshot *SessionSnapshot) error {
	if snapshot == nil {
		return nil
	}
	if snapshot.SchemaVersion != 1 {
		return ErrUnsupportedWork
	}
	return storedWorkError(snapshot.StoredRecord)
}

func deliveryWorkError(delivery Delivery) error {
	if delivery.Body.IsOpaqueStored() {
		return ErrUnsupportedWork
	}
	if !SupportsDeliveryBody(delivery.Body) {
		return errors.Join(ErrUnsupportedWork, ErrUnknownBody)
	}
	if (delivery.Source.Kind == SourceReaction) != (delivery.Body.Type == BodyReaction) {
		return ErrUnsupportedWork
	}
	if err := storedWorkError(delivery.StoredRecord); err != nil {
		if delivery.IsOpaqueStored() || delivery.StoredWorkKind() != WorkKindReaction || delivery.Body.Type != BodyReaction {
			return err
		}
	}
	if delivery.Snapshot != nil {
		if delivery.Snapshot.SchemaVersion != 1 {
			return ErrUnsupportedWork
		}
		return storedWorkError(delivery.Snapshot.StoredRecord)
	}
	return nil
}

func remoteWorkError(operation RemoteOperation) error {
	if operation.Kind != RemoteCreate && operation.Kind != RemoteInput {
		return ErrUnsupportedWork
	}
	if err := storedWorkError(operation.StoredRecord); err != nil && (operation.IsOpaqueStored() || operation.WorkKind != WorkKindUserAuth || operation.StoredWorkKind() != operation.WorkKind) {
		return err
	}
	return nil
}

// waitUnsupported 只观测和退避，不交还、续写或结算其他处理器拥有的任务。
func (r *Runtime) waitUnsupported(ctx context.Context, item Observation) error {
	item.Name, item.Status, item.ErrorKind = EventWorkBlocked, "blocked", "unsupported_work"
	r.report(ctx, item)
	return r.waitPoll(ctx)
}
