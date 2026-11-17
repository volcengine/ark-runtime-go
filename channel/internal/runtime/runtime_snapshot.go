// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"slices"
)

var errSnapshotHook = errors.New("session snapshot hook failed")
var errSnapshotEnvironment = errors.New("session snapshot environment failed")

// creationSnapshot 在远程创建前冻结 Hook 与 Environment 的结果；重试仅读取持久请求。
func (r *Runtime) creationSnapshot(ctx context.Context, j *job, connection Connection) (*SessionSnapshot, error) {
	if j.Session.SnapshotID != "" {
		snapshot, err := r.store.GetSessionSnapshot(ctx, j.Session.SnapshotID)
		if err != nil {
			return nil, err
		}
		if snapshot == nil || !snapshot.Valid() || snapshot.Route != j.ConversationID || snapshot.Generation != j.Session.Generation {
			return nil, ErrConflict
		}
		return snapshot, nil
	}
	creationJob := *j
	if connection.PrepareSession != nil {
		binding, err := connection.PrepareSession(ctx, j.binding.clone())
		if err != nil {
			return nil, err
		}
		if binding.ID != j.binding.ID || binding.AppID != j.binding.AppID || binding.Owner != j.binding.Owner || binding.Project != j.binding.Project || binding.BindingEpoch != j.binding.BindingEpoch {
			return nil, ErrConflict
		}
		creationJob.binding = binding
	}
	if j.userCredential != nil && !slices.Contains(creationJob.binding.VaultIDs, j.userCredential.VaultID) {
		creationJob.binding.VaultIDs = append(cloneStrings(creationJob.binding.VaultIDs), j.userCredential.VaultID)
	}
	extras, err := r.sessionExtras(ctx, &creationJob)
	if err != nil {
		return nil, errors.Join(errSnapshotHook, err)
	}
	plan, err := r.environmentPlan(ctx, connection, &creationJob, extras)
	if err != nil {
		return nil, errors.Join(errSnapshotEnvironment, err)
	}
	spec := CreateSessionSpec{UserCredential: j.userCredential, Binding: creationJob.binding, CreationKey: j.Session.CreationKey, LookupKey: lookupKey(j.ConversationID, j.Session.Generation), Extras: extras, EnvironmentOverride: plan.override, EnvironmentType: plan.baseType, ChatType: j.msg.ChatType}
	snapshot, err := r.store.FreezeSession(ctx, j.Lease, NewSessionSnapshot(j.ConversationID, j.Session.Generation, spec))
	if err != nil {
		return nil, err
	}
	j.Session.SnapshotID = snapshot.ID
	return &snapshot, nil
}
