// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDeletedCredentialRecoveryRequiresObservedReferenceAndClosedWriters(t *testing.T) {
	now := time.Now().UTC()
	p := UserPrincipal{Owner: "owner", Project: "project", ChannelID: "channel", Epoch: 1, AppID: "app", TenantKey: "tenant", OpenID: "sender"}
	original := UserAuthRecord{Principal: p, VaultID: "vault", CredentialID: "credential", State: UserAuthUncertain, Operation: UserAuthAuthorize, Revision: 7, Attempt: 3, FlowID: "old-flow", Grant: UserGrant{Status: AuthReady, VaultID: "vault", CredentialID: "credential", GrantVersion: "1"}, RequiredScopes: []string{"old"}, BatchScopes: []string{"old"}, VerifiedScopes: []string{"old"}, Batch: 2, ExpiresAt: now.Add(-time.Minute), Lease: Lease{ID: p.Key(), Fence: 4}}
	for _, tc := range []struct {
		name   string
		mutate func(*UserAuthRecord, *UserAuthBegin)
		want   error
	}{
		{"valid", func(*UserAuthRecord, *UserAuthBegin) {}, nil},
		{"missing_revision", func(_ *UserAuthRecord, b *UserAuthBegin) { b.ExpectedRevision = nil }, ErrInvalid},
		{"stale_revision", func(_ *UserAuthRecord, b *UserAuthBegin) { v := uint64(6); b.ExpectedRevision = &v }, ErrConflict},
		{"wrong_id", func(_ *UserAuthRecord, b *UserAuthBegin) { b.DeletedCredentialID = "other" }, ErrConflict},
		{"wrong_vault", func(_ *UserAuthRecord, b *UserAuthBegin) { b.VaultID = "other" }, ErrConflict},
		{"wrong_subject", func(_ *UserAuthRecord, b *UserAuthBegin) { b.Principal.OpenID = "other" }, ErrConflict},
		{"live_lease", func(r *UserAuthRecord, _ *UserAuthBegin) {
			r.Lease.Owner = "other"
			r.Lease.ExpiresAt = now.Add(time.Hour)
		}, ErrConflict},
		{"refresh_unknown", func(r *UserAuthRecord, _ *UserAuthBegin) { r.Operation = UserAuthRefresh }, ErrUserAuthGate},
		{"secret", func(r *UserAuthRecord, _ *UserAuthBegin) {
			r.Secret = &UserAuthSecret{Version: 1, KeyID: "key", Ciphertext: []byte("evidence")}
		}, ErrUserAuthGate},
		{"pending", func(r *UserAuthRecord, _ *UserAuthBegin) { r.State = UserAuthPending }, ErrUserAuthGate},
		{"verifying", func(r *UserAuthRecord, _ *UserAuthBegin) { r.State = UserAuthVerifying }, ErrUserAuthGate},
		{"grant_mismatch", func(r *UserAuthRecord, _ *UserAuthBegin) { r.Grant.CredentialID = "other" }, ErrConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := original
			revision := r.Revision
			b := UserAuthBegin{Principal: p, VaultID: "vault", Owner: "worker", TTL: time.Minute, ExpectedRevision: &revision, DeletedCredentialID: "credential"}
			tc.mutate(&r, &b)
			next, err := r.Begin(b, now)
			if tc.want != nil {
				require.ErrorIs(t, err, tc.want)
				require.Equal(t, r, next)
				return
			}
			require.NoError(t, err)
			require.Equal(t, "vault", next.VaultID)
			require.Equal(t, UserAuthPending, next.State)
			require.Equal(t, uint64(4), next.Attempt)
			require.Equal(t, uint64(5), next.Lease.Fence)
			require.Empty(t, next.CredentialID)
			require.Empty(t, next.FlowID)
			require.Empty(t, next.Grant)
			require.Empty(t, next.RequiredScopes)
			require.Empty(t, next.VerifiedScopes)
			require.Zero(t, next.Batch)
			require.True(t, next.ExpiresAt.IsZero())
			_, err = r.BeginRefresh(b, now)
			require.ErrorIs(t, err, ErrInvalid)
			draining, err := r.Drain(b, now)
			require.NoError(t, err)
			require.Equal(t, UserAuthUncertain, draining.State)
			require.Equal(t, "credential", draining.CredentialID)
			require.Equal(t, "old-flow", draining.FlowID)
			require.Equal(t, r.Grant, draining.Grant)
			require.Empty(t, draining.Lease.Owner)
		})
	}
}

func TestDeletedCredentialRecoveryDrainPreservesUncertaintyAcrossRestart(t *testing.T) {
	now := time.Now().UTC()
	p := UserPrincipal{Owner: "owner", Project: "project", ChannelID: "channel", Epoch: 1, AppID: "app", TenantKey: "tenant", OpenID: "sender"}
	r := UserAuthRecord{Principal: p, VaultID: "vault", CredentialID: "credential", State: UserAuthUncertain, Operation: UserAuthAuthorize, Revision: 7, FlowID: "old-flow"}
	revision := r.Revision
	drained, err := r.Drain(UserAuthBegin{Principal: p, VaultID: "vault", Owner: "new-request", TTL: time.Minute, ExpectedRevision: &revision, DeletedCredentialID: "credential"}, now)
	require.NoError(t, err)
	raw, err := EncodeStored(drained)
	require.NoError(t, err)
	var reopened UserAuthRecord
	require.NoError(t, DecodeStored(raw, &reopened))
	_, err = reopened.Begin(UserAuthBegin{Principal: p, VaultID: "vault", Owner: "ordinary-message", TTL: time.Minute}, now.Add(time.Minute))
	require.ErrorIs(t, err, ErrUserAuthGate)
	require.Equal(t, UserAuthUncertain, reopened.State)
	require.Equal(t, "old-flow", reopened.FlowID)
}
