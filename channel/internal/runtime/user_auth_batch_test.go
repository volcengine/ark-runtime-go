// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUserAuthBatchLimit(t *testing.T) {
	now := time.Now()
	p := UserPrincipal{Owner: "o", Project: "p", ChannelID: "c", Epoch: 1, AppID: "a", TenantKey: "t", OpenID: "u"}
	r, e := (UserAuthRecord{}).Begin(UserAuthBegin{Principal: p, VaultID: "v", Owner: "worker", TTL: time.Minute}, now)
	require.NoError(t, e)
	r, e = r.Apply(UserAuthUpdate{Principal: p, Lease: r.Lease, Revision: r.Revision, State: UserAuthVerifying, BeginBatch: true, RequiredScopes: []string{"a", "b"}, BatchScopes: []string{"a"}}, now)
	require.NoError(t, e)
	require.Equal(t, uint64(1), r.Batch)
}

func TestUserAuthBatchAdvanceRequiresCumulativeGrant(t *testing.T) {
	now := time.Now()
	p := UserPrincipal{Owner: "o", Project: "p", ChannelID: "c", Epoch: 1, AppID: "a", TenantKey: "t", OpenID: "u"}
	r, e := (UserAuthRecord{}).Begin(UserAuthBegin{Principal: p, VaultID: "v", Owner: "w", TTL: time.Minute}, now)
	require.NoError(t, e)
	update := func(r UserAuthRecord, scopes []string, g UserGrant) (UserAuthRecord, error) {
		return r.Apply(UserAuthUpdate{Principal: p, Lease: r.Lease, Revision: r.Revision, State: UserAuthVerifying, BeginBatch: true, RequiredScopes: []string{"a", "b", "c"}, BatchScopes: scopes, BatchGrant: g}, now)
	}
	r, e = update(r, []string{"a"}, UserGrant{})
	require.NoError(t, e)
	r, e = r.Apply(UserAuthUpdate{Principal: p, Lease: r.Lease, Revision: r.Revision, State: UserAuthPending, FlowID: "f1"}, now)
	require.NoError(t, e)
	grant := UserGrant{Status: AuthReady, VaultID: "v", CredentialID: "credential", GrantVersion: "1", FlowID: "f1", GrantedScopes: []string{"a"}}
	r, e = update(r, []string{"b"}, grant)
	require.NoError(t, e)
	require.Equal(t, uint64(2), r.Batch)
	require.Equal(t, UserAuthVerifying, r.State)
	require.Empty(t, r.FlowID)
	require.Equal(t, "credential", r.CredentialID)
	r, e = r.Apply(UserAuthUpdate{Principal: p, Lease: r.Lease, Revision: r.Revision, State: UserAuthPending, FlowID: "f2"}, now)
	require.NoError(t, e)
	grant.FlowID = "f2"
	grant.GrantedScopes = []string{"b"}
	_, e = update(r, []string{"c"}, grant)
	require.ErrorIs(t, e, ErrConflict)
	grant.GrantedScopes = []string{"a", "b"}
	r, e = update(r, []string{"c"}, grant)
	require.NoError(t, e)
	require.Equal(t, uint64(3), r.Batch)
	require.ErrorIs(t, r.CheckIntent("o", RemoteIntent{UserAuthKey: p.Key(), GrantVersion: "1"}), ErrUserAuthGate)
}

func TestUnknownUserAuthWorkIsOpaque(t *testing.T) {
	for _, kind := range []string{"future_auth_v99", "unknown_auth_protocol"} {
		raw := []byte(`{"_work_kind":"` + kind + `","state":"ready","future_field":{"keep":true}}`)
		var record UserAuthRecord
		require.NoError(t, DecodeStored(raw, &record))
		require.True(t, record.IsOpaqueStored())
		_, e := record.Apply(UserAuthUpdate{State: UserAuthReady}, time.Now())
		require.ErrorIs(t, e, ErrUnsupportedWork)
	}
}
