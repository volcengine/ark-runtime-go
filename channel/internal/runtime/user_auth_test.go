// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUserAuthOptionalSnapshotAndUnknownFields(t *testing.T) {
	type oldSpec struct {
		Binding             Binding
		CreationKey         string
		LookupKey           string
		Extras              SessionExtras
		EnvironmentOverride *EnvironmentOverride
		EnvironmentType     string
		ChatType            string
	}
	old := oldSpec{Binding: Binding{ID: "binding"}, CreationKey: "key", LookupKey: "lookup"}
	current := CreateSessionSpec{Binding: old.Binding, CreationKey: old.CreationKey, LookupKey: old.LookupKey}
	require.Equal(t, digestJSON(old), digestJSON(current))
	raw := []byte(`{"_work_kind":"user_auth_v2","principal":{"namespace":"test","future":"kept"},"future_root":{"x":1},"state":"pending"}`)
	var r UserAuthRecord
	require.NoError(t, DecodeStored(raw, &r))
	require.False(t, r.IsOpaqueStored())
	r.State = UserAuthVerifying
	round, e := EncodeStored(r)
	require.NoError(t, e)
	require.Contains(t, string(round), `"future":"kept"`)
	require.Contains(t, string(round), `"future_root":{"x":1}`)
	var future UserAuthRecord
	require.NoError(t, DecodeStored([]byte(`{"_work_kind":"user_auth_v999","state":99}`), &future))
	require.True(t, future.IsOpaqueStored())
	round, e = EncodeStored(future)
	require.NoError(t, e)
	require.JSONEq(t, `{"_work_kind":"user_auth_v999","state":99}`, string(round))
	b := Binding{UserAuthScopes: []string{"one"}}
	c := b.clone()
	b.UserAuthScopes[0] = "mutated"
	require.Equal(t, "one", c.UserAuthScopes[0])
}
func TestUserAuthUserIntentCannotBypassGate(t *testing.T) {
	turn := Turn{WorkKind: WorkKindUserAuth, Phase: PhaseSend}
	_, e := turn.Apply(TurnUpdate{Phase: PhaseSend, RemoteIntent: &RemoteIntent{Kind: RemoteInput}})
	require.ErrorIs(t, e, ErrUserAuthGate)
}
func TestUserAuthCiphertextCloneAndDeadline(t *testing.T) {
	p := UserPrincipal{Owner: "o", Project: "p", ChannelID: "c", Epoch: 1, AppID: "a", TenantKey: "t", OpenID: "u"}
	now := time.Now().UTC()
	r, e := (UserAuthRecord{}).Begin(UserAuthBegin{Principal: p, VaultID: "v", Owner: "w", TTL: time.Minute}, now)
	require.NoError(t, e)
	secret := &UserAuthSecret{Version: 1, KeyID: "key", Ciphertext: []byte{1, 2, 3}}
	r, e = r.Apply(UserAuthUpdate{Principal: p, Lease: r.Lease, Revision: r.Revision, State: UserAuthPending, Secret: secret, FlowID: "f", ExpiresAt: now.Add(time.Minute)}, now)
	require.NoError(t, e)
	secret.Ciphertext[0] = 9
	require.Equal(t, byte(1), r.Secret.Ciphertext[0])
	_, e = r.Apply(UserAuthUpdate{Principal: p, Lease: r.Lease, Revision: r.Revision, State: UserAuthPending, ExpiresAt: now.Add(time.Hour)}, now)
	require.ErrorIs(t, e, ErrConflict)
	b, _ := json.Marshal(r)
	require.NotContains(t, string(b), "URL")
}

func TestUserAuthSnapshotFreezesCredential(t *testing.T) {
	credential := &UserCredentialSpec{VaultID: "vault", CredentialID: "credential", GrantVersion: "1"}
	spec := CreateSessionSpec{Binding: Binding{ID: "binding", UserAuthScopes: []string{"read"}}, CreationKey: "create", LookupKey: "lookup", UserCredential: credential}
	snap := NewSessionSnapshot("route", 1, spec)
	require.True(t, snap.Valid())
	credential.GrantVersion = "mutated"
	spec.Binding.UserAuthScopes[0] = "mutated"
	require.Equal(t, "1", snap.Spec.UserCredential.GrantVersion)
	require.Equal(t, "read", snap.Spec.Binding.UserAuthScopes[0])
	require.True(t, snap.Valid())
}

func TestUserAuthReadyKeepsEncryptedCredential(t *testing.T) {
	p := UserPrincipal{Owner: "o", Project: "p", ChannelID: "c", Epoch: 1, AppID: "a", TenantKey: "t", OpenID: "u"}
	now := time.Now().UTC()
	r, e := (UserAuthRecord{}).Begin(UserAuthBegin{Principal: p, VaultID: "vault", Owner: "w", TTL: time.Minute}, now)
	require.NoError(t, e)
	ready, e := r.Apply(UserAuthUpdate{Principal: p, Lease: r.Lease, Revision: r.Revision, State: UserAuthReady, Grant: UserGrant{Status: UserAuthReady, VaultID: "vault", CredentialID: "credential", GrantVersion: "1"}, Secret: &UserAuthSecret{Version: 1, KeyID: "key", Ciphertext: []byte("opaque-token-envelope")}}, now)
	require.NoError(t, e)
	require.NotNil(t, ready.Secret, "ready self-running grants must retain encrypted access/refresh envelope")
	require.Empty(t, ready.Lease.Owner)
}

func TestUserAuthObservedUncertainCanOnlySettleUnchangedEvidence(t *testing.T) {
	p := UserPrincipal{Owner: "o", Project: "p", ChannelID: "c", Epoch: 1, AppID: "a", TenantKey: "t", OpenID: "u"}
	now := time.Now().UTC()
	r, e := (UserAuthRecord{}).Begin(UserAuthBegin{Principal: p, VaultID: "v", Owner: "w", TTL: time.Minute}, now)
	require.NoError(t, e)
	r, e = r.Apply(UserAuthUpdate{Principal: p, Lease: r.Lease, Revision: r.Revision, State: UserAuthUncertain, FlowID: "flow", Secret: &UserAuthSecret{Version: 1, KeyID: "key", Ciphertext: []byte("evidence")}}, now)
	require.NoError(t, e)
	u := UserAuthUpdate{Principal: p, Revision: r.Revision, State: UserAuthUncertain}
	after, e := r.Apply(u, now)
	require.NoError(t, e)
	require.Equal(t, r, after)
	for _, mutation := range []func(*UserAuthUpdate){func(u *UserAuthUpdate) { u.CredentialID = "tampered-target" }, func(u *UserAuthUpdate) { u.Revision++ }, func(u *UserAuthUpdate) {
		u.Secret = &UserAuthSecret{Version: 1, KeyID: "key", Ciphertext: []byte("tampered")}
	}, func(u *UserAuthUpdate) { u.FlowID = "other" }, func(u *UserAuthUpdate) { u.TTL = time.Hour }, func(u *UserAuthUpdate) { u.ExpiresAt = now.Add(time.Hour) }, func(u *UserAuthUpdate) { u.Grant.Status = AuthReady }} {
		bad := u
		mutation(&bad)
		_, e = r.Apply(bad, now)
		require.Error(t, e)
	}
	live := r
	live.Lease.Owner = "other-worker"
	live.Lease.ExpiresAt = now.Add(time.Hour)
	_, e = live.Apply(u, now)
	require.Error(t, e)
}

func TestUserAuthExpiredRefreshMayOnlyTightenToUncertain(t *testing.T) {
	p := UserPrincipal{Owner: "o", Project: "p", ChannelID: "c", Epoch: 1, AppID: "a", TenantKey: "t", OpenID: "u"}
	now := time.Now().UTC()
	begin := UserAuthBegin{Principal: p, VaultID: "vault", Owner: "maintenance", TTL: time.Minute}
	r, e := (UserAuthRecord{}).Begin(begin, now)
	require.NoError(t, e)
	r, e = r.Apply(UserAuthUpdate{Principal: p, Lease: r.Lease, Revision: r.Revision, State: UserAuthReady, Grant: UserGrant{Status: AuthReady, VaultID: "vault", CredentialID: "credential", GrantVersion: "g", FlowID: "flow"}, Secret: &UserAuthSecret{Version: 1, KeyID: "key", Ciphertext: []byte("unknown-refresh")}}, now)
	require.NoError(t, e)
	r, e = r.BeginRefresh(begin, now)
	require.NoError(t, e)
	u := UserAuthUpdate{Principal: p, Lease: r.Lease, Revision: r.Revision, State: UserAuthUncertain}
	_, e = r.Apply(u, now)
	require.NoError(t, e, "owner can close its live refresh")
	tightened, e := r.Apply(u, now.Add(2*time.Minute))
	require.NoError(t, e)
	require.Equal(t, UserAuthUncertain, tightened.State)
	require.Equal(t, r.Secret, tightened.Secret)
	require.Equal(t, r.Grant, tightened.Grant)
	require.Equal(t, r.FlowID, tightened.FlowID)
	require.Empty(t, tightened.Lease.Owner)
	require.Equal(t, r.Revision+1, tightened.Revision)
	for _, mutate := range []func(*UserAuthUpdate){func(u *UserAuthUpdate) { u.CredentialID = "tampered-target" }, func(u *UserAuthUpdate) { u.Lease.Fence++ }, func(u *UserAuthUpdate) { u.Revision++ }, func(u *UserAuthUpdate) {
		u.Secret = &UserAuthSecret{Version: 1, KeyID: "key", Ciphertext: []byte("replacement")}
	}, func(u *UserAuthUpdate) { u.State = UserAuthReady; u.Grant = r.Grant }} {
		bad := u
		mutate(&bad)
		_, e = r.Apply(bad, now.Add(2*time.Minute))
		require.Error(t, e)
	}
}
