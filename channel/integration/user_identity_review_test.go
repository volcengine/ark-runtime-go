// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	memory "github.com/volcengine/ark-runtime-go/channel/internal/testutil/memorystore"
)

type deadlineHTTPAuthorizer struct {
	*userAuthFixture
	url, mode string
}

func (a *deadlineHTTPAuthorizer) read(ctx context.Context, out any) error {
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, a.url, nil)
	if e != nil {
		return e
	}
	resp, e := http.DefaultClient.Do(req)
	if e != nil {
		return e
	}
	defer func() { _ = resp.Body.Close() }()
	return json.NewDecoder(resp.Body).Decode(out)
}
func (a *deadlineHTTPAuthorizer) Status(ctx context.Context, ref core.AuthFlowRef) (core.AuthStatus, error) {
	if a.mode != "status" {
		return a.userAuthFixture.Status(ctx, ref)
	}
	var status core.AuthStatus
	e := a.read(ctx, &status)
	return status, e
}
func (a *deadlineHTTPAuthorizer) Lookup(ctx context.Context, q core.UserGrantQuery) (core.UserGrant, error) {
	if a.mode != "lookup" {
		return a.userAuthFixture.Lookup(ctx, q)
	}
	var grant core.UserGrant
	e := a.read(ctx, &grant)
	return grant, e
}

func TestRuntimeUserHTTPReadyCannotCrossOriginalDeadline(t *testing.T) {
	for _, tc := range []struct {
		name, mode   string
		wait         bool
		turnDeadline bool
	}{{"status_auth_wait", "status", true, false}, {"status_flow", "status", false, false}, {"lookup_auth_wait", "lookup", true, false}, {"lookup_turn", "lookup", false, true}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			clock := conformance.NewClock(time.Now().UTC())
			s, e := memory.New(memory.Options{Clock: clock.Now, PayloadCodec: &conformance.UserAuthTestCodec{}, PayloadNamespace: "test", RequirePayloadEncryption: true})
			require.NoError(t, e)
			defer func() { require.NoError(t, s.Close()) }()
			b := userBinding()
			p := userPrincipalFixture(b)
			result, e := s.Ingest(ctx, core.NewBindingVersion(b), runtimeMessage())
			require.NoError(t, e)
			claim := conformance.Claim("setup")
			claim.UserAuth = true
			claim.MessageDeadline = time.Hour
			if tc.turnDeadline {
				claim.MessageDeadline = time.Second
			}
			leased, e := s.LeaseTurn(ctx, claim)
			require.NoError(t, e)
			turn, e := s.UpdateTurn(ctx, leased.Turn.Lease, core.TurnUpdate{Phase: core.PhaseCreate, Session: core.TurnSession{Generation: 1, CreationKey: "late-create"}})
			require.NoError(t, e)
			begin := core.UserAuthBegin{Principal: p, VaultID: "user-vault", Owner: "setup", TTL: time.Minute}
			gate, e := s.BeginUserAuth(ctx, begin)
			require.NoError(t, e)
			deadline := clock.Now().Add(time.Second)
			if tc.turnDeadline {
				deadline = clock.Now().Add(time.Hour)
			}
			secret := &core.UserAuthSecret{Version: 1, KeyID: "key", Ciphertext: []byte("verified-credential-evidence")}
			gate, e = s.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: gate.Lease, Revision: gate.Revision, State: core.UserAuthPending, FlowID: "flow", ExpiresAt: deadline, Secret: secret})
			require.NoError(t, e)
			if tc.wait {
				require.NoError(t, s.WaitForUserAuth(ctx, turn.Lease, core.UserAuthWait{Principal: p, Lease: gate.Lease, Revision: gate.Revision, Attempt: gate.Attempt, FlowID: "flow", DeadlineAt: deadline, Until: clock.Now()}))
			} else {
				_, e = s.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: gate.Lease, Revision: gate.Revision, State: core.UserAuthPending, Release: true})
				require.NoError(t, e)
				require.NoError(t, s.ReleaseTurn(ctx, turn.Lease))
			}
			fixture := &userAuthFixture{ready: true}
			grant := fixture.grant("user-vault")
			if tc.mode == "lookup" {
				gate, e = s.BeginUserAuth(ctx, begin)
				require.NoError(t, e)
				_, e = s.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: gate.Lease, Revision: gate.Revision, State: core.UserAuthReady, Grant: grant})
				require.NoError(t, e)
			}
			entered, release := make(chan struct{}, 1), make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				entered <- struct{}{}
				select {
				case <-release:
				case <-req.Context().Done():
					return
				}
				if tc.mode == "status" {
					_ = json.NewEncoder(w).Encode(core.AuthStatus{Status: core.AuthReady, Grant: grant})
				} else {
					_ = json.NewEncoder(w).Encode(grant)
				}
			}))
			defer server.Close()
			authorizer := &deadlineHTTPAuthorizer{userAuthFixture: fixture, url: server.URL, mode: tc.mode}
			f := &conformance.MAServer{}
			options := testRuntimeOptions()
			options.Clock = clock.Now
			options.LeaseTTL = time.Minute
			options.MessageDeadline = time.Hour
			r, e := core.NewRuntime(s, []core.Connection{{Binding: b, Provider: &runtimeProvider{}, MAClient: newMAForHarness(t, f), UserAuthorizer: authorizer}}, options)
			require.NoError(t, e)
			runCtx, cancel := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() { done <- r.Run(runCtx) }()
			defer func() { cancel(); require.NoError(t, <-done) }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("authorizer HTTP did not start")
			}
			clock.Advance(2 * time.Second)
			close(release)
			require.Eventually(t, func() bool {
				turn, e := s.GetTurn(ctx, result.Turn.ID)
				return e == nil && turn.Status == core.TurnFailed && turn.Outcome.ErrorKind == "user_auth_timeout"
			}, time.Second, 10*time.Millisecond)
			f.Lock()
			require.Zero(t, f.CreateCount)
			require.Zero(t, f.SendCount)
			f.Unlock()
			saved, e := s.GetUserAuth(ctx, p)
			require.NoError(t, e)
			require.Equal(t, core.UserAuthReady, saved.State)
			require.Equal(t, secret, saved.Secret)
			ds, e := s.ListDeliveries(ctx, core.DeliveryQuery{})
			require.NoError(t, e)
			require.Len(t, ds, 1)
			require.Contains(t, ds[0].Body.Text, "未继续执行")
		})
	}
}

type checkpointCredentialAuthorizer struct {
	*userAuthFixture
	auth core.UserAuthStore
}

func (a *checkpointCredentialAuthorizer) Lookup(ctx context.Context, q core.UserGrantQuery) (core.UserGrant, error) {
	record, e := a.auth.GetUserAuth(ctx, q.Principal)
	if e != nil {
		return core.UserGrant{}, e
	}
	if record != nil && record.State == core.UserAuthInvalidSubject {
		return core.UserGrant{Status: core.AuthSubjectMismatch}, nil
	}
	return a.userAuthFixture.Lookup(ctx, q)
}

func (a *checkpointCredentialAuthorizer) Status(ctx context.Context, ref core.AuthFlowRef) (core.AuthStatus, error) {
	_, e := a.auth.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: ref.Principal, Lease: ref.Lease, Revision: ref.Revision, State: core.UserAuthVerifying, FlowID: ref.FlowID, CredentialID: "user-credential"})
	if e != nil {
		return core.AuthStatus{}, e
	}
	return a.userAuthFixture.Status(ctx, ref)
}
func TestRuntimeUserWrongFirstSubjectRetriesSameCredential(t *testing.T) {
	ctx := context.Background()
	s := newUserStore(t)
	b := userBinding()
	p := userPrincipalFixture(b)
	f := &conformance.MAServer{EnvironmentType: "cloud"}
	base := &userAuthFixture{status: core.AuthSubjectMismatch, lookupReauth: true, store: s}
	authorizer := &checkpointCredentialAuthorizer{userAuthFixture: base, auth: s}
	options := testRuntimeOptions()
	options.MessageDeadline = time.Minute
	r, e := core.NewRuntime(s, []core.Connection{{Binding: b, Provider: &runtimeProvider{}, MAClient: &userVaultMA{MAClient: newMAForHarness(t, f)}, UserAuthorizer: authorizer}}, options)
	require.NoError(t, e)
	first := mustEnqueue(t, r, b.ID, runtimeMessage())
	runUntilCleanup(t, r)
	require.Eventually(t, func() bool {
		turn, e := s.GetTurn(ctx, first.Turn.ID)
		return e == nil && turn.Status == core.TurnFailed
	}, 4*time.Second, 10*time.Millisecond)
	failed, e := s.GetUserAuth(ctx, p)
	require.NoError(t, e)
	require.Equal(t, core.UserAuthInvalidSubject, failed.State)
	require.Equal(t, "user-credential", failed.CredentialID)
	require.Empty(t, failed.Grant.CredentialID)
	base.mu.Lock()
	base.status = ""
	base.ready = true
	base.flow = "flow-2"
	base.mu.Unlock()
	m := runtimeMessage()
	m.MessageID = "retry-after-subject"
	m.ProviderEventID = "retry-after-subject"
	second := mustEnqueue(t, r, b.ID, m)
	require.Eventually(t, func() bool { base.mu.Lock(); defer base.mu.Unlock(); return base.starts == 2 }, time.Second, 10*time.Millisecond)
	base.mu.Lock()
	require.Equal(t, "user-credential", base.request.ExistingCredentialID)
	base.mu.Unlock()
	f.AppendAfterInput(t, conformance.AnswerEvents("same credential")...)
	require.Eventually(t, func() bool {
		turn, e := s.GetTurn(ctx, second.Turn.ID)
		return e == nil && turn.Status == core.TurnDone
	}, time.Second, 10*time.Millisecond)
	ready, e := s.GetUserAuth(ctx, p)
	require.NoError(t, e)
	require.Equal(t, "user-credential", ready.CredentialID)
	require.Equal(t, ready.CredentialID, ready.Grant.CredentialID)
	f.Lock()
	require.Equal(t, 1, f.CreateCount)
	require.Equal(t, 1, f.SendCount)
	f.Unlock()
}

type identityConflictResolver struct {
	fixtureResolver
	failure error
}

func (r identityConflictResolver) ResolveForTurn(context.Context, core.Turn, *core.SessionSnapshot) (core.Connection, error) {
	return core.Connection{}, r.failure
}
func TestRuntimeUserResolverIdentityConflictClosesGateAndReplies(t *testing.T) {
	ctx := context.Background()
	s := newUserStore(t)
	b := userBinding()
	p := userPrincipalFixture(b)
	b.VaultIDs = []string{"user-vault"}
	snapshot := seedUserSession(t, s, b, &core.UserCredentialSpec{Principal: p, VaultID: "user-vault", CredentialID: "user-credential", GrantVersion: "grant-1", FlowID: "flow"})
	gate, e := s.BeginUserAuth(ctx, core.UserAuthBegin{Principal: p, VaultID: "user-vault", Owner: "seed", TTL: time.Minute})
	require.NoError(t, e)
	a := &userAuthFixture{ready: true}
	_, e = s.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: gate.Lease, Revision: gate.Revision, State: core.UserAuthReady, Grant: a.grant("user-vault")})
	require.NoError(t, e)
	f := &conformance.MAServer{}
	connection := core.Connection{Binding: b, Provider: &runtimeProvider{}, MAClient: newMAForHarness(t, f), UserAuthorizer: a}
	options := testRuntimeOptions()
	options.MessageDeadline = time.Minute
	options.Resolver = identityConflictResolver{fixtureResolver: fixtureResolver{connections: map[string]core.Connection{b.ID: connection}}, failure: core.ErrUserIdentityConflict}
	r, e := core.NewRuntime(s, nil, options)
	require.NoError(t, e)
	result := mustEnqueue(t, r, b.ID, runtimeMessage())
	runUntilCleanup(t, r)
	require.Eventually(t, func() bool {
		turn, e := s.GetTurn(ctx, result.Turn.ID)
		return e == nil && turn.Status == core.TurnFailed
	}, time.Second, 10*time.Millisecond)
	blocked, e := s.GetUserAuth(ctx, p)
	require.NoError(t, e)
	require.Equal(t, core.UserAuthInvalidSubject, blocked.State)
	ds, e := s.ListDeliveries(ctx, core.DeliveryQuery{})
	require.NoError(t, e)
	require.Len(t, ds, 1)
	require.Contains(t, ds[0].Body.Text, "未继续执行")
	require.Contains(t, ds[0].Body.Text, "发消息")
	require.Contains(t, ds[0].Body.Text, "原会话已保留")
	c, e := s.GetConversation(ctx, result.Turn.ConversationID)
	require.NoError(t, e)
	require.Equal(t, snapshot.ID, c.Session.SnapshotID)
	f.Lock()
	require.Zero(t, f.CreateCount)
	require.Zero(t, f.SendCount)
	f.Unlock()
}

func TestRuntimeUserResolverNetworkErrorDoesNotInvalidateIdentity(t *testing.T) {
	ctx := context.Background()
	s := newUserStore(t)
	b := userBinding()
	p := userPrincipalFixture(b)
	b.VaultIDs = []string{"user-vault"}
	seedUserSession(t, s, b, &core.UserCredentialSpec{Principal: p, VaultID: "user-vault", CredentialID: "user-credential", GrantVersion: "grant-1", FlowID: "flow"})
	gate, e := s.BeginUserAuth(ctx, core.UserAuthBegin{Principal: p, VaultID: "user-vault", Owner: "seed", TTL: time.Minute})
	require.NoError(t, e)
	a := &userAuthFixture{ready: true}
	_, e = s.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: gate.Lease, Revision: gate.Revision, State: core.UserAuthReady, Grant: a.grant("user-vault")})
	require.NoError(t, e)
	f := &conformance.MAServer{}
	connection := core.Connection{Binding: b, Provider: &runtimeProvider{}, MAClient: newMAForHarness(t, f), UserAuthorizer: a}
	options := testRuntimeOptions()
	options.MessageDeadline = time.Minute
	options.Resolver = identityConflictResolver{fixtureResolver: fixtureResolver{connections: map[string]core.Connection{b.ID: connection}}, failure: errors.New("temporary database or network failure")}
	r, e := core.NewRuntime(s, nil, options)
	require.NoError(t, e)
	result := mustEnqueue(t, r, b.ID, runtimeMessage())
	runUntilCleanup(t, r)
	require.Eventually(t, func() bool {
		turn, e := s.GetTurn(ctx, result.Turn.ID)
		return e == nil && turn.WaitReason == "connection_unavailable"
	}, time.Second, 10*time.Millisecond)
	saved, e := s.GetUserAuth(ctx, p)
	require.NoError(t, e)
	require.Equal(t, core.UserAuthReady, saved.State)
	ds, e := s.ListDeliveries(ctx, core.DeliveryQuery{})
	require.NoError(t, e)
	require.Empty(t, ds)
	f.Lock()
	require.Zero(t, f.CreateCount)
	require.Zero(t, f.SendCount)
	f.Unlock()
}
