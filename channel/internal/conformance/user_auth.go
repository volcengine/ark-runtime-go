// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package conformance

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// RunUserAuthStore 验证授权门闩、跨代远端意图及持久 fence。
func RunUserAuthStore(t *testing.T, factory StoreFactory) {
	runUserAuthInterruptBarrier(t, factory)
	t.Run("stable_credential_survives_failed_first_grant", func(t *testing.T) {
		ctx := context.Background()
		s := factory(t, StoreConfig{})
		defer func() { require.NoError(t, s.Close()) }()
		auth := s.(core.UserAuthStore)
		p := core.UserPrincipal{Owner: "owner", Project: "project", ChannelID: "channel", Epoch: 1, AppID: "app", TenantKey: "tenant", OpenID: "user"}
		begin := core.UserAuthBegin{Principal: p, VaultID: "vault", Owner: "worker", TTL: time.Minute}
		r, e := auth.BeginUserAuth(ctx, begin)
		require.NoError(t, e)
		r, e = auth.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: r.Lease, Revision: r.Revision, State: core.UserAuthVerifying, FlowID: "flow-1", CredentialID: "created-credential"})
		require.NoError(t, e)
		require.Equal(t, "created-credential", r.CredentialID)
		require.Empty(t, r.Grant.CredentialID)
		bad := core.UserAuthUpdate{Principal: p, Lease: r.Lease, Revision: r.Revision, State: core.UserAuthVerifying, CredentialID: "different"}
		_, e = auth.UpdateUserAuth(ctx, bad)
		require.ErrorIs(t, e, core.ErrConflict)
		bad.CredentialID = "created-credential"
		bad.Revision--
		_, e = auth.UpdateUserAuth(ctx, bad)
		require.ErrorIs(t, e, core.ErrConflict)
		r, e = auth.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: r.Lease, Revision: r.Revision, State: core.UserAuthInvalidSubject})
		require.NoError(t, e)
		require.Equal(t, "created-credential", r.CredentialID)
		r, e = auth.BeginUserAuth(ctx, begin)
		require.NoError(t, e)
		require.Equal(t, "created-credential", r.CredentialID)
		_, e = auth.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: r.Lease, Revision: r.Revision, State: core.UserAuthReady, Grant: core.UserGrant{Status: core.AuthReady, VaultID: "vault", CredentialID: "different", GrantVersion: "g"}})
		require.ErrorIs(t, e, core.ErrConflict)
		r, e = auth.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: r.Lease, Revision: r.Revision, State: core.UserAuthReady, Grant: core.UserGrant{Status: core.AuthReady, VaultID: "vault", CredentialID: "created-credential", GrantVersion: "g"}})
		require.NoError(t, e)
		require.Equal(t, "created-credential", r.Grant.CredentialID)
	})

	t.Run("gate_remote_intent_and_stale_fence", func(t *testing.T) {
		ctx := context.Background()
		clock := NewClock(time.Now().UTC())
		s := factory(t, StoreConfig{Clock: clock.Now})
		defer func() { require.NoError(t, s.Close()) }()
		auth, ok := s.(core.UserAuthStore)
		require.True(t, ok)
		p := core.UserPrincipal{Owner: "owner", Project: "project", ChannelID: "channel", Epoch: 1, AppID: "app", TenantKey: "tenant", OpenID: "user"}
		b := SampleBinding()
		b.Owner = p.Owner
		_, err := s.Ingest(ctx, core.NewBindingVersion(b), SampleMessage("auth"))
		require.NoError(t, err)
		l, err := s.LeaseTurn(ctx, Claim("worker"))
		require.NoError(t, err)
		require.NotNil(t, l)
		r, err := auth.BeginUserAuth(ctx, core.UserAuthBegin{Principal: p, VaultID: "vault", Owner: "auth-worker", TTL: time.Minute})
		require.NoError(t, err)
		u := core.TurnUpdate{Phase: core.PhaseSend, Session: core.TurnSession{Generation: 1, SessionID: "session"}, Submit: core.Submission{WriteAttempts: 1, SubmittedAt: clock.Now()}, RemoteIntent: &core.RemoteIntent{Kind: core.RemoteInput, UserAuthKey: p.Key(), GrantVersion: "1"}}
		_, err = s.UpdateTurn(ctx, l.Turn.Lease, u)
		require.ErrorIs(t, err, core.ErrUserAuthGate)
		bad := r.Lease
		bad.Fence++
		_, err = auth.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: bad, Revision: r.Revision, State: core.UserAuthReady, Grant: core.UserGrant{Status: core.UserAuthReady, VaultID: "vault", CredentialID: "credential", GrantVersion: "1"}})
		require.ErrorIs(t, err, core.ErrLeaseLost)
		r, err = auth.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: r.Lease, Revision: r.Revision, State: core.UserAuthReady, Secret: &core.UserAuthSecret{Version: 1, KeyID: "key", Ciphertext: []byte{1, 2, 3}}, Grant: core.UserGrant{Status: core.UserAuthReady, VaultID: "vault", CredentialID: "credential", GrantVersion: "1"}})
		require.NoError(t, err)
		_, err = s.UpdateTurn(ctx, l.Turn.Lease, u)
		require.NoError(t, err)
		stale := r.Revision - 1
		_, err = auth.BeginUserAuth(ctx, core.UserAuthBegin{Principal: p, VaultID: "vault", Owner: "stale-observer", TTL: time.Minute, ExpectedRevision: &stale})
		require.ErrorIs(t, err, core.ErrConflict)
		unchanged, err := auth.GetUserAuth(ctx, p)
		require.NoError(t, err)
		require.Equal(t, r, *unchanged, "stale lookup must not close the latest gate, even with an open remote operation")
		_, err = auth.BeginUserAuth(ctx, core.UserAuthBegin{Principal: p, VaultID: "vault", Owner: "reauth", TTL: time.Minute})
		require.ErrorIs(t, err, core.ErrRemoteBarrier)
		draining, err := auth.GetUserAuth(ctx, p)
		require.NoError(t, err)
		require.Equal(t, core.UserAuthDraining, draining.State, "reauth must close gate before draining old remote")
		require.Equal(t, r.Grant, draining.Grant)
		require.Equal(t, r.Secret, draining.Secret)
		require.Empty(t, draining.Lease.Owner)
		_, err = s.UpdateTurn(ctx, l.Turn.Lease, u)
		require.ErrorIs(t, err, core.ErrUserAuthGate, "new user writes must not starve draining")
		ops, err := s.ListRemoteOperations(ctx, core.RemoteOperationQuery{OpenOnly: true})
		require.NoError(t, err)
		require.Len(t, ops, 1)
		require.Equal(t, p.Key(), ops[0].UserAuthKey)
		u.RemoteIntent = nil
		u.RemoteKind = core.RemoteInput
		u.RemoteProof = &core.RemoteProof{Kind: core.RemoteNotAccepted, Reference: "not-sent"}
		_, err = s.UpdateTurn(ctx, l.Turn.Lease, u)
		require.NoError(t, err)
		next, err := auth.BeginUserAuth(ctx, core.UserAuthBegin{Principal: p, VaultID: "vault", Owner: "reauth", TTL: time.Minute})
		require.NoError(t, err)
		require.Greater(t, next.Lease.Fence, r.Lease.Fence)
		require.Equal(t, core.UserAuthPending, next.State)
		_, err = auth.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: r.Lease, Revision: r.Revision, State: core.UserAuthInvalidSubject})
		require.ErrorIs(t, err, core.ErrLeaseLost)
	})
}

// RunUserAuthWait 验证授权等待、密文提示、绝对时限和失败原子结算。
func RunUserAuthWait(t *testing.T, factory StoreFactory) {
	runUserAuthBatch(t, factory)
	ctx := context.Background()
	clock := NewClock(time.Now().UTC())
	s := factory(t, StoreConfig{Clock: clock.Now})
	defer func() { require.NoError(t, s.Close()) }()
	auth := s.(core.UserAuthStore)
	p := core.UserPrincipal{Owner: "owner", Project: "project", ChannelID: "channel", Epoch: 1, AppID: "app", TenantKey: "tenant", OpenID: "user"}
	b := SampleBinding()
	b.Owner = p.Owner
	b.Project = p.Project
	_, e := s.Ingest(ctx, core.NewBindingVersion(b), SampleMessage("wait"))
	require.NoError(t, e)
	l, e := s.LeaseTurn(ctx, Claim("worker"))
	require.NoError(t, e)
	require.NotNil(t, l)
	r, e := auth.BeginUserAuth(ctx, core.UserAuthBegin{Principal: p, VaultID: "vault", Owner: "authorizer", TTL: time.Hour})
	require.NoError(t, e)
	deadline := clock.Now().Add(time.Minute)
	r, e = auth.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: r.Lease, Revision: r.Revision, State: core.UserAuthPending, FlowID: "flow", ExpiresAt: deadline})
	require.NoError(t, e)
	w := core.UserAuthWait{Lease: r.Lease, Revision: r.Revision, Principal: p, FlowID: "flow", DeadlineAt: deadline, Until: clock.Now().Add(time.Second)}
	require.NoError(t, auth.WaitForUserAuth(ctx, l.Turn.Lease, w))
	saved, e := s.GetTurn(ctx, l.Turn.ID)
	require.NoError(t, e)
	require.Equal(t, deadline, saved.AuthWait.DeadlineAt)
	gate, e := auth.GetUserAuth(ctx, p)
	require.NoError(t, e)
	require.Empty(t, gate.Lease.Owner, "waiting must release the gate for another runtime before the lease timeout")
	require.Equal(t, r.Revision+1, gate.Revision)
	require.Equal(t, r.Secret, gate.Secret)
	require.Empty(t, saved.Lease.Owner)
	require.Equal(t, l.Turn.Schedule.DeadlineAt, saved.Schedule.DeadlineAt)
	clock.Advance(2 * time.Second)
	legacy, e := s.LeaseTurn(ctx, Claim("legacy"))
	require.NoError(t, e)
	require.Nil(t, legacy, "legacy workers must not take user auth work")
	c := Claim("user-worker")
	c.UserAuth = true
	l, e = s.LeaseTurn(ctx, c)
	require.NoError(t, e)
	require.NotNil(t, l)
	r, e = auth.BeginUserAuth(ctx, core.UserAuthBegin{Principal: p, VaultID: "vault", Owner: "second-runtime", TTL: time.Hour})
	require.NoError(t, e)
	require.Greater(t, r.Lease.Fence, gate.Lease.Fence)
	require.ErrorIs(t, auth.WaitForUserAuth(ctx, l.Turn.Lease, w), core.ErrLeaseLost, "a previous runtime must not release the newly claimed gate")
	w.Lease, w.Revision = r.Lease, r.Revision
	bad := w
	bad.DeadlineAt = deadline.Add(time.Minute)
	require.ErrorIs(t, auth.WaitForUserAuth(ctx, l.Turn.Lease, bad), core.ErrConflict)

	f := core.UserAuthFailure{Update: core.UserAuthUpdate{Principal: p, Lease: r.Lease, Revision: r.Revision, State: core.UserAuthInvalidSubject}, Outcome: core.TurnOutcome{Status: core.TurnFailed, ErrorKind: "invalid_subject"}}
	stale := l.Turn.Lease
	stale.Fence++
	require.ErrorIs(t, auth.FailUserAuth(ctx, stale, f), core.ErrLeaseLost)
	unchanged, e := auth.GetUserAuth(ctx, p)
	require.NoError(t, e)
	require.Equal(t, r.Revision, unchanged.Revision)
	require.NoError(t, auth.FailUserAuth(ctx, l.Turn.Lease, f))
	saved, e = s.GetTurn(ctx, l.Turn.ID)
	require.NoError(t, e)
	require.Equal(t, core.TurnFailed, saved.Status)
	r2, e := auth.GetUserAuth(ctx, p)
	require.NoError(t, e)
	require.Equal(t, core.UserAuthInvalidSubject, r2.State)
}

// RunUserAuthRace 验证重授与新 intent 只能有一个赢得同主体事务竞争。
func RunUserAuthRace(t *testing.T, factory StoreFactory) {
	ctx := context.Background()
	s := factory(t, StoreConfig{})
	defer func() { require.NoError(t, s.Close()) }()
	auth := s.(core.UserAuthStore)
	p := core.UserPrincipal{Owner: "race-owner", Project: "project", ChannelID: "channel", Epoch: 1, AppID: "app", TenantKey: "tenant", OpenID: "user"}
	b := SampleBinding()
	b.Owner = p.Owner
	_, e := s.Ingest(ctx, core.NewBindingVersion(b), SampleMessage("race"))
	require.NoError(t, e)
	l, e := s.LeaseTurn(ctx, Claim("worker"))
	require.NoError(t, e)
	r, e := auth.BeginUserAuth(ctx, core.UserAuthBegin{Principal: p, VaultID: "vault", Owner: "auth", TTL: time.Minute})
	require.NoError(t, e)
	_, e = auth.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: r.Lease, Revision: r.Revision, State: core.UserAuthReady, Grant: core.UserGrant{Status: core.UserAuthReady, VaultID: "vault", CredentialID: "cred", GrantVersion: "1"}})
	require.NoError(t, e)
	start := make(chan struct{})
	results := make(chan error, 2)
	go func() {
		<-start
		_, err := auth.BeginUserAuth(ctx, core.UserAuthBegin{Principal: p, VaultID: "vault", Owner: "reauth", TTL: time.Minute})
		results <- err
	}()
	go func() {
		<-start
		_, err := s.UpdateTurn(ctx, l.Turn.Lease, core.TurnUpdate{Phase: core.PhaseSend, Session: core.TurnSession{Generation: 1, SessionID: "session"}, Submit: core.Submission{WriteAttempts: 1, SubmittedAt: time.Now()}, RemoteIntent: &core.RemoteIntent{Kind: core.RemoteInput, UserAuthKey: p.Key(), GrantVersion: "1"}})
		results <- err
	}()
	close(start)
	a, z := <-results, <-results
	require.True(t, (a == nil) != (z == nil), "exactly one winner: %v / %v", a, z)
	if a != nil {
		require.True(t, errors.Is(a, core.ErrRemoteBarrier) || errors.Is(a, core.ErrUserAuthGate))
	}
	if z != nil {
		require.True(t, errors.Is(z, core.ErrRemoteBarrier) || errors.Is(z, core.ErrUserAuthGate))
	}
}

// RunUserAuthPayload 验证故障注入不落半状态，授权 metadata 完全不需要 KMS。
func RunUserAuthPayload(t *testing.T, c *UserAuthTestCodec, factory StoreFactory) {
	ctx := context.Background()
	clock := NewClock(time.Now().UTC())
	s := factory(t, StoreConfig{Clock: clock.Now})
	defer func() { require.NoError(t, s.Close()) }()
	auth := s.(core.UserAuthStore)
	p := core.UserPrincipal{Owner: "owner", Project: "project", ChannelID: "channel", Epoch: 1, AppID: "app", TenantKey: "tenant", OpenID: "user"}
	b := SampleBinding()
	b.Owner = p.Owner
	b.Project = p.Project
	_, e := s.Ingest(ctx, core.NewBindingVersion(b), SampleMessage("payload"))
	require.NoError(t, e)
	l, e := s.LeaseTurn(ctx, Claim("worker"))
	require.NoError(t, e)
	r, e := auth.BeginUserAuth(ctx, core.UserAuthBegin{Principal: p, VaultID: "vault", Owner: "auth", TTL: time.Hour})
	require.NoError(t, e)
	calls := c.Calls.Load()
	c.Fail.Store(true)
	r, e = auth.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: r.Lease, Revision: r.Revision, State: core.UserAuthPending, FlowID: "flow", Secret: &core.UserAuthSecret{Version: 1, KeyID: "external-key", Ciphertext: []byte{1, 2, 3}}})
	require.NoError(t, e)
	saved, e := auth.GetUserAuth(ctx, p)
	require.NoError(t, e)
	require.Equal(t, []byte{1, 2, 3}, saved.Secret.Ciphertext)
	require.Equal(t, calls, c.Calls.Load(), "metadata get/CAS must not call codec")
	prompt := core.NewDelivery(core.DeliverySource{Kind: core.SourceProgress, ID: l.Turn.ID}, 1, "user-auth-prompt", b.ID, core.ReplyTarget{ChatID: "chat", MessageID: "message", Mode: "reply"}, core.TextBody("https://example.test/authorize?secret=url-sentinel"))
	w := core.UserAuthWait{Lease: r.Lease, Revision: r.Revision, Principal: p, FlowID: "flow", DeadlineAt: clock.Now().Add(time.Minute), Until: clock.Now().Add(time.Second), Prompt: &prompt}
	require.ErrorContains(t, auth.WaitForUserAuth(ctx, l.Turn.Lease, w), "injected seal failure")
	c.Fail.Store(false)
	turn, e := s.GetTurn(ctx, l.Turn.ID)
	require.NoError(t, e)
	require.Nil(t, turn.AuthWait)
	require.Equal(t, l.Turn.Lease, turn.Lease)
	ds, e := s.ListDeliveries(ctx, core.DeliveryQuery{})
	require.NoError(t, e)
	require.Empty(t, ds)
	require.NoError(t, auth.WaitForUserAuth(ctx, l.Turn.Lease, w))
	d, e := s.LeaseDelivery(ctx, "bot-worker", time.Minute)
	require.NoError(t, e)
	require.NotNil(t, d, "pending user gate must not block bot prompt")
	require.Equal(t, prompt.Body.Text, d.Body.Text)
	clock.Advance(2 * time.Second)
	claim := Claim("user")
	claim.UserAuth = true
	l, e = s.LeaseTurn(ctx, claim)
	require.NoError(t, e)
	require.NotNil(t, l)
	reply := core.NewDelivery(core.DeliverySource{Kind: core.SourceTurn, ID: l.Turn.ID}, 0, "auth-failed", b.ID, prompt.ReplyTarget, core.TextBody("wrong subject"))

	r, e = auth.BeginUserAuth(ctx, core.UserAuthBegin{Principal: p, VaultID: "vault", Owner: "second-runtime", TTL: time.Hour})
	require.NoError(t, e)
	f := core.UserAuthFailure{Update: core.UserAuthUpdate{Principal: p, Lease: r.Lease, Revision: r.Revision, State: core.UserAuthInvalidSubject}, Outcome: core.TurnOutcome{Status: core.TurnFailed, ErrorKind: "invalid_subject"}, Deliveries: []core.Delivery{reply}}
	c.Fail.Store(true)
	require.ErrorContains(t, auth.FailUserAuth(ctx, l.Turn.Lease, f), "injected seal failure")
	saved, e = auth.GetUserAuth(ctx, p)
	require.NoError(t, e)
	require.Equal(t, core.UserAuthPending, saved.State)
	c.Fail.Store(false)
	require.NoError(t, auth.FailUserAuth(ctx, l.Turn.Lease, f))
	turn, e = s.GetTurn(ctx, l.Turn.ID)
	require.NoError(t, e)
	require.Equal(t, core.TurnFailed, turn.Status)
	ds, e = s.ListDeliveries(ctx, core.DeliveryQuery{})
	require.NoError(t, e)
	require.Len(t, ds, 2)
	// 已验证但迟到的 ready 与原 Turn timeout/回复必须一并提交，不能清掉成功证据。
	_, e = s.Ingest(ctx, core.NewBindingVersion(b), SampleMessage("late-ready"))
	require.NoError(t, e)
	l, e = s.LeaseTurn(ctx, claim)
	require.NoError(t, e)
	require.NotNil(t, l)
	r, e = auth.BeginUserAuth(ctx, core.UserAuthBegin{Principal: p, VaultID: "vault", Owner: "late-ready", TTL: time.Hour})
	require.NoError(t, e)
	secret := &core.UserAuthSecret{Version: 1, KeyID: "key", Ciphertext: []byte("verified-material")}
	r, e = auth.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: r.Lease, Revision: r.Revision, State: core.UserAuthVerifying, FlowID: "late-flow", Secret: secret})
	require.NoError(t, e)
	late := core.UserAuthFailure{Update: core.UserAuthUpdate{Principal: p, Lease: r.Lease, Revision: r.Revision, State: core.UserAuthReady, Grant: core.UserGrant{Status: core.AuthReady, VaultID: "vault", CredentialID: "late-credential", FlowID: "late-flow", GrantVersion: "late-g"}}, Outcome: core.TurnOutcome{Status: core.TurnFailed, ErrorKind: "user_auth_timeout"}, Deliveries: []core.Delivery{core.NewDelivery(core.DeliverySource{Kind: core.SourceTurn, ID: l.Turn.ID}, 0, "late-ready", b.ID, prompt.ReplyTarget, core.TextBody("authorization completed after this request expired"))}}
	invalid := late
	invalid.Outcome.ErrorKind = "unrelated_failure"
	require.ErrorIs(t, auth.FailUserAuth(ctx, l.Turn.Lease, invalid), core.ErrInvalid)
	c.Fail.Store(true)
	require.ErrorContains(t, auth.FailUserAuth(ctx, l.Turn.Lease, late), "injected seal failure")
	kept, e := auth.GetUserAuth(ctx, p)
	require.NoError(t, e)
	require.Equal(t, core.UserAuthVerifying, kept.State)
	require.Equal(t, r.Revision, kept.Revision)
	require.Equal(t, secret, kept.Secret)
	c.Fail.Store(false)
	require.NoError(t, auth.FailUserAuth(ctx, l.Turn.Lease, late))
	kept, e = auth.GetUserAuth(ctx, p)
	require.NoError(t, e)
	require.Equal(t, core.UserAuthReady, kept.State)
	require.Equal(t, "late-credential", kept.CredentialID)
	require.Equal(t, secret, kept.Secret)
	turn, e = s.GetTurn(ctx, l.Turn.ID)
	require.NoError(t, e)
	require.Equal(t, core.TurnFailed, turn.Status)
	require.Equal(t, "user_auth_timeout", turn.Outcome.ErrorKind)
}

// RunUserAuthIngest 验证 User 入站从第一笔持久化起隔离旧 worker。
func RunUserAuthIngest(t *testing.T, factory StoreFactory) {
	s := factory(t, StoreConfig{})
	defer func() { require.NoError(t, s.Close()) }()
	ctx := context.Background()
	b := SampleBinding()
	b.Identity = core.IdentityPolicy{P2P: core.IdentityUser, Group: core.IdentityBot}
	m := SampleMessage("initial-user")
	m.ChatType = "p2p"
	got, e := s.Ingest(ctx, core.NewBindingVersion(b), m)
	require.NoError(t, e)
	require.Equal(t, core.WorkKindUserAuth, got.Turn.WorkKind)
	l, e := s.LeaseTurn(ctx, Claim("legacy"))
	require.NoError(t, e)
	require.Nil(t, l)
	c := Claim("user")
	c.UserAuth = true
	l, e = s.LeaseTurn(ctx, c)
	require.NoError(t, e)
	require.NotNil(t, l)
}

// RunUserAuthRefresh 验证维护租约允许已有远端运行但不能改变授权主体或代次。
func RunUserAuthRefresh(t *testing.T, factory StoreFactory) {
	ctx := context.Background()
	clock := NewClock(time.Now().UTC())
	s := factory(t, StoreConfig{Clock: clock.Now})
	defer func() { require.NoError(t, s.Close()) }()
	auth := s.(core.UserAuthStore)
	refresher, ok := s.(interface {
		BeginUserAuthRefresh(context.Context, core.UserAuthBegin) (core.UserAuthRecord, error)
	})
	require.True(t, ok, "refresh must have a separate maintenance lease")
	p := core.UserPrincipal{Owner: "owner", Project: "project", ChannelID: "channel", Epoch: 1, AppID: "app", TenantKey: "tenant", OpenID: "user"}
	b := SampleBinding()
	b.Owner = p.Owner
	_, e := s.Ingest(ctx, core.NewBindingVersion(b), SampleMessage("refresh"))
	require.NoError(t, e)
	l, e := s.LeaseTurn(ctx, Claim("worker"))
	require.NoError(t, e)
	begin := core.UserAuthBegin{Principal: p, VaultID: "vault", Owner: "auth", TTL: time.Minute}
	r, e := auth.BeginUserAuth(ctx, begin)
	require.NoError(t, e)
	grant := core.UserGrant{Status: core.UserAuthReady, VaultID: "vault", CredentialID: "cred", GrantVersion: "1", FlowID: "flow", ExpiresAt: clock.Now().Add(time.Hour)}
	r, e = auth.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: r.Lease, Revision: r.Revision, State: core.UserAuthReady, FlowID: "flow", Grant: grant, Secret: &core.UserAuthSecret{Version: 1, KeyID: "key", Ciphertext: []byte{1, 2, 3}}})
	require.NoError(t, e)
	u := core.TurnUpdate{Phase: core.PhaseSend, Session: core.TurnSession{Generation: 1, SessionID: "session"}, Submit: core.Submission{WriteAttempts: 1, SubmittedAt: clock.Now()}, RemoteIntent: &core.RemoteIntent{Kind: core.RemoteInput, UserAuthKey: p.Key(), GrantVersion: "1"}}
	_, e = s.UpdateTurn(ctx, l.Turn.Lease, u)
	require.NoError(t, e)
	refreshed, e := refresher.BeginUserAuthRefresh(ctx, begin)
	require.NoError(t, e, "active remote may continue while refreshing same credential")
	require.Equal(t, r.Secret, refreshed.Secret)
	require.Equal(t, r.Grant, refreshed.Grant)
	require.Equal(t, r.FlowID, refreshed.FlowID)
	_, e = s.UpdateTurn(ctx, l.Turn.Lease, u)
	require.ErrorIs(t, e, core.ErrUserAuthGate)
	for _, field := range []string{"version", "vault", "credential", "flow"} {
		bad := grant
		switch field {
		case "version":
			bad.GrantVersion = "2"
		case "vault":
			bad.VaultID = "other"
		case "credential":
			bad.CredentialID = "other"
		case "flow":
			bad.FlowID = "other"
		}
		_, e = auth.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: refreshed.Lease, Revision: refreshed.Revision, State: core.UserAuthReady, Grant: bad})
		require.Error(t, e, field)
	}
	grant.ExpiresAt = clock.Now().Add(2 * time.Hour)
	r, e = auth.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: refreshed.Lease, Revision: refreshed.Revision, State: core.UserAuthReady, Grant: grant, Secret: &core.UserAuthSecret{Version: 1, KeyID: "key", Ciphertext: []byte{4, 5, 6}}})
	require.NoError(t, e)
	require.Equal(t, grant.ExpiresAt, r.Grant.ExpiresAt)
	refreshed, e = refresher.BeginUserAuthRefresh(ctx, begin)
	require.NoError(t, e)
	_, e = auth.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: refreshed.Lease, Revision: refreshed.Revision, State: "uncertain", Secret: &core.UserAuthSecret{Version: 1, KeyID: "key", Ciphertext: []byte{7, 8, 9}}})
	require.NoError(t, e)
	// 清掉远端屏障后仍由 uncertain 自身阻止重开，不能借 active operation 掩盖缺失判定。
	u.RemoteIntent = nil
	u.RemoteKind = core.RemoteInput
	u.RemoteProof = &core.RemoteProof{Kind: core.RemoteNotAccepted, Reference: "not-sent"}
	_, e = s.UpdateTurn(ctx, l.Turn.Lease, u)
	require.NoError(t, e)
	clock.Advance(2 * time.Minute)
	_, e = auth.BeginUserAuth(ctx, begin)
	require.ErrorIs(t, e, core.ErrUserAuthGate)
	_, e = refresher.BeginUserAuthRefresh(ctx, begin)
	require.ErrorIs(t, e, core.ErrUserAuthGate)
	saved, e := auth.GetUserAuth(ctx, p)
	require.NoError(t, e)
	require.Equal(t, "uncertain", saved.State)
	require.Equal(t, []byte{7, 8, 9}, saved.Secret.Ciphertext)
}

// RunUserAuthUncertainFailure 验证未知刷新结果与失败回复同事务提交并保留证据。
func RunUserAuthUncertainFailure(t *testing.T, factory StoreFactory) {
	ctx := context.Background()
	clock := NewClock(time.Now().UTC())
	s := factory(t, StoreConfig{Clock: clock.Now})
	defer func() { require.NoError(t, s.Close()) }()
	auth := s.(core.UserAuthStore)
	p := core.UserPrincipal{Owner: "owner", Project: "project", ChannelID: "channel", Epoch: 1, AppID: "app", TenantKey: "tenant", OpenID: "user"}
	b := SampleBinding()
	b.Owner = p.Owner
	b.Project = p.Project
	b.Identity = core.IdentityPolicy{P2P: core.IdentityUser}
	m := SampleMessage("uncertain")
	m.ChatType = "p2p"
	_, e := s.Ingest(ctx, core.NewBindingVersion(b), m)
	require.NoError(t, e)
	claim := Claim("worker")
	claim.UserAuth = true
	l, e := s.LeaseTurn(ctx, claim)
	require.NoError(t, e)
	begin := core.UserAuthBegin{Principal: p, VaultID: "vault", Owner: "auth", TTL: time.Minute}
	r, e := auth.BeginUserAuth(ctx, begin)
	require.NoError(t, e)
	reply := core.NewDelivery(core.DeliverySource{Kind: core.SourceTurn, ID: l.Turn.ID}, 0, "uncertain", b.ID, core.ReplyTarget{Mode: "reply", MessageID: "message", ChatID: "chat"}, core.TextBody("authorization outcome unknown"))
	f := core.UserAuthFailure{Update: core.UserAuthUpdate{Principal: p, Lease: r.Lease, Revision: r.Revision, State: core.UserAuthUncertain, Secret: &core.UserAuthSecret{Version: 1, KeyID: "key", Ciphertext: []byte{1, 2, 3}}}, Outcome: core.TurnOutcome{Status: core.TurnFailed, ErrorKind: "user_auth_uncertain"}, Deliveries: []core.Delivery{reply}}
	require.NoError(t, auth.FailUserAuth(ctx, l.Turn.Lease, f))
	saved, e := auth.GetUserAuth(ctx, p)
	require.NoError(t, e)
	require.Equal(t, core.UserAuthUncertain, saved.State)
	require.Equal(t, []byte{1, 2, 3}, saved.Secret.Ciphertext)
	turn, e := s.GetTurn(ctx, l.Turn.ID)
	require.NoError(t, e)
	require.Equal(t, core.TurnFailed, turn.Status)
	ds, e := s.ListDeliveries(ctx, core.DeliveryQuery{})
	require.NoError(t, e)
	require.Len(t, ds, 1)
	clock.Advance(2 * time.Minute)
	_, e = auth.BeginUserAuth(ctx, begin)
	require.ErrorIs(t, e, core.ErrUserAuthGate)
	_, e = auth.BeginUserAuthRefresh(ctx, begin)
	require.ErrorIs(t, e, core.ErrUserAuthGate)
	_, e = s.Ingest(ctx, core.NewBindingVersion(b), SampleMessage("new-after-uncertain"))
	require.NoError(t, e)
	next, e := s.LeaseTurn(ctx, claim)
	require.NoError(t, e)
	require.NotNil(t, next)
	observed := core.UserAuthFailure{Update: core.UserAuthUpdate{Principal: p, Revision: saved.Revision, State: core.UserAuthUncertain}, Outcome: core.TurnOutcome{Status: core.TurnFailed, ErrorKind: "user_auth_uncertain"}, Deliveries: []core.Delivery{core.NewDelivery(core.DeliverySource{Kind: core.SourceTurn, ID: next.Turn.ID}, 0, "uncertain", b.ID, reply.ReplyTarget, core.TextBody("still unknown"))}}
	bad := observed
	bad.Update.Revision++
	require.Error(t, auth.FailUserAuth(ctx, next.Turn.Lease, bad))
	unchanged, e := s.GetTurn(ctx, next.Turn.ID)
	require.NoError(t, e)
	require.False(t, unchanged.Status.Terminal())
	require.NoError(t, auth.FailUserAuth(ctx, next.Turn.Lease, observed))
	kept, e := auth.GetUserAuth(ctx, p)
	require.NoError(t, e)
	require.Equal(t, saved, kept)
}
