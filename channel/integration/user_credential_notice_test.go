// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

type missingCredentialAuthorizer struct {
	*deletedCredentialAuthorizer
	lookupErr    error
	probe        func()
	currentGrant *core.UserGrant
}

func (a *missingCredentialAuthorizer) Lookup(context.Context, core.UserGrantQuery) (core.UserGrant, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.currentGrant != nil {
		return *a.currentGrant, nil
	}
	return core.UserGrant{Status: core.AuthUnknown}, a.lookupErr
}

func (a *missingCredentialAuthorizer) UserCredentialExists(ctx context.Context, q core.UserGrantQuery) (bool, error) {
	if a.probe != nil {
		a.probe()
	}
	return a.deletedCredentialAuthorizer.UserCredentialExists(ctx, q)
}

func TestDeletedCredentialOrdinaryMessageSuggestsNewAfterRestart(t *testing.T) {
	for _, kind := range []string{"sql", "embedded"} {
		for _, scenario := range []string{"ready_unknown", "ready_error", "uncertain"} {
			t.Run(kind+"/"+scenario, func(t *testing.T) {
				s := userRuntimeStore(t, kind)
				auth := s.(core.UserAuthStore)
				b := userBinding()
				p := userPrincipalFixture(b)
				old := b
				old.VaultIDs = []string{"user-vault"}
				snapshot := seedUserSession(t, s, old, &core.UserCredentialSpec{Principal: p, VaultID: "user-vault", CredentialID: "deleted-credential", GrantVersion: "old", FlowID: "old-flow"})
				held, err := auth.BeginUserAuth(t.Context(), core.UserAuthBegin{Principal: p, VaultID: "user-vault", Owner: "seed", TTL: time.Minute})
				require.NoError(t, err)
				held, err = auth.UpdateUserAuth(t.Context(), core.UserAuthUpdate{Principal: p, Lease: held.Lease, Revision: held.Revision, State: core.UserAuthReady, Grant: core.UserGrant{Status: core.AuthReady, VaultID: "user-vault", CredentialID: "deleted-credential", FlowID: "old-flow", GrantVersion: "old", GrantedScopes: []string{"read"}}})
				require.NoError(t, err)
				if scenario == "uncertain" {
					held, err = auth.BeginUserAuth(t.Context(), core.UserAuthBegin{Principal: p, VaultID: "user-vault", Owner: "unknown", TTL: time.Minute})
					require.NoError(t, err)
					held, err = auth.UpdateUserAuth(t.Context(), core.UserAuthUpdate{Principal: p, Lease: held.Lease, Revision: held.Revision, State: core.UserAuthUncertain})
					require.NoError(t, err)
				}
				before, err := auth.GetUserAuth(t.Context(), p)
				require.NoError(t, err)
				f := &conformance.MAServer{EnvironmentType: "cloud"}
				client := &deletedCredentialMA{userVaultMA: &userVaultMA{MAClient: newMAForHarness(t, f)}}
				a := &missingCredentialAuthorizer{deletedCredentialAuthorizer: &deletedCredentialAuthorizer{userAuthFixture: &userAuthFixture{}, reader: client}}
				if scenario == "ready_error" {
					a.lookupErr = errors.New("old flow unavailable")
				}
				provider := &runtimeProvider{err: runtimeDeliveryError{kind: "temporary", retryable: true}}
				connection := core.Connection{Binding: b, MAClient: client, Provider: provider, UserAuthorizer: a}
				options := deadlineRuntimeOptions()
				options.MessageDeadline = time.Minute
				options.DeliveryMaxAttempts = 100
				rt, err := core.NewRuntime(s, []core.Connection{connection}, options)
				require.NoError(t, err)
				msg := runtimeMessage()
				msg.Text = "hello"
				result := mustEnqueue(t, rt, b.ID, msg)
				t.Cleanup(logUserAuthOnFailure(t, func() core.Store { return s }, result.Turn.ID, p))
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan error, 1)
				go func() { done <- rt.Run(ctx) }()
				defer func() {
					cancel()
					if done != nil {
						require.NoError(t, <-done)
					}
				}()
				require.Eventually(t, func() bool {
					turn, e := s.GetTurn(t.Context(), result.Turn.ID)
					return e == nil && turn.Status == core.TurnFailed
				}, 3*time.Second, 10*time.Millisecond)
				ds, err := s.ListDeliveries(t.Context(), core.DeliveryQuery{})
				require.NoError(t, err)
				require.Len(t, ds, 1)
				require.Contains(t, ds[0].Body.Text, "/new")
				require.Contains(t, ds[0].Body.Text, "原会话已保留")
				require.NotContains(t, ds[0].Body.Text, "联系管理员")
				require.NotContains(t, ds[0].Body.Text, "deleted-credential")
				require.NotContains(t, ds[0].Body.Text, "user-vault")
				cancel()
				require.NoError(t, <-done)
				done = nil
				current, err := auth.GetUserAuth(t.Context(), p)
				require.NoError(t, err)
				require.Equal(t, before, current)
				oldSnapshot, err := s.GetSessionSnapshot(t.Context(), snapshot.ID)
				require.NoError(t, err)
				require.NotNil(t, oldSnapshot)
				require.Equal(t, snapshot.ID, oldSnapshot.ID)
				require.Equal(t, snapshot.Spec.UserCredential, oldSnapshot.Spec.UserCredential)
				require.True(t, oldSnapshot.Valid())
				provider.mu.Lock()
				provider.err = nil
				provider.mu.Unlock()
				restarted, err := core.NewRuntime(s, []core.Connection{connection}, options)
				require.NoError(t, err)
				runUntilCleanup(t, restarted)
				require.Eventually(t, func() bool {
					after, e := s.ListDeliveries(t.Context(), core.DeliveryQuery{})
					return e == nil && len(after) == 1 && after[0].Status == core.DeliveryDone && after[0].UUID == ds[0].UUID
				}, 3*time.Second, 10*time.Millisecond)
				a.mu.Lock()
				starts := a.starts
				a.mu.Unlock()
				require.Zero(t, starts)
				f.Lock()
				creates, sends := f.CreateCount, f.SendCount
				f.Unlock()
				require.Zero(t, creates)
				require.Zero(t, sends)
			})
		}
	}
}

func TestDeletedCredentialNoticeRechecksGrantChangedDuringProbe(t *testing.T) {
	s := newUserStore(t)
	b := userBinding()
	p := userPrincipalFixture(b)
	held, err := s.BeginUserAuth(t.Context(), core.UserAuthBegin{Principal: p, VaultID: "user-vault", Owner: "seed", TTL: time.Minute})
	require.NoError(t, err)
	held, err = s.UpdateUserAuth(t.Context(), core.UserAuthUpdate{Principal: p, Lease: held.Lease, Revision: held.Revision, State: core.UserAuthReady, Grant: core.UserGrant{Status: core.AuthReady, VaultID: "user-vault", CredentialID: "deleted-credential", GrantVersion: "old", FlowID: "old-flow", GrantedScopes: []string{"read"}}})
	require.NoError(t, err)
	held, err = s.BeginUserAuth(t.Context(), core.UserAuthBegin{Principal: p, VaultID: "user-vault", Owner: "unknown", TTL: time.Minute})
	require.NoError(t, err)
	held, err = s.UpdateUserAuth(t.Context(), core.UserAuthUpdate{Principal: p, Lease: held.Lease, Revision: held.Revision, State: core.UserAuthUncertain})
	require.NoError(t, err)
	f := &conformance.MAServer{EnvironmentType: "cloud"}
	client := &deletedCredentialMA{userVaultMA: &userVaultMA{MAClient: newMAForHarness(t, f)}}
	a := &missingCredentialAuthorizer{deletedCredentialAuthorizer: &deletedCredentialAuthorizer{userAuthFixture: &userAuthFixture{}, reader: client}}
	var once sync.Once
	a.probe = func() {
		once.Do(func() {
			current, e := s.GetUserAuth(t.Context(), p)
			require.NoError(t, e)
			next, e := s.BeginUserAuth(t.Context(), core.UserAuthBegin{Principal: p, VaultID: current.VaultID, Owner: "parallel-new", TTL: time.Minute, ExpectedRevision: &current.Revision, DeletedCredentialID: "deleted-credential"})
			require.NoError(t, e)
			grant := core.UserGrant{Status: core.AuthReady, VaultID: "user-vault", CredentialID: "new-credential", GrantVersion: "new-version", FlowID: "new-flow", GrantedScopes: []string{"read"}}
			_, e = s.UpdateUserAuth(t.Context(), core.UserAuthUpdate{Principal: p, Lease: next.Lease, Revision: next.Revision, State: core.UserAuthReady, Grant: grant})
			require.NoError(t, e)
			a.mu.Lock()
			a.currentGrant = &grant
			a.mu.Unlock()
		})
	}
	options := testRuntimeOptions()
	options.MessageDeadline = time.Minute
	rt, err := core.NewRuntime(s, []core.Connection{{Binding: b, MAClient: client, Provider: &runtimeProvider{}, UserAuthorizer: a}}, options)
	require.NoError(t, err)
	result := mustEnqueue(t, rt, b.ID, runtimeMessage())
	runUntilCleanup(t, rt)
	t.Cleanup(logUserAuthOnFailure(t, func() core.Store { return s }, result.Turn.ID, p))
	require.Eventually(t, func() bool { f.Lock(); defer f.Unlock(); return f.SendCount == 1 }, 3*time.Second, 10*time.Millisecond)
	f.AppendAfterInput(t, conformance.AnswerEvents("recovered")...)
	require.Eventually(t, func() bool {
		turn, e := s.GetTurn(t.Context(), result.Turn.ID)
		return e == nil && turn.Status == core.TurnDone
	}, 3*time.Second, 10*time.Millisecond)
	conversation, err := s.GetConversation(t.Context(), result.Turn.ConversationID)
	require.NoError(t, err)
	snapshot, err := s.GetSessionSnapshot(t.Context(), conversation.Session.SnapshotID)
	require.NoError(t, err)
	require.Equal(t, "new-credential", snapshot.Spec.UserCredential.CredentialID)
}
