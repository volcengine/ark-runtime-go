// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

type deletedCredentialMA struct {
	*userVaultMA
	exists   bool
	checkErr error
}

func (m *deletedCredentialMA) GetVault(_ context.Context, id string) (core.VaultRef, error) {
	return core.VaultRef{ID: id, Principal: userPrincipalFixture(userBinding()).Key()}, nil
}
func (m *deletedCredentialMA) UserCredentialExists(_ context.Context, q core.UserGrantQuery) (bool, error) {
	if q.VaultID != "user-vault" || q.CredentialID != "deleted-credential" {
		return false, core.ErrConflict
	}
	return m.exists, m.checkErr
}

type deletedCredentialAuthorizer struct {
	*userAuthFixture
	reader *deletedCredentialMA
}

func (a *deletedCredentialAuthorizer) UserCredentialExists(ctx context.Context, q core.UserGrantQuery) (bool, error) {
	return a.reader.UserCredentialExists(ctx, q)
}

func (a *deletedCredentialAuthorizer) Start(ctx context.Context, p core.UserPrincipal, req core.AuthRequest) (core.AuthFlow, error) {
	if req.VaultID != "user-vault" || req.ExistingCredentialID != "" {
		return core.AuthFlow{}, core.ErrConflict
	}
	return a.userAuthFixture.Start(ctx, p, req)
}

func TestNewReauthorizesDeletedCredentialInExistingVaultAfterRestart(t *testing.T) {
	for _, kind := range []string{"sql", "embedded"} {
		for _, state := range []string{core.UserAuthReady, core.UserAuthUncertain} {
			t.Run(kind+"/"+state, func(t *testing.T) {
				s := userRuntimeStore(t, kind)
				auth := s.(core.UserAuthStore)
				b := userBinding()
				p := userPrincipalFixture(b)
				old := b
				old.VaultIDs = []string{"user-vault"}
				snapshot := seedUserSession(t, s, old, &core.UserCredentialSpec{Principal: p, VaultID: "user-vault", CredentialID: "deleted-credential", GrantVersion: "old", FlowID: "old-flow"})
				held, err := auth.BeginUserAuth(t.Context(), core.UserAuthBegin{Principal: p, VaultID: "user-vault", Owner: "seed", TTL: time.Minute})
				require.NoError(t, err)
				held, err = auth.UpdateUserAuth(t.Context(), core.UserAuthUpdate{Principal: p, Lease: held.Lease, Revision: held.Revision, State: core.UserAuthReady, Grant: core.UserGrant{Status: core.AuthReady, VaultID: "user-vault", CredentialID: "deleted-credential", GrantVersion: "old", FlowID: "old-flow", GrantedScopes: []string{"read"}}})
				require.NoError(t, err)
				if state == core.UserAuthUncertain {
					held, err = auth.BeginUserAuth(t.Context(), core.UserAuthBegin{Principal: p, VaultID: "user-vault", Owner: "unknown", TTL: time.Minute})
					require.NoError(t, err)
					_, err = auth.UpdateUserAuth(t.Context(), core.UserAuthUpdate{Principal: p, Lease: held.Lease, Revision: held.Revision, State: core.UserAuthUncertain})
					require.NoError(t, err)
				}
				f := &conformance.MAServer{EnvironmentType: "cloud", CreateSessionID: "sesn-replacement"}
				client := &deletedCredentialMA{userVaultMA: &userVaultMA{MAClient: newMAForHarness(t, f)}}
				a := &deletedCredentialAuthorizer{userAuthFixture: &userAuthFixture{flow: "replacement-flow", version: "replacement-grant"}, reader: client}
				connection := core.Connection{Binding: b, MAClient: client, Provider: &runtimeProvider{}, UserAuthorizer: a}
				options := restartRuntimeOptions()
				options.MessageDeadline = time.Minute
				first, err := core.NewRuntime(s, []core.Connection{connection}, options)
				require.NoError(t, err)
				msg := runtimeMessage()
				msg.Text = "/new"
				result := mustEnqueue(t, first, b.ID, msg)
				t.Cleanup(logUserAuthOnFailure(t, func() core.Store { return s }, result.Turn.ID, p))
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan error, 1)
				go func() { done <- first.Run(ctx) }()
				defer func() {
					cancel()
					if done != nil {
						require.NoError(t, <-done)
					}
				}()
				require.Eventually(t, func() bool {
					turn, e := s.GetTurn(t.Context(), result.Turn.ID)
					return e == nil && turn.AuthWait != nil
				}, 15*time.Second, 10*time.Millisecond)
				require.Eventually(t, func() bool {
					ds, e := s.ListDeliveries(t.Context(), core.DeliveryQuery{})
					for _, d := range ds {
						if e == nil && d.Source.Kind == core.SourceProgress && d.Status == core.DeliveryDone {
							return true
						}
					}
					return false
				}, 15*time.Second, 10*time.Millisecond)
				current, err := auth.GetUserAuth(t.Context(), p)
				require.NoError(t, err)
				require.Equal(t, "user-vault", current.VaultID)
				require.Empty(t, current.CredentialID)
				require.Empty(t, current.Grant)
				cancel()
				require.NoError(t, <-done)
				done = nil
				a.mu.Lock()
				a.ready = true
				a.mu.Unlock()
				restarted, err := core.NewRuntime(s, []core.Connection{connection}, options)
				require.NoError(t, err)
				runUntilCleanup(t, restarted)
				require.Eventually(t, func() bool {
					turn, e := s.GetTurn(t.Context(), result.Turn.ID)
					return e == nil && turn.Status == core.TurnDone
				}, 15*time.Second, 10*time.Millisecond)
				previous, err := s.GetSessionSnapshot(t.Context(), snapshot.ID)
				require.NoError(t, err)
				require.Equal(t, "deleted-credential", previous.Spec.UserCredential.CredentialID)
				conversation, err := s.GetConversation(t.Context(), result.Turn.ConversationID)
				require.NoError(t, err)
				next, err := s.GetSessionSnapshot(t.Context(), conversation.Session.SnapshotID)
				require.NoError(t, err)
				require.Equal(t, "user-vault", next.Spec.UserCredential.VaultID)
				require.Equal(t, "user-credential", next.Spec.UserCredential.CredentialID)
				client.mu.Lock()
				vaultCreates := client.creates
				client.mu.Unlock()
				require.Zero(t, vaultCreates)
				a.mu.Lock()
				starts, target := a.starts, a.request.ExistingCredentialID
				a.mu.Unlock()
				require.Equal(t, 1, starts)
				require.Empty(t, target)
				f.Lock()
				creates, sends := f.CreateCount, f.SendCount
				f.Unlock()
				require.Equal(t, 1, creates)
				require.Zero(t, sends)
			})
		}
	}
}

func TestNewDoesNotResetUncertainCredentialWithoutDeletionProof(t *testing.T) {
	for _, tc := range []struct {
		name   string
		exists bool
		err    error
		text   string
	}{
		{"exists", true, nil, "/new"}, {"network", false, errors.New("temporary failure"), "/new"}, {"ordinary", false, nil, "hello"},
		{"ordinary_exists", true, nil, "hello"},
		{"ordinary_network", false, errors.New("temporary failure"), "hello"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newUserStore(t)
			b := userBinding()
			p := userPrincipalFixture(b)
			held, err := s.BeginUserAuth(t.Context(), core.UserAuthBegin{Principal: p, VaultID: "user-vault", Owner: "seed", TTL: time.Minute})
			require.NoError(t, err)
			held, err = s.UpdateUserAuth(t.Context(), core.UserAuthUpdate{Principal: p, Lease: held.Lease, Revision: held.Revision, State: core.UserAuthReady, Grant: core.UserGrant{Status: core.AuthReady, VaultID: "user-vault", CredentialID: "deleted-credential", GrantVersion: "old"}})
			require.NoError(t, err)
			held, err = s.BeginUserAuth(t.Context(), core.UserAuthBegin{Principal: p, VaultID: "user-vault", Owner: "unknown", TTL: time.Minute})
			require.NoError(t, err)
			held, err = s.UpdateUserAuth(t.Context(), core.UserAuthUpdate{Principal: p, Lease: held.Lease, Revision: held.Revision, State: core.UserAuthUncertain})
			require.NoError(t, err)
			f := &conformance.MAServer{EnvironmentType: "cloud"}
			client := &deletedCredentialMA{userVaultMA: &userVaultMA{MAClient: newMAForHarness(t, f)}, exists: tc.exists, checkErr: tc.err}
			a := &deletedCredentialAuthorizer{userAuthFixture: &userAuthFixture{}, reader: client}
			options := testRuntimeOptions()
			options.MessageDeadline = time.Minute
			rt, err := core.NewRuntime(s, []core.Connection{{Binding: b, MAClient: client, Provider: &runtimeProvider{}, UserAuthorizer: a}}, options)
			require.NoError(t, err)
			msg := runtimeMessage()
			msg.Text = tc.text
			result := mustEnqueue(t, rt, b.ID, msg)
			runUntilCleanup(t, rt)
			require.Eventually(t, func() bool {
				turn, e := s.GetTurn(t.Context(), result.Turn.ID)
				if tc.err != nil && tc.text == "/new" {
					return e == nil && turn.WaitReason == "user_credential_unavailable"
				}
				return e == nil && turn.Status == core.TurnFailed
			}, 3*time.Second, 10*time.Millisecond)
			current, err := s.GetUserAuth(t.Context(), p)
			require.NoError(t, err)
			require.Equal(t, core.UserAuthUncertain, current.State)
			require.Equal(t, "deleted-credential", current.CredentialID)
			if tc.text == "hello" && (tc.exists || tc.err != nil) {
				deliveries, e := s.ListDeliveries(t.Context(), core.DeliveryQuery{})
				require.NoError(t, e)
				require.Len(t, deliveries, 1)
				require.NotContains(t, deliveries[0].Body.Text, "/new")
				require.Contains(t, deliveries[0].Body.Text, "无法确认授权结果")
			}
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

func TestDeletedCredentialRecoveryKeepsUnknownExecutionBarrier(t *testing.T) {
	for _, kind := range []string{"sql", "embedded"} {
		t.Run(kind, func(t *testing.T) {
			s := userRuntimeStore(t, kind)
			auth := s.(core.UserAuthStore)
			b := userBinding()
			p := userPrincipalFixture(b)
			ctx := t.Context()
			old := b
			old.VaultIDs = []string{"user-vault"}
			snapshot := seedUserSession(t, s, old, &core.UserCredentialSpec{Principal: p, VaultID: "user-vault", CredentialID: "deleted-credential", GrantVersion: "old", FlowID: "old-flow"})
			held, err := auth.BeginUserAuth(ctx, core.UserAuthBegin{Principal: p, VaultID: "user-vault", Owner: "seed", TTL: time.Minute})
			require.NoError(t, err)
			ready, err := auth.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: held.Lease, Revision: held.Revision, State: core.UserAuthReady, Grant: core.UserGrant{Status: core.AuthReady, VaultID: "user-vault", CredentialID: "deleted-credential", GrantVersion: "old", FlowID: "old-flow"}})
			require.NoError(t, err)
			msg := runtimeMessage()
			msg.MessageID = "unknown-write"
			msg.ProviderEventID = msg.MessageID
			_, err = s.Ingest(ctx, core.NewBindingVersion(b), msg)
			require.NoError(t, err)
			claim := conformance.Claim("lost-response")
			claim.UserAuth = true
			leased, err := s.LeaseTurn(ctx, claim)
			require.NoError(t, err)
			require.NotNil(t, leased)
			_, err = s.UpdateTurn(ctx, leased.Turn.Lease, core.TurnUpdate{Phase: core.PhaseSend, Session: core.TurnSession{SnapshotID: snapshot.ID, Generation: 1, SessionID: "sesn-demo", CreationKey: "seed-create"}, Submit: core.Submission{WriteAttempts: 1, SubmittedAt: time.Now()}, RemoteIntent: &core.RemoteIntent{Kind: core.RemoteInput, UserAuthKey: p.Key(), GrantVersion: "old"}})
			require.NoError(t, err)
			require.NoError(t, s.SettleTurn(ctx, leased.Turn.Lease, core.TurnOutcome{Status: core.TurnFailed, ErrorKind: "send_unconfirmed"}, nil))
			_, err = auth.BeginUserAuth(ctx, core.UserAuthBegin{Principal: p, VaultID: "user-vault", Owner: "recover", TTL: time.Minute, ExpectedRevision: &ready.Revision, DeletedCredentialID: "deleted-credential"})
			require.ErrorIs(t, err, core.ErrRemoteBarrier)
			after, err := auth.GetUserAuth(ctx, p)
			require.NoError(t, err)
			require.Equal(t, core.UserAuthDraining, after.State)
			require.Equal(t, "deleted-credential", after.CredentialID)
			require.Equal(t, ready.Grant, after.Grant)
			require.Empty(t, after.Lease.Owner)
			ops, err := s.ListRemoteOperations(ctx, core.RemoteOperationQuery{OpenOnly: true})
			require.NoError(t, err)
			require.Len(t, ops, 1)
		})
	}
}
