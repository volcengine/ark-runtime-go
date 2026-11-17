// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

type deletedVaultResolver struct{ identityConflictResolver }

func (r deletedVaultResolver) ResolveForDelivery(ctx context.Context, d core.Delivery) (core.Connection, error) {
	if d.Snapshot == nil || !d.Snapshot.Valid() || d.SessionSnapshotID == "" {
		return core.Connection{}, errors.New("frozen delivery snapshot required")
	}
	return r.identityConflictResolver.ResolveForDelivery(ctx, d)
}

func TestRuntimeDeletedUserVaultFailsWithReauthNoticeAfterRestart(t *testing.T) {
	for _, submitted := range []bool{false, true} {
		t.Run(fmt.Sprintf("submitted_%t", submitted), func(t *testing.T) {
			ctx := context.Background()
			s := newUserStore(t)
			b := userBinding()
			b.VaultIDs = []string{"user-vault"}
			p := userPrincipalFixture(b)
			a := &userAuthFixture{ready: true}
			snapshot := seedUserSession(t, s, b, &core.UserCredentialSpec{Principal: p, VaultID: "user-vault", CredentialID: "user-credential", GrantVersion: "grant-1", FlowID: "flow"})
			gate, err := s.BeginUserAuth(ctx, core.UserAuthBegin{Principal: p, VaultID: "user-vault", Owner: "seed", TTL: time.Minute})
			require.NoError(t, err)
			_, err = s.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: gate.Lease, Revision: gate.Revision, State: core.UserAuthReady, Grant: a.grant("user-vault")})
			require.NoError(t, err)
			f := &conformance.MAServer{}
			provider := &runtimeProvider{}
			connection := core.Connection{Binding: b, Provider: provider, MAClient: newMAForHarness(t, f), UserAuthorizer: a}
			options := restartRuntimeOptions()
			options.MessageDeadline = time.Minute
			options.DeliveryMaxAttempts = 100
			resolver := deletedVaultResolver{identityConflictResolver{fixtureResolver: fixtureResolver{connections: map[string]core.Connection{b.ID: connection}, deliveryError: true}, failure: fmt.Errorf("frozen vault: %w", core.ErrUserVaultDeleted)}}
			options.Resolver = resolver
			first, err := core.NewRuntime(s, nil, options)
			require.NoError(t, err)
			result := mustEnqueue(t, first, b.ID, runtimeMessage())
			if submitted {
				claim := conformance.Claim("lost-response")
				claim.UserAuth = true
				claim.MessageDeadline = time.Minute
				leased, err := s.LeaseTurn(ctx, claim)
				require.NoError(t, err)
				require.NotNil(t, leased)
				_, err = s.UpdateTurn(ctx, leased.Turn.Lease, core.TurnUpdate{Phase: core.PhaseSend, Session: core.TurnSession{SnapshotID: snapshot.ID, Generation: 1, SessionID: "sesn-demo", CreationKey: "seed-create"}, Submit: core.Submission{WriteAttempts: 1, SubmittedAt: time.Now()}, RemoteIntent: &core.RemoteIntent{Kind: core.RemoteInput, UserAuthKey: p.Key(), GrantVersion: "grant-1"}})
				require.NoError(t, err)
				require.NoError(t, s.ReleaseTurn(ctx, leased.Turn.Lease))
			}
			runCtx, cancel := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() { done <- first.Run(runCtx) }()
			defer func() {
				cancel()
				if done != nil {
					require.NoError(t, <-done)
				}
			}()
			require.Eventually(t, func() bool {
				turn, err := s.GetTurn(ctx, result.Turn.ID)
				return err == nil && turn.Status == core.TurnFailed
			}, time.Second, 10*time.Millisecond)
			cancel()
			require.NoError(t, <-done)
			done = nil
			turn, err := s.GetTurn(ctx, result.Turn.ID)
			require.NoError(t, err)
			require.Equal(t, "user_vault_deleted", turn.Outcome.ErrorKind)
			require.Empty(t, turn.WaitReason)
			if submitted {
				require.Equal(t, uint64(1), turn.Submit.WriteAttempts)
			} else {
				require.Zero(t, turn.Submit.WriteAttempts)
			}
			saved, err := s.GetUserAuth(ctx, p)
			require.NoError(t, err)
			require.NotEqual(t, core.UserAuthReady, saved.State)
			require.Equal(t, "user-vault", saved.VaultID)
			conversation, err := s.GetConversation(ctx, turn.ConversationID)
			require.NoError(t, err)
			require.Equal(t, snapshot.ID, conversation.Session.SnapshotID)
			require.Equal(t, "sesn-demo", conversation.Session.SessionID)
			if submitted {
				ops, err := s.ListRemoteOperations(ctx, core.RemoteOperationQuery{TurnID: turn.ID, OpenOnly: true})
				require.NoError(t, err)
				require.Len(t, ops, 1, "unknown input must retain its independent remote barrier")
			}
			deliveries, err := s.ListDeliveries(ctx, core.DeliveryQuery{})
			require.NoError(t, err)
			require.Len(t, deliveries, 1)
			require.NotNil(t, deliveries[0].Snapshot)
			require.True(t, deliveries[0].Snapshot.Valid())
			require.Equal(t, snapshot.ID, deliveries[0].SessionSnapshotID)
			require.Equal(t, core.DeliveryPending, deliveries[0].Status)
			require.Contains(t, deliveries[0].Body.Text, "重新授权")
			require.Contains(t, deliveries[0].Body.Text, "原会话已保留")
			require.NotContains(t, deliveries[0].Body.Text, "user-vault")
			require.NotContains(t, deliveries[0].Body.Text, "账号不符")
			resolver.deliveryError = false
			options.Resolver = resolver
			restarted, err := core.NewRuntime(s, nil, options)
			require.NoError(t, err)
			runUntilCleanup(t, restarted)
			require.Eventually(t, func() bool {
				ds, e := s.ListDeliveries(ctx, core.DeliveryQuery{})
				return e == nil && len(ds) == 1 && ds[0].Status == core.DeliveryDone
			}, time.Second, 10*time.Millisecond)
			require.Equal(t, 1, provider.count())
			require.Equal(t, deliveries[0].UUID, provider.delivered()[0].UUID)
			f.Lock()
			require.Zero(t, f.CreateCount)
			require.Zero(t, f.SendCount)
			f.Unlock()
		})
	}
}

func TestOldDeletedVaultDoesNotRevokeReplacementGrant(t *testing.T) {
	ctx := context.Background()
	s := newUserStore(t)
	b := userBinding()
	p := userPrincipalFixture(b)
	old := b
	old.VaultIDs = []string{"deleted-vault"}
	snapshot := seedUserSession(t, s, old, &core.UserCredentialSpec{Principal: p, VaultID: "deleted-vault", CredentialID: "old", GrantVersion: "old", FlowID: "old"})
	held, err := s.BeginUserAuth(ctx, core.UserAuthBegin{Principal: p, VaultID: "replacement-vault", Owner: "new-authorizer", TTL: time.Minute})
	require.NoError(t, err)
	ready, err := s.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: held.Lease, Revision: held.Revision, State: core.UserAuthReady, Grant: core.UserGrant{Status: core.AuthReady, VaultID: "replacement-vault", CredentialID: "new", GrantVersion: "new", FlowID: "new", GrantedScopes: []string{"read"}}})
	require.NoError(t, err)
	f := &conformance.MAServer{}
	a := &userAuthFixture{ready: true}
	connection := core.Connection{Binding: b, Provider: &runtimeProvider{}, MAClient: newMAForHarness(t, f), UserAuthorizer: a}
	options := testRuntimeOptions()
	options.MessageDeadline = time.Minute
	options.Resolver = identityConflictResolver{fixtureResolver: fixtureResolver{connections: map[string]core.Connection{b.ID: connection}}, failure: core.ErrUserVaultDeleted}
	r, err := core.NewRuntime(s, nil, options)
	require.NoError(t, err)
	result := mustEnqueue(t, r, b.ID, runtimeMessage())
	runUntilCleanup(t, r)
	require.Eventually(t, func() bool {
		turn, e := s.GetTurn(ctx, result.Turn.ID)
		return e == nil && turn.Status == core.TurnFailed
	}, time.Second, 10*time.Millisecond)
	after, err := s.GetUserAuth(ctx, p)
	require.NoError(t, err)
	require.Equal(t, core.UserAuthReady, after.State)
	require.Equal(t, ready.Revision, after.Revision)
	require.Equal(t, ready.Grant, after.Grant)
	previous, err := s.GetSessionSnapshot(ctx, snapshot.ID)
	require.NoError(t, err)
	require.Equal(t, "deleted-vault", previous.Spec.UserCredential.VaultID)
	ds, err := s.ListDeliveries(ctx, core.DeliveryQuery{})
	require.NoError(t, err)
	require.Len(t, ds, 1)
	require.Contains(t, ds[0].Body.Text, "/new")
	f.Lock()
	require.Zero(t, f.CreateCount)
	require.Zero(t, f.SendCount)
	f.Unlock()
}
