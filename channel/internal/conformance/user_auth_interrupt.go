// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package conformance

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func runUserAuthInterruptBarrier(t *testing.T, factory StoreFactory) {
	t.Run("interrupt_proof_drains_user_gate", func(t *testing.T) {
		ctx := context.Background()
		s := factory(t, StoreConfig{})
		defer func() { require.NoError(t, s.Close()) }()
		auth := s.(core.UserAuthStore)
		p := core.UserPrincipal{Owner: "owner", Project: "project", ChannelID: "channel", Epoch: 1, AppID: "app", TenantKey: "tenant", OpenID: "user"}
		b := SampleBinding()
		b.Owner = p.Owner
		b.Identity = core.IdentityPolicy{P2P: core.IdentityUser}
		m := SampleMessage("interrupt-user")
		m.ChatType = "p2p"
		_, err := s.Ingest(ctx, core.NewBindingVersion(b), m)
		require.NoError(t, err)
		claim := Claim("user")
		claim.UserAuth = true
		turn, err := s.LeaseTurn(ctx, claim)
		require.NoError(t, err)
		require.NotNil(t, turn)
		begin := core.UserAuthBegin{Principal: p, VaultID: "vault", Owner: "auth", TTL: time.Minute}
		r, err := auth.BeginUserAuth(ctx, begin)
		require.NoError(t, err)
		_, err = auth.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: r.Lease, Revision: r.Revision, State: core.UserAuthReady, Grant: core.UserGrant{Status: core.AuthReady, VaultID: "vault", CredentialID: "credential", GrantVersion: "g"}})
		require.NoError(t, err)
		update := core.TurnUpdate{Phase: core.PhaseSend, Session: core.TurnSession{Generation: 1, SessionID: "session"}, Submit: core.Submission{WriteAttempts: 1, SubmittedAt: time.Now()}, RemoteIntent: &core.RemoteIntent{Kind: core.RemoteInput, UserAuthKey: p.Key(), GrantVersion: "g", Limit: 1}}
		_, err = s.UpdateTurn(ctx, turn.Turn.Lease, update)
		require.NoError(t, err)
		update.RemoteIntent, update.RemoteKind, update.RemoteCleanup = nil, core.RemoteInput, true
		_, err = s.UpdateTurn(ctx, turn.Turn.Lease, update)
		require.NoError(t, err)
		require.NoError(t, s.SettleTurn(ctx, turn.Turn.Lease, core.TurnOutcome{Status: core.TurnStopped}, nil))
		_, err = auth.BeginUserAuth(ctx, begin)
		require.ErrorIs(t, err, core.ErrRemoteBarrier)
		legacy, err := s.LeaseRemoteOperation(ctx, core.RemoteClaim{Owner: "legacy", TTL: time.Minute})
		require.NoError(t, err)
		require.Nil(t, legacy)
		operation, err := s.LeaseRemoteOperation(ctx, core.RemoteClaim{Owner: "user", TTL: time.Minute, UserAuth: true})
		require.NoError(t, err)
		require.NotNil(t, operation)
		for _, proof := range []core.RemoteProof{{Kind: core.RemoteTerminal, Reference: operation.InputEventID}, {Kind: core.RemoteInterrupted, Reference: "http-200"}} {
			_, err = s.UpdateRemoteOperation(ctx, operation.Lease, core.RemoteUpdate{Proof: &proof})
			require.ErrorIs(t, err, core.ErrRemoteBarrier)
			_, err = auth.BeginUserAuth(ctx, begin)
			require.ErrorIs(t, err, core.ErrRemoteBarrier)
		}
		resolved, err := s.UpdateRemoteOperation(ctx, operation.Lease, core.RemoteUpdate{Proof: &core.RemoteProof{Kind: core.RemoteInterrupted, Reference: operation.InterruptEventID()}})
		require.NoError(t, err)
		require.True(t, resolved.Resolved)
		require.False(t, resolved.Counted)
		_, err = auth.BeginUserAuth(ctx, begin)
		require.NoError(t, err)
	})
}
