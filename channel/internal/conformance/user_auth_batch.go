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

func runUserAuthBatch(t *testing.T, factory StoreFactory) {
	t.Run("batch_advance_keeps_gate_closed_and_fenced", func(t *testing.T) {
		ctx := context.Background()
		s := factory(t, StoreConfig{})
		defer func() { require.NoError(t, s.Close()) }()
		auth := s.(core.UserAuthStore)
		p := core.UserPrincipal{Owner: "owner", Project: "project", ChannelID: "channel", Epoch: 1, AppID: "app", TenantKey: "tenant", OpenID: "user"}
		r, e := auth.BeginUserAuth(ctx, core.UserAuthBegin{Principal: p, VaultID: "vault", Owner: "worker", TTL: time.Minute})
		require.NoError(t, e)
		b := SampleBinding()
		b.Owner = p.Owner
		b.Project = p.Project
		_, e = s.Ingest(ctx, core.NewBindingVersion(b), SampleMessage("batch-wait"))
		require.NoError(t, e)
		turn, e := s.LeaseTurn(ctx, Claim("worker"))
		require.NoError(t, e)
		require.NotNil(t, turn)
		required := []string{"a", "b"}
		r, e = auth.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: r.Lease, Revision: r.Revision, State: core.UserAuthVerifying, BeginBatch: true, RequiredScopes: required, BatchScopes: []string{"a"}, ExpiresAt: time.Now().Add(time.Minute)})
		require.NoError(t, e)
		r, e = auth.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: r.Lease, Revision: r.Revision, State: core.UserAuthPending, FlowID: "one"})
		require.NoError(t, e)
		deadline := r.ExpiresAt
		prompt := core.NewDelivery(core.DeliverySource{Kind: core.SourceProgress, ID: turn.Turn.ID}, 1, "batch-1", b.ID, core.ReplyTargetFor(SampleMessage("batch-wait"), b), core.TextBody("first"))
		w := core.UserAuthWait{Batch: 1, Attempt: r.Attempt, Lease: r.Lease, Revision: r.Revision, Principal: p, FlowID: "one", DeadlineAt: deadline, Until: time.Now().Add(-time.Second), Prompt: &prompt, RegisterFlow: true}
		require.NoError(t, auth.WaitForUserAuth(ctx, turn.Turn.Lease, w))
		claim := Claim("worker-2")
		claim.UserAuth = true
		turn, e = s.LeaseTurn(ctx, claim)
		require.NoError(t, e)
		require.NotNil(t, turn)
		r, e = auth.BeginUserAuth(ctx, core.UserAuthBegin{Principal: p, VaultID: "vault", Owner: "worker-2", TTL: time.Minute})
		require.NoError(t, e)
		g := core.UserGrant{Status: core.AuthReady, VaultID: "vault", CredentialID: "credential", GrantVersion: "one", FlowID: "one", GrantedScopes: []string{"a"}}
		_, e = auth.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: r.Lease, Revision: r.Revision, State: core.UserAuthReady, Grant: g})
		require.ErrorIs(t, e, core.ErrConflict)
		advance := core.UserAuthUpdate{Principal: p, Lease: r.Lease, Revision: r.Revision, State: core.UserAuthVerifying, BeginBatch: true, RequiredScopes: required, BatchScopes: []string{"b"}, BatchGrant: g}
		next, e := auth.UpdateUserAuth(ctx, advance)
		require.NoError(t, e)
		_, e = auth.UpdateUserAuth(ctx, advance)
		require.ErrorIs(t, e, core.ErrConflict)
		saved, e := auth.GetUserAuth(ctx, p)
		require.NoError(t, e)
		require.Equal(t, next, *saved)
		prompt2 := core.NewDelivery(core.DeliverySource{Kind: core.SourceProgress, ID: turn.Turn.ID}, 1, "batch-2", b.ID, core.ReplyTargetFor(SampleMessage("batch-wait"), b), core.TextBody("second"))
		require.NotEqual(t, prompt.ID, prompt2.ID)
		w.Batch = 2
		w.Lease = next.Lease
		w.Revision = next.Revision
		w.FlowID = "two"
		w.Prompt = &prompt2
		bad := w
		bad.DeadlineAt = deadline.Add(time.Second)
		require.ErrorIs(t, auth.WaitForUserAuth(ctx, turn.Turn.Lease, bad), core.ErrConflict)
		require.NoError(t, auth.WaitForUserAuth(ctx, turn.Turn.Lease, w))
		after, e := s.GetTurn(ctx, turn.Turn.ID)
		require.NoError(t, e)
		require.Equal(t, "two", after.AuthWait.FlowID)
		require.Equal(t, uint64(2), after.AuthWait.Batch)
		require.Equal(t, deadline, after.AuthWait.DeadlineAt)
		afterGate, e := auth.GetUserAuth(ctx, p)
		require.NoError(t, e)
		require.Empty(t, afterGate.Lease.Owner)
		require.Equal(t, "two", afterGate.FlowID)
		require.Equal(t, uint64(2), saved.Batch)
		require.Empty(t, saved.FlowID)
		require.Equal(t, "credential", saved.CredentialID)
		require.Equal(t, core.UserAuthVerifying, saved.State)
		require.ErrorIs(t, saved.CheckIntent(p.Owner, core.RemoteIntent{UserAuthKey: p.Key(), GrantVersion: "one"}), core.ErrUserAuthGate)
	})
}
