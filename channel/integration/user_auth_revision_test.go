// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

type revisionRaceAuthorizer struct {
	*userAuthFixture
	once             sync.Once
	arrived, release chan struct{}
	stale            bool
}

func (a *revisionRaceAuthorizer) Lookup(ctx context.Context, q core.UserGrantQuery) (core.UserGrant, error) {
	first := false
	a.once.Do(func() { first = true; close(a.arrived) })
	if first {
		select {
		case <-a.release:
		case <-ctx.Done():
			return core.UserGrant{}, ctx.Err()
		}
		if a.stale {
			return core.UserGrant{Status: core.AuthReauthRequired}, nil
		}
	}
	return a.userAuthFixture.Lookup(ctx, q)
}
func TestRuntimeUserLookupRevisionCannotResetNewReady(t *testing.T) {
	for _, stale := range []bool{true, false} {
		t.Run(map[bool]string{true: "old_reauth", false: "new_ready"}[stale], func(t *testing.T) {
			ctx := context.Background()
			s := newUserStore(t)
			b := userBinding()
			p := userPrincipalFixture(b)
			fixture := &userAuthFixture{ready: true, flow: "new-flow", version: "new-version"}
			begin := core.UserAuthBegin{Principal: p, VaultID: "user-vault", Owner: "other", TTL: time.Minute}
			held, e := s.BeginUserAuth(ctx, begin)
			require.NoError(t, e)
			old := fixture.grant("user-vault")
			old.FlowID = "old-flow"
			old.GrantVersion = "old-version"
			_, e = s.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: held.Lease, Revision: held.Revision, State: core.UserAuthReady, FlowID: old.FlowID, Grant: old})
			require.NoError(t, e)
			a := &revisionRaceAuthorizer{userAuthFixture: fixture, arrived: make(chan struct{}), release: make(chan struct{}), stale: stale}
			f := &conformance.MAServer{EnvironmentType: "cloud"}
			c := core.Connection{Binding: b, MAClient: &userVaultMA{MAClient: newMAForHarness(t, f)}, Provider: &runtimeProvider{}, UserAuthorizer: a}
			r, e := core.NewRuntime(s, []core.Connection{c}, testRuntimeOptions())
			require.NoError(t, e)
			mustEnqueue(t, r, b.ID, runtimeMessage())
			runUntilCleanup(t, r)
			<-a.arrived
			held, e = s.BeginUserAuth(ctx, begin)
			require.NoError(t, e)
			secret := &core.UserAuthSecret{Version: 1, KeyID: "key", Ciphertext: []byte("latest-sealed")}
			fresh, e := s.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: held.Lease, Revision: held.Revision, State: core.UserAuthReady, FlowID: "new-flow", Grant: fixture.grant("user-vault"), Secret: secret})
			require.NoError(t, e)
			close(a.release)
			f.AppendAfterInput(t, conformance.AnswerEvents("fresh")...)
			got, e := s.GetUserAuth(ctx, p)
			require.NoError(t, e)
			require.Equal(t, fresh, *got)
			a.mu.Lock()
			require.Zero(t, a.starts)
			a.mu.Unlock()
		})
	}
}
