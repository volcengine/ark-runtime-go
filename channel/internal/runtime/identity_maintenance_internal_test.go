// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIdentityMaintenanceCacheBoundsAndPinsActivePrincipals(t *testing.T) {
	options, err := (RuntimeOptions{}).Normalize()
	require.NoError(t, err)
	r := &Runtime{options: options}
	active := r.botState("in-use-old-epoch")
	for i := 0; i < 2*botIdentityCacheLimit; i++ {
		state := r.botState(fmt.Sprintf("finished-epoch-%d", i))
		require.NotNil(t, state)
		r.releaseBotState(state)
		require.LessOrEqual(t, len(r.identities), botIdentityCacheLimit)
	}
	require.Same(t, active, r.identities["in-use-old-epoch"], "in-flight refresh state cannot be evicted")
	r.releaseBotState(active)
	for _, state := range r.identities {
		require.Zero(t, state.refs)
	}
}

func TestIdentityMaintenanceCacheRejectsOverflowWhileAllPrincipalsAreInUse(t *testing.T) {
	options, err := (RuntimeOptions{}).Normalize()
	require.NoError(t, err)
	r := &Runtime{options: options}
	for i := 0; i < botIdentityCacheLimit; i++ {
		require.NotNil(t, r.botState(fmt.Sprintf("active-%d", i)))
	}
	require.Nil(t, r.botState("overflow"))
	require.Len(t, r.identities, botIdentityCacheLimit)
}

func TestIdentityMaintenanceCacheKeyFollowsPrincipalAndCredential(t *testing.T) {
	b := Binding{Owner: "owner", Project: "project", ChannelID: "channel", AppID: "app", BindingEpoch: 1, BotCredential: BotCredentialSpec{VaultID: "vault", SecretName: "token", AllowedHosts: []string{"open.feishu.cn"}}}
	first := botIdentityKey(b)
	b.ID = "different-policy-key"
	b.ConfigVersion = 2
	b.AgentVersion = "new-agent"
	require.Equal(t, first, botIdentityKey(b), "policy and Agent changes do not create a second renewal writer")
	b.BindingEpoch++
	require.NotEqual(t, first, botIdentityKey(b))
	b.BindingEpoch--
	b.BotCredential.VaultID = "different-tenant-vault"
	require.NotEqual(t, first, botIdentityKey(b))
	b.BotCredential.VaultID = "vault"
	b.Owner = "other-account"
	require.NotEqual(t, first, botIdentityKey(b))
}

func TestIdentityMaintenanceCacheUsesFrozenClockForLRU(t *testing.T) {
	now := time.Now()
	options, err := (RuntimeOptions{Clock: func() time.Time { return now }}).Normalize()
	require.NoError(t, err)
	r := &Runtime{options: options}
	for i := 0; i < botIdentityCacheLimit; i++ {
		state := r.botState(fmt.Sprintf("epoch-%d", i))
		r.releaseBotState(state)
		now = now.Add(time.Second)
	}
	require.NotNil(t, r.botState("new"))
	require.NotContains(t, r.identities, "epoch-0")
}

type identityBlockingProvider struct {
	Provider
	entered chan struct{}
}

func (p identityBlockingProvider) BotToken(ctx context.Context) (string, time.Time, error) {
	close(p.entered)
	<-ctx.Done()
	return "", time.Time{}, ctx.Err()
}

type identityNoopVault struct{ MAClient }

func (identityNoopVault) UpsertEnvCredential(context.Context, BotCredentialSpec, string) (string, error) {
	return "credential", nil
}
func TestIdentityMaintenanceCancelledWaiterReleasesWithoutWaitingForOtherRefresh(t *testing.T) {
	options, err := (RuntimeOptions{}).Normalize()
	require.NoError(t, err)
	r := &Runtime{options: options}
	b := Binding{ID: "binding", AppID: "app", Identity: IdentityPolicy{P2P: IdentityBot}, VaultIDs: []string{"vault"}, BotCredential: BotCredentialSpec{VaultID: "vault", SecretName: "token", AllowedHosts: []string{"open.feishu.cn"}}}
	entered := make(chan struct{})
	c := Connection{Binding: b, Provider: identityBlockingProvider{entered: entered}, MAClient: identityNoopVault{}}
	firstCtx, firstCancel := context.WithCancel(context.Background())
	defer firstCancel()
	firstDone := make(chan error, 1)
	go func() { firstDone <- r.ensureBotIdentity(firstCtx, c, "p2p") }()
	<-entered
	waiterCtx, waiterCancel := context.WithCancel(context.Background())
	waiterDone := make(chan error, 1)
	go func() { waiterDone <- r.ensureBotIdentity(waiterCtx, c, "p2p") }()
	waiterCancel()
	waiterReturned := false
	select {
	case err := <-waiterDone:
		require.ErrorIs(t, err, context.Canceled)
		waiterReturned = true
	case <-time.After(100 * time.Millisecond):
		t.Error("cancelled waiter retained its identity reference until an unrelated refresh finished")
	}
	firstCancel()
	<-firstDone
	if !waiterReturned {
		require.ErrorIs(t, <-waiterDone, context.Canceled)
	}
	require.Zero(t, r.identities[botIdentityKey(b)].refs)
}
