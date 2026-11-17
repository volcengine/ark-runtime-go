// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestDynamicMixedIdentityWarmUsesCurrentChatType(t *testing.T) {
	for _, tc := range []struct {
		chatType string
		wantErr  error
	}{{"group", nil}, {"topic", nil}, {"p2p", core.ErrIdentityUnsupported}} {
		t.Run(tc.chatType, func(t *testing.T) {
			binding := maintenanceBinding(1)
			binding.Identity.P2P = core.IdentityUser
			provider, vault := &maintenanceProvider{}, &vaultMA{}
			options := testRuntimeOptions()
			options.Resolver = fixtureResolver{}
			runtime, err := core.NewRuntime(newIdentityMaintenanceStore(t), nil, options)
			require.NoError(t, err)
			err = runtime.WarmBotIdentity(context.Background(), core.Connection{Binding: binding, Provider: provider, MAClient: vault}, tc.chatType)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				require.Zero(t, provider.calls.Load())
				require.Empty(t, vault.writes)
				return
			}
			require.NoError(t, err)
			require.Equal(t, int32(1), provider.calls.Load())
			require.Equal(t, []string{"vault-1/FEISHU_TENANT_ACCESS_TOKEN=token-1"}, vault.writes)
			require.Equal(t, core.IdentityUser, binding.Identity.P2P, "群身份校验不能改写原策略")
		})
	}
}

func TestDynamicMixedIdentityGroupCreatesAndRestoresFrozenSnapshot(t *testing.T) {
	for _, restored := range []bool{false, true} {
		name := "fresh"
		if restored {
			name = "restored"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store, err := newEmbeddedStore(t)
			require.NoError(t, err)
			binding := maintenanceBinding(1)
			binding.Identity.P2P = core.IdentityUser
			binding.ConfigVersion = 1
			message := runtimeMessage()
			message.ChatType, message.Mentioned = "group", true
			accepted, err := store.Ingest(ctx, core.NewBindingVersion(binding), message)
			require.NoError(t, err)
			var before *core.SessionSnapshot
			if restored {
				leased, leaseErr := store.LeaseTurn(ctx, conformance.Claim("before-restart"))
				require.NoError(t, leaseErr)
				require.NotNil(t, leased)
				turn, updateErr := store.UpdateTurn(ctx, leased.Turn.Lease, core.TurnUpdate{Phase: core.PhaseCreate, Session: core.TurnSession{Generation: 1, CreationKey: "frozen-create"}})
				require.NoError(t, updateErr)
				snapshot, freezeErr := store.FreezeSession(ctx, turn.Lease, core.NewSessionSnapshot(turn.ConversationID, 1, core.CreateSessionSpec{
					Binding: binding, CreationKey: "frozen-create", LookupKey: "frozen-lookup", ChatType: "group",
					EnvironmentOverride: &core.EnvironmentOverride{Type: "cloud", Env: map[string]string{"BASE": "frozen", core.EnvLarkSuiteStrictMode: "bot"}},
				}))
				require.NoError(t, freezeErr)
				before = &snapshot
				require.NoError(t, store.ReleaseTurn(ctx, turn.Lease))
			}
			fixture := &conformance.MAServer{EnvironmentType: "cloud", EnvironmentEnv: map[string]string{"BASE": "original"}}
			provider := &maintenanceProvider{}
			vault := &vaultMA{MAClient: newMAForHarness(t, fixture)}
			var prepares, hooks atomic.Int32
			current := binding
			current.ConfigVersion = 2
			current.Identity.P2P = core.IdentityBot
			connection := core.Connection{Binding: current, Provider: provider, MAClient: vault, PrepareSession: func(context.Context, core.Binding) (core.Binding, error) {
				prepares.Add(1)
				return binding, nil
			}}
			options := testRuntimeOptions()
			options.Resolver = fixtureResolver{connections: map[string]core.Connection{binding.ID: connection}}
			options.Hooks.BuildSessionSpec = func(context.Context, core.HookMessage) (core.SessionExtras, error) {
				hooks.Add(1)
				return core.SessionExtras{}, nil
			}
			runtime, err := core.NewRuntime(store, nil, options)
			require.NoError(t, err)
			runUntilCleanup(t, runtime)
			fixture.AppendAfterInput(t, conformance.AnswerEvents("group answer")...)
			state := waitSettledExternal(t, store, core.TurnDone, core.DeliveryDone, 3*time.Second)
			require.Equal(t, accepted.Turn.ID, state.turn.ID)
			snapshot, err := store.GetSessionSnapshot(ctx, state.turn.Session.SnapshotID)
			require.NoError(t, err)
			require.Equal(t, core.IdentityPolicy{Group: core.IdentityBot, P2P: core.IdentityUser}, snapshot.Spec.Binding.Identity)
			require.Equal(t, uint64(1), snapshot.Spec.Binding.ConfigVersion)
			if restored {
				require.Equal(t, before, snapshot, "恢复不得改写冻结请求或请求哈希")
				require.Zero(t, prepares.Load(), "恢复不得重新准备远端资源")
				require.Zero(t, hooks.Load())
			} else {
				require.Equal(t, int32(1), prepares.Load())
				require.Equal(t, int32(1), hooks.Load())
			}
			fixture.Lock()
			require.Equal(t, 1, fixture.CreateCount)
			require.Equal(t, 1, fixture.SendCount)
			require.Equal(t, "bot", fixture.CreateEnvironment["config"].(map[string]any)["env"].(map[string]any)[core.EnvLarkSuiteStrictMode])
			fixture.Unlock()
		})
	}
}

func TestDynamicMixedIdentityUserTurnFailsWithoutRemoteWrites(t *testing.T) {
	store, err := newEmbeddedStore(t)
	require.NoError(t, err)
	binding := maintenanceBinding(1)
	binding.Identity.P2P = core.IdentityUser
	fixture := &conformance.MAServer{EnvironmentType: "cloud"}
	provider := &maintenanceProvider{}
	vault := &vaultMA{MAClient: newMAForHarness(t, fixture)}
	options := testRuntimeOptions()
	options.Resolver = fixtureResolver{connections: map[string]core.Connection{binding.ID: {Binding: binding, Provider: provider, MAClient: vault}}}
	runtime, err := core.NewRuntime(store, nil, options)
	require.NoError(t, err)
	mustEnqueue(t, runtime, binding.ID, runtimeMessage())
	runUntilCleanup(t, runtime)
	state := waitSettledExternal(t, store, core.TurnFailed, core.DeliveryDone, 3*time.Second)
	require.Equal(t, "identity_unsupported", state.turn.Outcome.ErrorKind)
	require.Zero(t, provider.calls.Load(), "未开放的用户身份不能降级为Bot取令牌")
	vault.vmu.Lock()
	require.Empty(t, vault.writes)
	vault.vmu.Unlock()
	fixture.Lock()
	defer fixture.Unlock()
	require.Zero(t, fixture.CreateCount)
	require.Zero(t, fixture.SendCount)
	require.Zero(t, fixture.InterruptCount)
}

func TestDynamicMixedIdentityMaintenanceRestoresGroupSnapshot(t *testing.T) {
	ctx := context.Background()
	store := newIdentityMaintenanceStore(t)
	binding := maintenanceBinding(1)
	binding.Identity.P2P = core.IdentityUser
	message := runtimeMessage()
	message.ChatType, message.Mentioned = "group", true
	_, err := store.Ingest(ctx, core.NewBindingVersion(binding), message)
	require.NoError(t, err)
	leased, err := store.LeaseTurn(ctx, conformance.Claim("freeze-group"))
	require.NoError(t, err)
	turn, err := store.UpdateTurn(ctx, leased.Turn.Lease, core.TurnUpdate{Phase: core.PhaseCreate, Session: core.TurnSession{Generation: 1, CreationKey: "group-create"}})
	require.NoError(t, err)
	before, err := store.FreezeSession(ctx, turn.Lease, core.NewSessionSnapshot(turn.ConversationID, 1, core.CreateSessionSpec{Binding: binding, CreationKey: "group-create", LookupKey: "group-lookup", ChatType: "group"}))
	require.NoError(t, err)
	require.NoError(t, store.WaitTurn(ctx, turn.Lease, core.TurnWait{Reason: "long_group_task", Until: time.Now().Add(time.Hour)}))
	provider, vault := &maintenanceProvider{}, &vaultMA{}
	resolver := &maintenanceResolver{connections: map[uint64]core.Connection{1: {Binding: binding, Provider: provider, MAClient: vault}}}
	observer := &maintenanceRefreshObserver{}
	options := testRuntimeOptions()
	options.Observer = observer
	_, cancel, done := startIdentityMaintenance(t, identityMaintenanceStore{Store: store}, resolver, options)
	require.Eventually(t, func() bool { return observer.completed.Load() == 1 }, time.Second, 5*time.Millisecond)
	cancel()
	require.NoError(t, <-done)
	after, err := store.GetSessionSnapshot(ctx, before.ID)
	require.NoError(t, err)
	require.Equal(t, before, *after)
	require.Equal(t, int32(1), provider.calls.Load())
	vault.vmu.Lock()
	defer vault.vmu.Unlock()
	require.Equal(t, []string{"vault-1/FEISHU_TENANT_ACCESS_TOKEN=token-1"}, vault.writes)
}
