// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

type recoveringPromptAuthorizer struct {
	*userAuthFixture
	failure error
	ready   atomic.Bool
	reads   atomic.Int32
	invalid bool
}

func (a *recoveringPromptAuthorizer) AuthPrompt(_ context.Context, ref core.AuthFlowRef) (core.AuthFlow, error) {
	a.reads.Add(1)
	if !a.ready.Load() {
		return core.AuthFlow{}, a.failure
	}
	flow := core.AuthFlow{ID: ref.FlowID, URL: "https://accounts.feishu.cn/device", UserCode: "RECOVERED-CODE"}
	if a.invalid {
		flow.ID = "other-flow"
	}
	return flow, nil
}

func seedPendingPrompt(t *testing.T, store core.UserAuthStore, binding core.Binding) core.UserAuthRecord {
	t.Helper()
	ctx := context.Background()
	gate, err := store.BeginUserAuth(ctx, core.UserAuthBegin{Principal: userPrincipalFixture(binding), VaultID: "user-vault", Owner: "crashed-start", TTL: time.Minute})
	require.NoError(t, err)
	gate, err = store.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: gate.Principal, Lease: gate.Lease, Revision: gate.Revision, State: core.UserAuthPending, FlowID: "original-flow", ExpiresAt: time.Now().Add(time.Minute), Secret: &core.UserAuthSecret{Version: 1, KeyID: "key", Ciphertext: []byte("original encrypted flow")}, Release: true})
	require.NoError(t, err)
	return gate
}

func TestRuntimeAuthPromptTransientFailureRecoversOriginalPrompt(t *testing.T) {
	for _, failure := range []error{errors.New("temporary authorizer unavailable"), context.DeadlineExceeded} {
		t.Run(failure.Error(), func(t *testing.T) {
			store := userRuntimeStore(t, "embedded")
			auth := store.(core.UserAuthStore)
			binding := userBinding()
			authorizer := &recoveringPromptAuthorizer{userAuthFixture: &userAuthFixture{store: auth}, failure: failure}
			options := testRuntimeOptions()
			options.PollInterval = 100 * time.Millisecond
			options.UserAuthPollInterval = time.Second
			runtime, err := core.NewRuntime(store, []core.Connection{{Binding: binding, Provider: &runtimeProvider{}, MAClient: newMAForHarness(t, &conformance.MAServer{}), UserAuthorizer: authorizer}}, options)
			require.NoError(t, err)
			result := mustEnqueue(t, runtime, binding.ID, runtimeMessage())
			original := seedPendingPrompt(t, auth, binding)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- runtime.Run(ctx) }()
			defer func() { cancel(); require.NoError(t, <-done) }()
			require.Eventually(t, func() bool {
				turn, err := store.GetTurn(context.Background(), result.Turn.ID)
				return err == nil && turn.WaitReason == "user_auth_prompt_unavailable" && turn.Status == core.TurnWaiting
			}, time.Second, 5*time.Millisecond)
			saved, err := auth.GetUserAuth(context.Background(), original.Principal)
			require.NoError(t, err)
			require.Equal(t, original.FlowID, saved.FlowID)
			require.Equal(t, original.ExpiresAt, saved.ExpiresAt)
			require.Equal(t, original.Attempt, saved.Attempt)
			authorizer.ready.Store(true)
			require.Eventually(t, func() bool {
				deliveries, err := store.ListDeliveries(context.Background(), core.DeliveryQuery{})
				return err == nil && len(deliveries) == 1 && deliveries[0].Body.Text != ""
			}, time.Second, 5*time.Millisecond)
			deliveries, err := store.ListDeliveries(context.Background(), core.DeliveryQuery{})
			require.NoError(t, err)
			require.Contains(t, deliveries[0].Body.Text, "RECOVERED-CODE")
			authorizer.mu.Lock()
			starts, polls := authorizer.starts, authorizer.polls
			authorizer.mu.Unlock()
			require.Zero(t, starts)
			require.Zero(t, polls, "Status cannot run before the recovered prompt is displayed")
		})
	}
}

func TestRuntimeAuthPromptInvalidFlowFailsOnlyTurn(t *testing.T) {
	store := userRuntimeStore(t, "embedded")
	auth := store.(core.UserAuthStore)
	binding := userBinding()
	authorizer := &recoveringPromptAuthorizer{userAuthFixture: &userAuthFixture{store: auth}, invalid: true}
	authorizer.ready.Store(true)
	runtime, err := core.NewRuntime(store, []core.Connection{{Binding: binding, Provider: &runtimeProvider{}, MAClient: newMAForHarness(t, &conformance.MAServer{}), UserAuthorizer: authorizer}}, testRuntimeOptions())
	require.NoError(t, err)
	result := mustEnqueue(t, runtime, binding.ID, runtimeMessage())
	original := seedPendingPrompt(t, auth, binding)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	defer func() { cancel(); require.NoError(t, <-done) }()
	require.Eventually(t, func() bool {
		turn, err := store.GetTurn(context.Background(), result.Turn.ID)
		return err == nil && turn.Status == core.TurnFailed && turn.Outcome.ErrorKind == "user_auth_failed"
	}, time.Second, 5*time.Millisecond)
	saved, err := auth.GetUserAuth(context.Background(), original.Principal)
	require.NoError(t, err)
	require.Equal(t, core.UserAuthFailed, saved.State)
}
