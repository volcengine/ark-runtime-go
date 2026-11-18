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

func TestCreateRetryUsesFrozenHookAndEnvironment(t *testing.T) {
	f := &conformance.MAServer{CreateCode: 429, EnvironmentType: "cloud", EnvironmentEnv: map[string]string{"BASE": "original"}}
	var hooks atomic.Int32
	h := hookHarness(t, f, core.Hooks{BuildSessionSpec: func(context.Context, core.HookMessage) (core.SessionExtras, error) {
		hooks.Add(1)
		return core.SessionExtras{Env: map[string]string{"HOOK": "frozen"}}, nil
	}}, nil)
	h.enqueue(runtimeMessage())
	h.run()
	require.Eventually(t, func() bool { f.Lock(); defer f.Unlock(); return f.CreateCount >= 2 }, 10*time.Second, 10*time.Millisecond)
	f.Lock()
	f.EnvironmentEnv = map[string]string{"BASE": "changed"}
	f.CreateCode = 0
	f.Unlock()
	f.AppendAfterInput(t, conformance.AnswerEvents("ok")...)
	h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Equal(t, int32(1), hooks.Load(), "retry must not rerun hook")
	f.Lock()
	defer f.Unlock()
	require.Equal(t, 1, f.EnvironmentGetCount, "重试不得重读base")
	require.Equal(t, "env", f.CreateEnvironment["id"])
	config := f.CreateEnvironment["config"].(map[string]any)
	require.Equal(t, "original", config["env"].(map[string]any)["BASE"])
}

func TestCreateSnapshotSurvivesRuntimeRestart(t *testing.T) {
	f := &conformance.MAServer{CreateCode: 429, EnvironmentType: "cloud", EnvironmentEnv: map[string]string{"BASE": "original"}}
	h := hookHarness(t, f, core.Hooks{BuildSessionSpec: func(context.Context, core.HookMessage) (core.SessionExtras, error) {
		return core.SessionExtras{Env: map[string]string{"HOOK": "original"}}, nil
	}}, nil)
	accepted := h.enqueue(runtimeMessage())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.runtime.Run(ctx) }()
	require.Eventually(t, func() bool { f.Lock(); defer f.Unlock(); return f.CreateCount > 0 }, 10*time.Second, 10*time.Millisecond)
	cancel()
	require.NoError(t, <-done)
	saved, err := h.store.GetTurn(context.Background(), accepted.Turn.ID)
	require.NoError(t, err)
	require.NotEmpty(t, saved.Session.SnapshotID)
	snapshot, err := h.store.GetSessionSnapshot(context.Background(), saved.Session.SnapshotID)
	require.NoError(t, err)
	f.Lock()
	f.CreateCode = 0
	f.EnvironmentEnv = map[string]string{"BASE": "changed"}
	f.Unlock()
	options := testRuntimeOptions()
	options.Hooks.BuildSessionSpec = func(context.Context, core.HookMessage) (core.SessionExtras, error) {
		return core.SessionExtras{}, errors.New("must not rerun hook")
	}
	binding := runtimeBinding()
	binding.MaxTurnDuration = 0
	h.runtime = mustRuntime(t, h.store, binding, newMAForHarness(t, f), &runtimeProvider{}, options)
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("restored")...)
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Equal(t, snapshot.ID, state.turn.Session.SnapshotID)
	after, err := h.store.GetSessionSnapshot(context.Background(), snapshot.ID)
	require.NoError(t, err)
	require.Equal(t, snapshot.RequestHash, after.RequestHash)
	f.Lock()
	defer f.Unlock()
	require.Equal(t, 1, f.EnvironmentGetCount, "重启仍复用冻结env")
}

func TestEnvironmentCreate412UsesGenericRejection(t *testing.T) {
	// 412 不再表示专用版本冲突，沿用所有确定性 4xx 的创建失败路径。
	f := &conformance.MAServer{CreateCode: 412, CreateErrorCode: "PreconditionFailed.Other", EnvironmentType: "cloud", EnvironmentEnv: map[string]string{"BASE": "original"}}
	h := hookHarness(t, f, core.Hooks{BuildSessionSpec: func(context.Context, core.HookMessage) (core.SessionExtras, error) {
		return core.SessionExtras{Env: map[string]string{"HOOK": "v"}}, nil
	}}, nil)
	h.enqueue(runtimeMessage())
	h.run()
	state := h.waitSettled(core.TurnFailed, core.DeliveryDone, 4*time.Second)
	require.Equal(t, "create_rejected", state.turn.Outcome.ErrorKind)
	f.Lock()
	defer f.Unlock()
	require.Equal(t, 1, f.CreateCount)
	require.Equal(t, 1, f.EnvironmentGetCount)
	require.Zero(t, f.ListSessionCount)
}

func TestRejectedCreationDoesNotPoisonConversation(t *testing.T) {
	f := &conformance.MAServer{CreateCode: 400}
	h := newHarness(t, f, &runtimeProvider{})
	h.enqueue(runtimeMessage())
	h.run()
	h.waitSettled(core.TurnFailed, core.DeliveryDone, 3*time.Second)
	f.Lock()
	f.CreateCode = 0
	f.Unlock()
	next := runtimeMessage()
	next.MessageID = "next"
	next.ProviderEventID = "next"
	h.enqueue(next)
	f.AppendAfterInput(t, conformance.AnswerEvents("recovered")...)
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 3*time.Second)
	require.Equal(t, uint64(2), state.turn.Session.Generation)
}

func TestUnknownCreationRecoversFrozenHookVaultAfterRestart(t *testing.T) {
	f := &conformance.MAServer{CreateCode: 503, CreatePersistThenError: true}
	h := hookHarness(t, f, core.Hooks{BuildSessionSpec: func(context.Context, core.HookMessage) (core.SessionExtras, error) {
		return core.SessionExtras{VaultIDs: []string{"hook-vault"}}, nil
	}}, nil)
	h.enqueue(runtimeMessage())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.runtime.Run(ctx) }()
	require.Eventually(t, func() bool { f.Lock(); defer f.Unlock(); return f.CreateCount > 0 }, time.Second, 5*time.Millisecond)
	cancel()
	require.NoError(t, <-done)
	h.runtime = mustRuntime(t, h.store, runtimeBinding(), newMAForHarness(t, f), &runtimeProvider{}, testRuntimeOptions())
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("adopted")...)
	h.waitSettled(core.TurnDone, core.DeliveryDone, 3*time.Second)
	f.Lock()
	defer f.Unlock()
	require.Equal(t, 1, f.CreateCount)
}

func TestEnvironmentUnsupportedTypeFailsBeforeCreate(t *testing.T) {
	for _, kind := range []string{"self_hosted", "environment", "ark_sandbox"} {
		t.Run(kind, func(t *testing.T) {
			f := &conformance.MAServer{EnvironmentType: kind}
			h := hookHarness(t, f, core.Hooks{BuildSessionSpec: func(context.Context, core.HookMessage) (core.SessionExtras, error) {
				return core.SessionExtras{Env: map[string]string{"HOOK": "v"}}, nil
			}}, nil)
			h.enqueue(runtimeMessage())
			h.run()
			state := h.waitSettled(core.TurnFailed, core.DeliveryDone, 3*time.Second)
			require.Equal(t, "environment_unsupported", state.turn.Outcome.ErrorKind)
			f.Lock()
			defer f.Unlock()
			require.Equal(t, 1, f.EnvironmentGetCount)
			require.Zero(t, f.CreateCount)
		})
	}
}

func TestFrozenInvalidEnvironmentFailsAsExplicitRejection(t *testing.T) {
	f := &conformance.MAServer{}
	h := newHarness(t, f, &runtimeProvider{})
	h.enqueue(runtimeMessage())
	ctx := context.Background()
	claimed, err := h.store.LeaseTurn(ctx, conformance.Claim("prepare"))
	require.NoError(t, err)
	require.NotNil(t, claimed)
	turn, err := h.store.UpdateTurn(ctx, claimed.Turn.Lease, core.TurnUpdate{Phase: core.PhaseCreate, Session: core.TurnSession{Generation: 1, CreationKey: "creation"}})
	require.NoError(t, err)
	spec := core.CreateSessionSpec{Binding: runtimeBinding(), CreationKey: "creation", LookupKey: "lookup", EnvironmentOverride: &core.EnvironmentOverride{Type: "environment", Env: map[string]string{"BASE": "frozen"}}}
	_, err = h.store.FreezeSession(ctx, claimed.Turn.Lease, core.NewSessionSnapshot(turn.ConversationID, 1, spec))
	require.NoError(t, err)
	require.NoError(t, h.store.WaitTurn(ctx, claimed.Turn.Lease, core.TurnWait{Reason: "ready", Until: time.Now()}))
	h.run()
	state := h.waitSettled(core.TurnFailed, core.DeliveryDone, 3*time.Second)
	require.Equal(t, "create_rejected", state.turn.Outcome.ErrorKind)
	f.Lock()
	defer f.Unlock()
	require.Zero(t, f.EnvironmentGetCount)
	require.Zero(t, f.CreateCount)
	require.Zero(t, f.ListSessionCount)
}

func TestExistingSessionKeepsFrozenEnvironment(t *testing.T) {
	f := &conformance.MAServer{EnvironmentType: "cloud", EnvironmentEnv: map[string]string{"BASE": "original"}}
	var hooks atomic.Int32
	h := hookHarness(t, f, core.Hooks{BuildSessionSpec: func(context.Context, core.HookMessage) (core.SessionExtras, error) {
		hooks.Add(1)
		return core.SessionExtras{Env: map[string]string{"HOOK": "original"}}, nil
	}}, nil)
	h.enqueue(runtimeMessage())
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("first")...)
	first := h.waitSettled(core.TurnDone, core.DeliveryDone, 3*time.Second)
	f.Lock()
	f.EnvironmentEnv = map[string]string{"BASE": "changed"}
	f.Unlock()
	next := runtimeMessage()
	next.MessageID = "next"
	next.ProviderEventID = "next"
	h.enqueue(next)
	f.AppendAfterInputCount(t, 2, conformance.AnswerEvents("second")...)
	second := h.waitSettled(core.TurnDone, core.DeliveryDone, 3*time.Second)
	require.Equal(t, first.turn.Session.SessionID, second.turn.Session.SessionID)
	require.Equal(t, int32(1), hooks.Load())
	f.Lock()
	defer f.Unlock()
	require.Equal(t, 1, f.CreateCount)
	require.Equal(t, 1, f.EnvironmentGetCount)
	config := f.CreateEnvironment["config"].(map[string]any)
	require.Equal(t, "original", config["env"].(map[string]any)["BASE"])
}

// hiddenCreationMA 模拟写入已落地但查询索引延迟；重启后仍只能查账。
type hiddenCreationMA struct {
	core.MAClient
	visible atomic.Bool
	lookups atomic.Int32
}

func (m *hiddenCreationMA) ListSessions(ctx context.Context, key, page string) (core.SessionPage, error) {
	m.lookups.Add(1)
	if !m.visible.Load() {
		return core.SessionPage{}, nil
	}
	return m.MAClient.ListSessions(ctx, key, page)
}
func TestUnknownCreationAdoptsLateVisibilityAcrossGraceAndRestart(t *testing.T) {
	f := &conformance.MAServer{CreateCode: 503, CreatePersistThenError: true}
	store, err := newEmbeddedStore(t)
	require.NoError(t, err)
	client := &hiddenCreationMA{MAClient: newMAForHarness(t, f)}
	opts := deadlineRuntimeOptions()
	opts.UnknownWriteGrace = 40 * time.Millisecond
	opts.MessageDeadline = 2 * time.Second
	first := mustRuntime(t, store, runtimeBinding(), client, &runtimeProvider{}, opts)
	m := runtimeMessage()
	m.Text = "/new"
	accepted := mustEnqueue(t, first, "binding", m)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- first.Run(ctx) }()
	require.Eventually(t, func() bool { return client.lookups.Load() >= 6 }, time.Second, 5*time.Millisecond)
	cancel()
	require.NoError(t, <-done)
	before, err := store.GetTurn(context.Background(), accepted.Turn.ID)
	require.NoError(t, err)
	require.Equal(t, uint64(1), before.Submit.WriteAttempts)
	second := mustRuntime(t, store, runtimeBinding(), client, &runtimeProvider{}, opts)
	runUntilCleanup(t, second)
	target := client.lookups.Load() + 6
	require.Eventually(t, func() bool { return client.lookups.Load() >= target }, time.Second, 5*time.Millisecond)
	f.Lock()
	require.Equal(t, 1, f.CreateCount)
	f.Unlock()
	client.visible.Store(true)
	after := waitLatest(t, store, core.TurnDone, core.DeliveryDone, time.Second)
	require.Equal(t, before.Session.Generation, after.turn.Session.Generation)
	require.Equal(t, before.Session.SnapshotID, after.turn.Session.SnapshotID)
	f.Lock()
	defer f.Unlock()
	require.Equal(t, 1, f.CreateCount)
}

func TestFrozenModelEndpointRejectsForeignCreationAndRecovery(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(map[bool]string{false: "create response", true: "unknown recovery"}[unknown], func(t *testing.T) {
			f := &conformance.MAServer{ModelEndpoint: "ep-foreign"}
			if unknown {
				f.CreateCode = 503
				f.CreatePersistThenError = true
			}
			binding := runtimeBinding()
			binding.ModelEndpoint = "ep-frozen"
			h := newHarnessWith(t, f, &runtimeProvider{}, binding, deadlineRuntimeOptions())
			message := runtimeMessage()
			message.Text = "/new"
			h.enqueue(message)
			h.run()
			state := h.waitSettled(core.TurnFailed, core.DeliveryDone, 3*time.Second)
			want := "create_response_conflict"
			if unknown {
				want = "create_unconfirmed"
			}
			require.Equal(t, want, state.turn.Outcome.ErrorKind)
			require.Empty(t, state.export.Conversations[0].Session.SessionID)
			f.Lock()
			defer f.Unlock()
			require.Equal(t, 1, f.CreateCount)
			require.Zero(t, f.SendCount)
		})
	}
}
