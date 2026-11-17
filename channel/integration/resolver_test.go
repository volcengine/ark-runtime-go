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

type fixtureResolver struct {
	connections   map[string]core.Connection
	deliveryError bool
	generationTwo *atomic.Int32
}

func (f fixtureResolver) ResolveIngest(_ context.Context, s core.IngestScope) (core.ResolvedIngest, error) {
	return core.ResolvedIngest{Connection: f.connections[s.BindingID], AdmitUntil: time.Now().Add(time.Minute)}, nil
}
func (f fixtureResolver) ResolveForTurn(_ context.Context, t core.Turn, s *core.SessionSnapshot) (core.Connection, error) {
	if f.generationTwo != nil && t.Session.Generation == 2 {
		f.generationTwo.Add(1)
	}
	if t.BindingID == "bad" {
		return core.Connection{}, errors.New("one application unavailable")
	}
	return f.connections[t.BindingID], nil
}
func (f fixtureResolver) ResolveForDelivery(_ context.Context, d core.Delivery) (core.Connection, error) {
	if f.deliveryError {
		return core.Connection{}, errors.New("credentials unavailable")
	}
	return f.connections[d.BindingID], nil
}

func TestDynamicResolverIsolatesBrokenBinding(t *testing.T) {
	store, err := newEmbeddedStore(t)
	require.NoError(t, err)
	fixture := &conformance.MAServer{}
	client := newMAForHarness(t, fixture)
	provider := &runtimeProvider{}
	connections := map[string]core.Connection{}
	for _, id := range []string{"bad", "good"} {
		b := runtimeBinding()
		b.ID = id
		b.Owner = "account"
		b.BindingEpoch = 1
		connections[id] = core.Connection{Binding: b, MAClient: client, Provider: provider}
	}
	options := testRuntimeOptions()
	options.Resolver = fixtureResolver{connections: connections}
	runtime, err := core.NewRuntime(store, nil, options)
	require.NoError(t, err)
	for _, id := range []string{"bad", "good"} {
		m := runtimeMessage()
		m.BindingID = id
		m.MessageID = id
		m.ProviderEventID = id
		_, err = runtime.EnqueueScoped(context.Background(), core.IngestScope{Owner: "account", BindingID: id, BindingEpoch: 1}, m)
		require.NoError(t, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	t.Cleanup(func() {
		if t.Failed() {
			e, _ := store.Export(context.Background())
			for _, turn := range e.Turns {
				t.Logf("turn=%s binding=%s status=%s phase=%s wait=%s outcome=%+v", turn.ID, turn.BindingID, turn.Status, turn.Phase, turn.WaitReason, turn.Outcome)
			}
		}
	})
	fixture.AppendAfterInput(t, conformance.AnswerEvents("healthy")...)
	require.Eventually(t, func() bool { return provider.count() > 0 }, time.Second, 10*time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("one failed resolver stopped runtime: %v", err)
	default:
	}
	cancel()
	require.NoError(t, <-done)
}

func TestDynamicResolverRestoredStopInterruptsRemote(t *testing.T) {
	store, err := newEmbeddedStore(t)
	require.NoError(t, err)
	fixture := &conformance.MAServer{}
	b := runtimeBinding()
	client := newMAForHarness(t, fixture)
	opts := testRuntimeOptions()
	opts.Resolver = fixtureResolver{connections: map[string]core.Connection{b.ID: {Binding: b, MAClient: client, Provider: &runtimeProvider{}}}}
	runtime, err := core.NewRuntime(store, nil, opts)
	require.NoError(t, err)
	accepted, err := runtime.Enqueue(context.Background(), b.ID, runtimeMessage())
	require.NoError(t, err)
	leased, err := store.LeaseTurn(context.Background(), conformance.Claim("before-restart"))
	require.NoError(t, err)
	_, err = store.UpdateTurn(context.Background(), leased.Turn.Lease, core.TurnUpdate{Phase: core.PhaseObserve, Session: core.TurnSession{Generation: 1, SessionID: "sesn-demo", CreationKey: "key"}})
	require.NoError(t, err)
	require.NoError(t, store.ReleaseTurn(context.Background(), leased.Turn.Lease))
	stop := runtimeMessage()
	stop.MessageID = "stop"
	stop.ProviderEventID = "stop"
	stop.Text = "/stop"
	_, err = runtime.Enqueue(context.Background(), b.ID, stop)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	defer func() { cancel(); require.NoError(t, <-done) }()
	require.Eventually(t, func() bool {
		turn, _ := store.GetTurn(context.Background(), accepted.Turn.ID)
		return turn != nil && turn.Status == core.TurnStopped
	}, time.Second, 5*time.Millisecond)
	fixture.Lock()
	defer fixture.Unlock()
	require.Equal(t, 1, fixture.InterruptCount)
}

func TestDynamicResolverDeliveryFailureHasFiniteBudget(t *testing.T) {
	store, err := newEmbeddedStore(t)
	require.NoError(t, err)
	fixture := &conformance.MAServer{}
	b := runtimeBinding()
	opts := testRuntimeOptions()
	opts.DeliveryMaxAttempts = 2
	opts.Resolver = fixtureResolver{connections: map[string]core.Connection{b.ID: {Binding: b, MAClient: newMAForHarness(t, fixture), Provider: &runtimeProvider{}}}, deliveryError: true}
	runtime, err := core.NewRuntime(store, nil, opts)
	require.NoError(t, err)
	_, err = runtime.Enqueue(context.Background(), b.ID, runtimeMessage())
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	defer func() { cancel(); require.NoError(t, <-done) }()
	fixture.AppendAfterInput(t, conformance.AnswerEvents("answer")...)
	require.Eventually(t, func() bool {
		rows, _ := store.ListDeliveries(context.Background(), core.DeliveryQuery{Limit: 10})
		return len(rows) == 1 && rows[0].Status == core.DeliveryFailed
	}, time.Second, 10*time.Millisecond)
}

func TestDynamicResolverResolvesNewGenerationAfterSessionGone(t *testing.T) {
	fixture := &conformance.MAServer{CreateSessionID: "sesn-new", SessionStatus: map[string]string{"sesn-old": "terminated"}}
	h := newHarness(t, fixture, &runtimeProvider{})
	seedConversation(t, h.store, "sesn-old")
	var generationTwo atomic.Int32
	b := runtimeBinding()
	opts := testRuntimeOptions()
	opts.Resolver = fixtureResolver{connections: map[string]core.Connection{b.ID: {Binding: b, MAClient: newMAForHarness(t, fixture), Provider: &runtimeProvider{}}}, generationTwo: &generationTwo}
	var err error
	h.runtime, err = core.NewRuntime(h.store, nil, opts)
	require.NoError(t, err)
	h.enqueue(runtimeMessage())
	h.run()
	fixture.AppendAfterInput(t, conformance.AnswerEvents("new generation")...)
	h.waitSettled(core.TurnDone, core.DeliveryDone, 3*time.Second)
	require.Positive(t, generationTwo.Load())
}

type disabledCleanupResolver struct {
	fixtureResolver
	disabled atomic.Bool
	cleanups atomic.Int32
	releases atomic.Int32
	invalid  atomic.Int32
	frozen   core.Turn
}

func (r *disabledCleanupResolver) ResolveForTurn(ctx context.Context, t core.Turn, s *core.SessionSnapshot) (core.Connection, error) {
	if r.disabled.Load() {
		return core.Connection{}, core.ErrChannelDisabled
	}
	return r.fixtureResolver.ResolveForTurn(ctx, t, s)
}
func (r *disabledCleanupResolver) ResolveForCleanup(_ context.Context, t core.Turn, snapshot *core.SessionSnapshot) (core.Connection, error) {
	r.cleanups.Add(1)
	if snapshot == nil || snapshot.ID != r.frozen.Session.SnapshotID ||
		t.Session.SessionID != r.frozen.Session.SessionID || t.Session.Generation != r.frozen.Session.Generation ||
		t.BindingVersion != r.frozen.BindingVersion {
		r.invalid.Add(1)
	}
	connection := r.connections[t.BindingID]
	connection.Provider = nil
	connection.Release = func() { r.releases.Add(1) }
	return connection, nil
}
func TestDisabledSubmittedTurnUsesFrozenCleanupConnection(t *testing.T) {
	f, p := &conformance.MAServer{}, &runtimeProvider{}
	store, err := newEmbeddedStore(t)
	require.NoError(t, err)
	resolver := &disabledCleanupResolver{fixtureResolver: fixtureResolver{connections: map[string]core.Connection{"binding": {Binding: runtimeBinding(), MAClient: newMAForHarness(t, f), Provider: p}}}}
	options := testRuntimeOptions()
	options.Resolver = resolver
	options.TurnTimeout = time.Second
	first, err := core.NewRuntime(store, nil, options)
	require.NoError(t, err)
	accepted := mustEnqueue(t, first, "binding", runtimeMessage())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- first.Run(ctx) }()
	require.Eventually(t, func() bool {
		turn, e := store.GetTurn(context.Background(), accepted.Turn.ID)
		return e == nil && turn.Phase == core.PhaseObserve
	}, time.Second, 5*time.Millisecond)
	cancel()
	require.NoError(t, <-done)
	frozen, err := store.GetTurn(context.Background(), accepted.Turn.ID)
	require.NoError(t, err)
	require.NotNil(t, frozen)
	resolver.frozen = *frozen
	resolver.disabled.Store(true)
	second, err := core.NewRuntime(store, nil, options)
	require.NoError(t, err)
	ctx, cancel = context.WithCancel(context.Background())
	done = make(chan error, 1)
	go func() { done <- second.Run(ctx) }()
	t.Cleanup(func() { cancel(); require.NoError(t, <-done) })
	// 终态后的远端对账也会解析清理连接，校验每次冻结身份与释放，不按调度次数判断重复执行。
	require.Eventually(t, func() bool {
		turn, e := store.GetTurn(context.Background(), accepted.Turn.ID)
		return e == nil && turn.Status == core.TurnFailed && turn.Outcome.ErrorKind == "channel_disabled"
	}, time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool { return resolver.cleanups.Load() >= 2 }, time.Second, 5*time.Millisecond)
	require.Zero(t, resolver.invalid.Load())
	require.Eventually(t, func() bool { return resolver.releases.Load() == resolver.cleanups.Load() }, time.Second, 5*time.Millisecond)
	f.Lock()
	defer f.Unlock()
	require.Equal(t, 1, f.InterruptCount)
	require.Equal(t, 1, f.SendCount)
	require.Equal(t, 1, f.CreateCount)
}
