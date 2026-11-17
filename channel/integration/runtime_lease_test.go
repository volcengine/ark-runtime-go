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

// leaseFaultStore 注入一次竞争丢失或真实存储故障，后续操作仍使用真实存储。
type leaseFaultStore struct {
	core.Store
	point string
	fault error
	fired atomic.Bool
}

func (s *leaseFaultStore) LeaseTurn(ctx context.Context, claim core.ClaimOptions) (*core.TurnLease, error) {
	if s.point == "turn_claim" && s.fired.CompareAndSwap(false, true) {
		return nil, s.fault
	}
	return s.Store.LeaseTurn(ctx, claim)
}
func (s *leaseFaultStore) UpdateTurn(ctx context.Context, lease core.Lease, update core.TurnUpdate) (core.Turn, error) {
	if s.point == "turn_update" && s.fired.CompareAndSwap(false, true) {
		if errors.Is(s.fault, core.ErrLeaseLost) {
			_ = s.ReleaseTurn(ctx, lease)
		}
		return core.Turn{}, s.fault
	}
	return s.Store.UpdateTurn(ctx, lease, update)
}
func (s *leaseFaultStore) LeaseDelivery(ctx context.Context, owner string, ttl time.Duration) (*core.Delivery, error) {
	if s.point == "delivery_claim" && s.fired.CompareAndSwap(false, true) {
		return nil, s.fault
	}
	return s.Store.LeaseDelivery(ctx, owner, ttl)
}
func (s *leaseFaultStore) CompleteDelivery(ctx context.Context, lease core.Lease, result core.DeliveryResult) error {
	if s.point == "delivery_complete" && s.fired.CompareAndSwap(false, true) {
		if errors.Is(s.fault, core.ErrLeaseLost) {
			return errors.Join(s.fault, s.Store.CompleteDelivery(ctx, lease, result))
		}
		return s.fault
	}
	return s.Store.CompleteDelivery(ctx, lease, result)
}

func TestRuntimeLeaseLostIsolatesOnlyCompetingTask(t *testing.T) {
	for _, point := range []string{"turn_claim", "turn_update", "delivery_claim", "delivery_complete"} {
		t.Run(point, func(t *testing.T) {
			backing, err := newEmbeddedStore(t)
			require.NoError(t, err)
			store := &leaseFaultStore{Store: backing, point: point, fault: core.ErrLeaseLost}
			f, p := &conformance.MAServer{}, &runtimeProvider{}
			otherBinding := runtimeBinding()
			otherBinding.ID = "other-binding"
			otherBinding.AppID = "other-app"
			runtime, err := core.NewRuntime(store, []core.Connection{{Binding: runtimeBinding(), MAClient: newMAForHarness(t, f), Provider: p}, {Binding: otherBinding, MAClient: newMAForHarness(t, f), Provider: p}}, testRuntimeOptions())
			require.NoError(t, err)
			m := runtimeMessage()
			m.Text = "/new"
			mustEnqueue(t, runtime, "binding", m)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- runtime.Run(ctx) }()
			require.Eventually(t, func() bool {
				stats, e := backing.Stats(context.Background())
				return e == nil && stats.QueuedTurns+stats.RunningTurns+stats.WaitingTurns+stats.OpenDeliveries == 0
			}, time.Second, 5*time.Millisecond)
			require.True(t, store.fired.Load())
			m.ProviderEventID, m.MessageID, m.ChatID = "second", "second", "other-chat"
			m.BindingID = "other-binding"
			mustEnqueue(t, runtime, "other-binding", m)
			require.Eventually(t, func() bool { return p.count() == 2 }, time.Second, 5*time.Millisecond, "other conversation must remain served")
			cancel()
			require.NoError(t, <-done)
		})
	}
}

func TestRuntimeStoreSystemFailureStillStopsRuntime(t *testing.T) {
	backing, err := newEmbeddedStore(t)
	require.NoError(t, err)
	failure := errors.New("database unavailable")
	runtime := mustRuntime(t, &leaseFaultStore{Store: backing, point: "turn_claim", fault: failure}, runtimeBinding(), newMAForHarness(t, &conformance.MAServer{}), &runtimeProvider{}, testRuntimeOptions())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.ErrorIs(t, runtime.Run(ctx), failure)
}

// renewalFaultStore 在第二次续租时释放旧 lease，模拟另一个副本接管前的 fence 丢失。
type renewalFaultStore struct {
	core.Store
	calls atomic.Int32
	lost  atomic.Bool
	fault error
}

func (s *renewalFaultStore) LeaseTurn(ctx context.Context, claim core.ClaimOptions) (*core.TurnLease, error) {
	if s.lost.Load() {
		return nil, nil
	}
	return s.Store.LeaseTurn(ctx, claim)
}
func (s *renewalFaultStore) RenewTurn(ctx context.Context, lease core.Lease, ttl time.Duration) (core.Lease, error) {
	if s.calls.Add(1) == 2 {
		s.lost.Store(true)
		_ = s.ReleaseTurn(ctx, lease)
		return core.Lease{}, s.fault
	}
	return s.Store.RenewTurn(ctx, lease, ttl)
}
func TestRuntimeRenewLeaseLossCancelsOnlyOldTaskAndAllowsTakeover(t *testing.T) {
	backing, err := newEmbeddedStore(t)
	require.NoError(t, err)
	store := &renewalFaultStore{Store: backing, fault: core.ErrLeaseLost}
	f, p := &conformance.MAServer{}, &runtimeProvider{}
	client := newMAForHarness(t, f)
	options := testRuntimeOptions()
	options.Workers = 1
	options.TurnTimeout = 150 * time.Millisecond
	options.LeaseTTL = 210 * time.Millisecond
	options.DeliveryTimeout = 100 * time.Millisecond
	entered := make(chan struct{})
	cancelled := make(chan struct{})
	first, err := core.NewRuntime(store, []core.Connection{{Binding: runtimeBinding(), MAClient: client, Provider: p, PrepareSession: func(ctx context.Context, b core.Binding) (core.Binding, error) {
		close(entered)
		<-ctx.Done()
		close(cancelled)
		return core.Binding{}, ctx.Err()
	}}}, options)
	require.NoError(t, err)
	m := runtimeMessage()
	m.Text = "/new"
	mustEnqueue(t, first, "binding", m)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- first.Run(ctx) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("creation preparation never started")
	}
	select {
	case <-cancelled:
	case <-time.After(140 * time.Millisecond):
		t.Fatal("lease loss did not cancel old work before turn timeout")
	}
	require.True(t, store.lost.Load())
	second := mustRuntime(t, backing, runtimeBinding(), client, p, options)
	runUntilCleanup(t, second)
	waitLatest(t, backing, core.TurnDone, core.DeliveryDone, time.Second)
	m.ProviderEventID, m.MessageID, m.ChatID = "next", "next", "other"
	mustEnqueue(t, second, "binding", m)
	require.Eventually(t, func() bool { return p.count() == 2 }, time.Second, 5*time.Millisecond)
	cancel()
	require.NoError(t, <-done)
	f.Lock()
	defer f.Unlock()
	require.Equal(t, 2, f.CreateCount, "old task must not create after losing lease")
}

type deliveryRenewalFaultStore struct {
	core.Store
	calls atomic.Int32
	lost  atomic.Bool
}

func (s *deliveryRenewalFaultStore) LeaseDelivery(ctx context.Context, owner string, ttl time.Duration) (*core.Delivery, error) {
	if s.lost.Load() {
		return nil, nil
	}
	return s.Store.LeaseDelivery(ctx, owner, ttl)
}
func (s *deliveryRenewalFaultStore) RenewDelivery(ctx context.Context, lease core.Lease, ttl time.Duration) (core.Lease, error) {
	if s.calls.Add(1) == 2 {
		s.lost.Store(true)
		_ = s.CompleteDelivery(ctx, lease, core.DeliveryResult{Status: core.DeliveryPending, NextAttemptAt: time.Now()})
		return core.Lease{}, core.ErrLeaseLost
	}
	return s.Store.RenewDelivery(ctx, lease, ttl)
}

type blockedDeliveryResolver struct {
	fixtureResolver
	cancelled chan struct{}
}

func (r blockedDeliveryResolver) ResolveForDelivery(ctx context.Context, d core.Delivery) (core.Connection, error) {
	<-ctx.Done()
	close(r.cancelled)
	return r.connections[d.BindingID], nil
}
func TestDeliveryLeaseLossPreventsOldProviderCallAndAllowsTakeover(t *testing.T) {
	backing, err := newEmbeddedStore(t)
	require.NoError(t, err)
	store := &deliveryRenewalFaultStore{Store: backing}
	f, p := &conformance.MAServer{}, &runtimeProvider{}
	client := newMAForHarness(t, f)
	options := testRuntimeOptions()
	options.TurnTimeout = 150 * time.Millisecond
	options.LeaseTTL = 210 * time.Millisecond
	options.DeliveryTimeout = 100 * time.Millisecond
	cancelled := make(chan struct{})
	options.Resolver = blockedDeliveryResolver{fixtureResolver: fixtureResolver{connections: map[string]core.Connection{"binding": {Binding: runtimeBinding(), MAClient: client, Provider: p}}}, cancelled: cancelled}
	first, err := core.NewRuntime(store, nil, options)
	require.NoError(t, err)
	mustEnqueue(t, first, "binding", stopMessage("delivery"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- first.Run(ctx) }()
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("delivery lease loss never cancelled resolver")
	}
	require.Zero(t, p.count(), "old delivery fence must never call provider after resolver returns")
	options.Resolver = nil
	second := mustRuntime(t, backing, runtimeBinding(), client, p, options)
	runUntilCleanup(t, second)
	require.Eventually(t, func() bool { return p.count() == 1 }, time.Second, 5*time.Millisecond)
	cancel()
	require.NoError(t, <-done)
}
