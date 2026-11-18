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

type preparationFault struct{ permanent bool }

func (preparationFault) Error() string     { return "application preparation unavailable" }
func (e preparationFault) Permanent() bool { return e.permanent }

func TestSessionPreparationTransientFailureKeepsRuntimeServing(t *testing.T) {
	store, err := newEmbeddedStore(t)
	require.NoError(t, err)
	f, p := &conformance.MAServer{}, &runtimeProvider{}
	var calls atomic.Int32
	runtime, err := core.NewRuntime(store, []core.Connection{{Binding: runtimeBinding(), MAClient: newMAForHarness(t, f), Provider: p, PrepareSession: func(_ context.Context, b core.Binding) (core.Binding, error) {
		if calls.Add(1) == 1 {
			return core.Binding{}, preparationFault{}
		}
		return b, nil
	}}}, testRuntimeOptions())
	require.NoError(t, err)
	m := runtimeMessage()
	m.Text = "/new"
	mustEnqueue(t, runtime, "binding", m)
	runUntilCleanup(t, runtime)
	waitLatest(t, store, core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Equal(t, int32(2), calls.Load())
}
func TestSessionPreparationPermanentFailureDiagnosesWithoutStoppingRuntime(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		kind string
	}{
		{"permanent", preparationFault{true}, "preparation_rejected"},
		{"unsupported identity", core.ErrIdentityUnsupported, "identity_unsupported"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := newEmbeddedStore(t)
			require.NoError(t, err)
			f, p := &conformance.MAServer{}, &runtimeProvider{}
			runtime, err := core.NewRuntime(store, []core.Connection{{Binding: runtimeBinding(), MAClient: newMAForHarness(t, f), Provider: p, PrepareSession: func(context.Context, core.Binding) (core.Binding, error) {
				return core.Binding{}, errors.Join(errors.New("prepare"), tc.err)
			}}}, testRuntimeOptions())
			require.NoError(t, err)
			m := runtimeMessage()
			m.Text = "/new"
			mustEnqueue(t, runtime, "binding", m)
			runUntilCleanup(t, runtime)
			state := waitLatest(t, store, core.TurnFailed, core.DeliveryDone, time.Second)
			require.Equal(t, tc.kind, state.turn.Outcome.ErrorKind)
			f.Lock()
			defer f.Unlock()
			require.Zero(t, f.CreateCount)
		})
	}
}

type storeFailure struct{ error }

func (storeFailure) StoreFailure() bool { return true }
func (e storeFailure) Unwrap() error    { return e.error }

type fatalResolver struct {
	fixtureResolver
	failure  error
	delivery bool
}

func (r fatalResolver) ResolveForTurn(ctx context.Context, t core.Turn, s *core.SessionSnapshot) (core.Connection, error) {
	if !r.delivery {
		return core.Connection{}, r.failure
	}
	return r.fixtureResolver.ResolveForTurn(ctx, t, s)
}
func (r fatalResolver) ResolveForDelivery(context.Context, core.Delivery) (core.Connection, error) {
	return core.Connection{}, r.failure
}
func TestResolverStoreFailureTerminatesRuntime(t *testing.T) {
	for _, delivery := range []bool{false, true} {
		t.Run(map[bool]string{false: "turn", true: "delivery"}[delivery], func(t *testing.T) {
			store, err := newEmbeddedStore(t)
			require.NoError(t, err)
			failure := errors.New("persistent database failure")
			options := testRuntimeOptions()
			options.Resolver = fatalResolver{fixtureResolver: fixtureResolver{connections: map[string]core.Connection{"binding": {Binding: runtimeBinding(), MAClient: newMAForHarness(t, &conformance.MAServer{}), Provider: &runtimeProvider{}}}}, failure: storeFailure{failure}, delivery: delivery}
			runtime, err := core.NewRuntime(store, nil, options)
			require.NoError(t, err)
			message := runtimeMessage()
			if delivery {
				message = stopMessage("store")
			}
			mustEnqueue(t, runtime, "binding", message)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			require.ErrorIs(t, runtime.Run(ctx), failure, "typed Store failure must remain global infrastructure failure")
		})
	}
}

type failedLookupSnapshotStore struct {
	core.Store
	calls   atomic.Int32
	failure error
}

func (s *failedLookupSnapshotStore) GetSessionSnapshot(ctx context.Context, id string) (*core.SessionSnapshot, error) {
	if s.calls.Add(1) == 2 {
		return nil, s.failure
	}
	return s.Store.GetSessionSnapshot(ctx, id)
}
func TestUnknownCreationSnapshotStoreFailureTerminatesRuntime(t *testing.T) {
	backing, err := newEmbeddedStore(t)
	require.NoError(t, err)
	failure := errors.New("snapshot database failure")
	store := &failedLookupSnapshotStore{Store: backing, failure: failure}
	runtime := mustRuntime(t, store, runtimeBinding(), newMAForHarness(t, &conformance.MAServer{CreateCode: 503}), &runtimeProvider{}, testRuntimeOptions())
	message := runtimeMessage()
	message.Text = "/new"
	mustEnqueue(t, runtime, "binding", message)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, runtime.Run(ctx), failure)
}
