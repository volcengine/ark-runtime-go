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
	"github.com/volcengine/ark-runtime-go/channel/ma"
)

type cleanupFaultStore struct {
	core.Store
	failure error
	faults  atomic.Int32
}

func (s *cleanupFaultStore) UpdateTurn(ctx context.Context, l core.Lease, u core.TurnUpdate) (core.Turn, error) {
	if u.RemoteCleanup {
		s.faults.Add(1)
		return core.Turn{}, core.NewStoreFailure(s.failure)
	}
	return s.Store.UpdateTurn(ctx, l, u)
}

func TestSynchronousCleanupStoreFailureStopsRuntime(t *testing.T) {
	for _, mode := range []string{"stop", "deadline", "abort", "requires_action_rejected"} {
		t.Run(mode, func(t *testing.T) {
			f := &conformance.MAServer{}
			options := testRuntimeOptions()
			options.Workers = 1
			clock := conformance.NewClock(time.Now())
			if mode == "deadline" {
				options.Clock = clock.Now
			}
			if mode == "requires_action_rejected" {
				f.SendCodes = []int{400}
				f.SendErrorCodes = []string{"RequiresActionRejected"}
			}
			h := newHarnessWith(t, f, &runtimeProvider{}, runtimeBinding(), options)
			failure := errors.New("cleanup persistence io")
			store := &cleanupFaultStore{Store: h.store, failure: failure}
			client, err := newTestMA(h.server.URL, "fixture", ma.ClientOptions{})
			require.NoError(t, err)
			runtime, err := core.NewRuntime(store, []core.Connection{{Binding: runtimeBinding(), MAClient: client, Provider: &runtimeProvider{}}}, options)
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
			defer cancel()
			first, err := runtime.Enqueue(ctx, "binding", runtimeMessage())
			require.NoError(t, err)
			done := make(chan error, 1)
			go func() { done <- runtime.Run(ctx) }()
			if mode == "stop" || mode == "abort" || mode == "deadline" {
				h.phaseEventually(core.PhaseObserve)
			}
			if mode == "deadline" {
				// 先确认远端输入已提交，再推进逻辑时间，确保命中需要清理的超时分支。
				clock.Advance(options.MessageDeadline + time.Second)
			}
			if mode == "stop" {
				_, err = runtime.Enqueue(ctx, "binding", stopMessage("failure"))
				require.NoError(t, err)
			}
			if mode == "abort" {
				f.Lock()
				f.Events = append(f.Events, map[string]any{"id": "unsupported-error", "type": "session.error"})
				f.Unlock()
			}
			require.ErrorIs(t, <-done, failure)
			require.Positive(t, store.faults.Load())
			turn, err := h.store.GetTurn(context.Background(), first.Turn.ID)
			require.NoError(t, err)
			require.False(t, turn.Status.Terminal(), "failed cleanup persistence must not settle the turn")
			export, err := h.store.Export(context.Background())
			require.NoError(t, err)
			require.Empty(t, export.Deliveries, "failed cleanup persistence must not acknowledge stopped")
			f.Lock()
			interrupts := f.InterruptCount
			f.Unlock()
			require.Zero(t, interrupts, "cleanup intent must commit before interrupt HTTP")
		})
	}
}

type interruptFaultClient struct {
	core.MAClient
	calls atomic.Int32
}

func (c *interruptFaultClient) SendInterrupt(context.Context, string, string) (core.SentResponse, error) {
	c.calls.Add(1)
	return core.SentResponse{}, errors.New("remote interrupt unavailable")
}

func TestSynchronousCleanupRemoteFailureKeepsReservation(t *testing.T) {
	f := &conformance.MAServer{}
	h := newHarness(t, f, &runtimeProvider{})
	client, err := newTestMA(h.server.URL, "fixture", ma.ClientOptions{})
	require.NoError(t, err)
	faultClient := &interruptFaultClient{MAClient: client}
	h.runtime, err = core.NewRuntime(h.store, []core.Connection{{Binding: runtimeBinding(), MAClient: faultClient, Provider: &runtimeProvider{}}}, testRuntimeOptions())
	require.NoError(t, err)
	first := h.enqueue(runtimeMessage())
	h.run()
	h.phaseEventually(core.PhaseObserve)
	h.enqueue(stopMessage("remote-failure"))
	state := h.waitSettled(core.TurnStopped, core.DeliveryDone, 2*time.Second)
	require.Equal(t, first.Turn.ID, state.turn.ID)
	require.Positive(t, faultClient.calls.Load())
	ops, err := h.store.ListRemoteOperations(context.Background(), core.RemoteOperationQuery{TurnID: first.Turn.ID, OpenOnly: true})
	require.NoError(t, err)
	require.Len(t, ops, 1)
	require.True(t, ops[0].CleanupStarted)
	require.True(t, ops[0].Counted)
}

func TestSynchronousCleanupLeaseLossIsTaskLocal(t *testing.T) {
	f := &conformance.MAServer{}
	h := newHarness(t, f, &runtimeProvider{})
	store := &cleanupFaultStore{Store: h.store, failure: core.ErrLeaseLost}
	client, err := newTestMA(h.server.URL, "fixture", ma.ClientOptions{})
	require.NoError(t, err)
	runtime, err := core.NewRuntime(store, []core.Connection{{Binding: runtimeBinding(), MAClient: client, Provider: &runtimeProvider{}}}, testRuntimeOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first, err := runtime.Enqueue(ctx, "binding", runtimeMessage())
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	h.phaseEventually(core.PhaseObserve)
	_, err = runtime.Enqueue(ctx, "binding", stopMessage("lease-loss"))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return store.faults.Load() > 0 }, time.Second, 5*time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("lease loss must not stop runtime: %v", err)
	default:
	}
	turn, err := h.store.GetTurn(ctx, first.Turn.ID)
	require.NoError(t, err)
	require.False(t, turn.Status.Terminal())
	cancel()
	require.NoError(t, <-done)
}
