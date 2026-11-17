// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStageRuntimeOptions(t *testing.T) {
	options, err := (RuntimeOptions{ObserveConcurrency: 256}).Normalize()
	require.NoError(t, err)
	require.Equal(t, 8, options.CreateConcurrency)
	require.Equal(t, 32, options.SubmitConcurrency)
	require.Equal(t, 256, options.ObserveConcurrency)
	require.Zero(t, options.Workers)
	require.Zero(t, options.AccountLimit)
	require.Zero(t, options.CreationLimit)
	for _, bad := range []RuntimeOptions{
		{CreateConcurrency: -1}, {SubmitConcurrency: -1}, {ObserveConcurrency: -1},
		{CreateConcurrency: 129}, {SubmitConcurrency: 257}, {ObserveConcurrency: 4097},
		{ObserveConcurrency: 256, Workers: 8}, {ObserveConcurrency: 256, AccountLimit: 1}, {ObserveConcurrency: 256, CreationLimit: 1},
	} {
		_, err := bad.Normalize()
		require.ErrorIs(t, err, ErrRuntimeConfig)
	}
	legacy, err := (RuntimeOptions{Workers: 2, AccountLimit: 1}).Normalize()
	require.NoError(t, err)
	require.Equal(t, 2, legacy.Workers)
	require.Equal(t, 1, legacy.CreationLimit)
	require.Zero(t, legacy.ObserveConcurrency)
}

type poolStore struct {
	Store
	leases atomic.Int32
	claim  func(context.Context, ClaimOptions) (*TurnLease, error)
}

func (s *poolStore) LeaseTurn(ctx context.Context, claim ClaimOptions) (*TurnLease, error) {
	s.leases.Add(1)
	return s.claim(ctx, claim)
}

func TestStagePoolIdlePollingIsIndependentOfCapacity(t *testing.T) {
	s := &poolStore{claim: func(_ context.Context, c ClaimOptions) (*TurnLease, error) {
		require.Equal(t, StageObserve, c.Stage)
		return nil, nil
	}}
	r := &Runtime{store: s, options: RuntimeOptions{PollInterval: 20 * time.Millisecond}}
	ctx, cancel := context.WithTimeout(t.Context(), 75*time.Millisecond)
	defer cancel()
	require.NoError(t, r.dispatchTurns(ctx, StageObserve, 256, nil))
	require.GreaterOrEqual(t, s.leases.Load(), int32(1))
	require.LessOrEqual(t, s.leases.Load(), int32(5))
}

func TestStagePoolOnlyClaimsAvailableCapacityAndWaitsOnShutdown(t *testing.T) {
	s := &poolStore{claim: func(_ context.Context, c ClaimOptions) (*TurnLease, error) {
		return &TurnLease{Turn: Turn{Lease: Lease{Owner: c.Owner}}}, nil
	}}
	r := &Runtime{store: s, options: RuntimeOptions{PollInterval: time.Millisecond}}
	ctx, cancel := context.WithCancel(t.Context())
	entered := make(chan struct{}, 2)
	finished := make(chan struct{}, 2)
	done := make(chan error, 1)
	go func() {
		done <- r.dispatchTurns(ctx, StageCreate, 2, func(ctx context.Context, l *TurnLease, stage TurnStage) error {
			entered <- struct{}{}
			<-ctx.Done()
			finished <- struct{}{}
			return nil
		})
	}()
	for range 2 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("pool did not start")
		}
	}
	require.Never(t, func() bool { return s.leases.Load() > 2 }, 40*time.Millisecond, time.Millisecond)
	cancel()
	require.NoError(t, <-done)
	require.Len(t, finished, 2)
	require.Equal(t, int32(2), s.leases.Load())
}

func TestStagePoolPropagatesFailureAndPanic(t *testing.T) {
	for _, panics := range []bool{false, true} {
		s := &poolStore{claim: func(context.Context, ClaimOptions) (*TurnLease, error) { return &TurnLease{}, nil }}
		r := &Runtime{store: s, options: RuntimeOptions{PollInterval: time.Millisecond}}
		failure := errors.New("pool failed")
		err := r.dispatchTurns(t.Context(), StageSubmit, 1, func(context.Context, *TurnLease, TurnStage) error {
			if panics {
				panic("unexpected")
			}
			return failure
		})
		if panics {
			require.ErrorContains(t, err, "panic")
		} else {
			require.ErrorIs(t, err, failure)
		}
	}
}

func TestStageSubmitPermitIsSharedAndCancellationDoesNotAcquire(t *testing.T) {
	r := &Runtime{submitSlots: make(chan struct{}, 1)}
	release, ok := r.trySubmit(t.Context())
	require.True(t, ok)
	_, ok = r.trySubmit(t.Context())
	require.False(t, ok)
	release()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, ok = r.trySubmit(ctx)
	require.False(t, ok)
	require.Empty(t, r.submitSlots)
}

type capacityWaitStore struct {
	Store
	waits atomic.Int32
}

func (s *capacityWaitStore) WaitTurn(_ context.Context, _ Lease, wait TurnWait) error {
	if wait.Reason != "submit_capacity" {
		return errors.New("unexpected wait reason")
	}
	s.waits.Add(1)
	return nil
}
func TestSubmissionContextFailureDoesNotReplaceWaitReason(t *testing.T) {
	for _, expired := range []bool{false, true} {
		t.Run(fmt.Sprintf("expired=%t", expired), func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			if expired {
				cancel()
				ctx, cancel = context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
			}
			cancel()
			store := &capacityWaitStore{}
			r := &Runtime{store: store, options: RuntimeOptions{Clock: time.Now, PollInterval: time.Millisecond}}
			j := &job{stage: StageSubmit, Turn: Turn{Phase: PhaseSend, WaitReason: "attachment_unavailable", Schedule: Schedule{DeadlineAt: time.Now().Add(time.Hour)}}}
			err := r.sendAndObserve(ctx, j, Connection{})
			require.ErrorIs(t, err, ctx.Err())
			require.Zero(t, store.waits.Load(), "cancellation must not persist a capacity wait")
			require.Equal(t, "attachment_unavailable", j.WaitReason)
		})
	}
}
