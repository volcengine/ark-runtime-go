// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTaskLeaseFinishDoesNotReportItsOwnCancellation(t *testing.T) {
	var calls atomic.Int32
	entered := make(chan struct{})
	guard := startTaskLease(context.Background(), 30*time.Millisecond, func(ctx context.Context) error {
		if calls.Add(1) > 1 {
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	})
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("renewal never started")
	}
	require.NoError(t, guard.finish(nil), "normal finish cancellation must not become a Store failure")
}
func TestTaskLeaseBoundsBlockedRenewalAndPreservesStoreFailure(t *testing.T) {
	var calls atomic.Int32
	guard := startTaskLease(context.Background(), 30*time.Millisecond, func(ctx context.Context) error {
		if calls.Add(1) > 1 {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	})
	select {
	case <-guard.ctx.Done():
	case <-time.After(100 * time.Millisecond):
		_ = guard.finish(nil)
		t.Fatal("blocked renewal must have its own finite deadline")
	}
	err := guard.finish(nil)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	var failure StoreFailure
	require.True(t, errors.As(err, &failure), "renewal timeout is an infrastructure failure, not a turn deadline")
}

func TestTaskLeasePreservesCallerDeadlineWithoutCancellingOtherWork(t *testing.T) {
	var calls atomic.Int32
	guard := startTaskLease(context.Background(), time.Second, func(ctx context.Context) error {
		if calls.Add(1) > 1 {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	err := guard.check(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NoError(t, guard.ctx.Err(), "turn deadline is not a Store renewal failure")
	require.NoError(t, guard.finish(nil))
}
