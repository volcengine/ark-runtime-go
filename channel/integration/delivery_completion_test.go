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

type completionStore struct {
	core.Store
	complete func(context.Context, core.Lease, core.DeliveryResult) error
}

func (s completionStore) CompleteDelivery(ctx context.Context, lease core.Lease, result core.DeliveryResult) error {
	return s.complete(ctx, lease, result)
}

func TestRuntimeRetriesDeliveryCompletionWithoutResending(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(map[bool]string{false: "timeout_before_commit", true: "timeout_after_commit"}[committed], func(t *testing.T) {
			backing, err := newEmbeddedStore(t)
			require.NoError(t, err)
			var attempts atomic.Int32
			leases := make(chan core.Lease, 2)
			firstExpired := make(chan struct{})
			store := completionStore{Store: backing, complete: func(ctx context.Context, lease core.Lease, result core.DeliveryResult) error {
				leases <- lease
				if attempts.Add(1) == 1 {
					if committed {
						if err := backing.CompleteDelivery(ctx, lease, result); err != nil {
							return err
						}
					}
					<-ctx.Done()
					close(firstExpired)
					return ctx.Err()
				}
				return backing.CompleteDelivery(ctx, lease, result)
			}}
			provider := &runtimeProvider{}
			options := testRuntimeOptions()
			options.DeliveryTimeout = 40 * time.Millisecond
			runtime := mustRuntime(t, store, runtimeBinding(), newMAForHarness(t, &conformance.MAServer{}), provider, options)
			message := runtimeMessage()
			message.Text = "/new"
			mustEnqueue(t, runtime, "binding", message)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- runtime.Run(ctx) }()
			defer func() { cancel(); require.NoError(t, <-done) }()
			if committed {
				select {
				case <-firstExpired:
				case <-time.After(2 * time.Second):
					t.Fatal("completion did not exhaust its finite operation budget")
				}
				// The committed terminal state makes renewal report ErrLeaseLost;
				// the original fence must not trigger another provider call.
				require.Never(t, func() bool { return len(done) != 0 }, 100*time.Millisecond, 5*time.Millisecond)
				require.EqualValues(t, 1, attempts.Load())
			} else {
				require.Eventually(t, func() bool { return attempts.Load() == 2 }, 2*time.Second, 5*time.Millisecond)
				first, second := <-leases, <-leases
				require.Equal(t, first.ID, second.ID)
				require.Equal(t, first.Fence, second.Fence, "completion retries must retain the original lease")
			}
			require.Eventually(t, func() bool {
				deliveries, err := backing.ListDeliveries(context.Background(), core.DeliveryQuery{})
				return err == nil && len(deliveries) == 1 && deliveries[0].Status == core.DeliveryDone
			}, time.Second, 5*time.Millisecond)
			require.Equal(t, 1, provider.count())
			select {
			case err := <-done:
				done <- err
				t.Fatalf("recoverable completion timeout stopped runtime: %v", err)
			default:
			}
		})
	}
}

type cancelAfterDeliveryProvider struct {
	*runtimeProvider
	cancel context.CancelFunc
}

func (p cancelAfterDeliveryProvider) Deliver(ctx context.Context, delivery core.Delivery) (string, error) {
	id, err := p.runtimeProvider.Deliver(ctx, delivery)
	p.cancel()
	return id, err
}

func TestRuntimeCompletesSuccessfulDeliveryDuringShutdown(t *testing.T) {
	backing, err := newEmbeddedStore(t)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var completed atomic.Bool
	store := completionStore{Store: backing, complete: func(callCtx context.Context, lease core.Lease, result core.DeliveryResult) error {
		if callCtx.Err() != nil {
			return callCtx.Err()
		}
		if _, ok := callCtx.Deadline(); !ok {
			return errors.New("completion must have a finite operation budget")
		}
		err := backing.CompleteDelivery(callCtx, lease, result)
		completed.Store(err == nil)
		return err
	}}
	provider := cancelAfterDeliveryProvider{runtimeProvider: &runtimeProvider{}, cancel: cancel}
	runtime := mustRuntime(t, store, runtimeBinding(), newMAForHarness(t, &conformance.MAServer{}), provider, testRuntimeOptions())
	message := runtimeMessage()
	message.Text = "/new"
	mustEnqueue(t, runtime, "binding", message)
	require.NoError(t, runtime.Run(ctx))
	require.True(t, completed.Load(), "successful provider result must have one independent completion opportunity")
}

func TestRuntimeCompletionPreservesStoreFailures(t *testing.T) {
	for _, failure := range []error{core.NewStoreFailure(context.DeadlineExceeded), context.DeadlineExceeded} {
		t.Run(failure.Error(), func(t *testing.T) {
			backing, err := newEmbeddedStore(t)
			require.NoError(t, err)
			store := completionStore{Store: backing, complete: func(context.Context, core.Lease, core.DeliveryResult) error { return failure }}
			runtime := mustRuntime(t, store, runtimeBinding(), newMAForHarness(t, &conformance.MAServer{}), &runtimeProvider{}, testRuntimeOptions())
			message := runtimeMessage()
			message.Text = "/new"
			mustEnqueue(t, runtime, "binding", message)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			require.ErrorIs(t, runtime.Run(ctx), failure, "only the completion call's own deadline is recoverable")
		})
	}
}
