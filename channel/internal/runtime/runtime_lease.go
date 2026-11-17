// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// taskLease 在任务整个生命周期内续租；fence 丢失或存储故障只取消对应任务上下文。
// 外部写前再次续租，避免阻塞的 resolver/只读调用返回后沿用旧 fence 发起新副作用。
type taskLease struct {
	ctx          context.Context
	cancel       context.CancelCauseFunc
	renew        func(context.Context) error
	mu           sync.Mutex
	err          error
	done         chan struct{}
	finishing    atomic.Bool
	renewTimeout time.Duration
}

func startTaskLease(parent context.Context, ttl time.Duration, renew func(context.Context) error) *taskLease {
	ctx, cancel := context.WithCancelCause(parent)
	interval := ttl / 3
	if interval <= 0 {
		interval = time.Nanosecond
	}
	g := &taskLease{ctx: ctx, cancel: cancel, renew: renew, done: make(chan struct{}), renewTimeout: interval}
	if g.check(ctx) != nil {
		close(g.done)
		return g
	}
	go func() {
		defer close(g.done)
		defer func() {
			if recover() != nil {
				g.fail(ErrRuntimeConfig)
			}
		}()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if g.check(ctx) != nil {
					return
				}
			}
		}
	}()
	return g
}
func (g *taskLease) fail(err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.err == nil {
		g.err = err
		g.cancel(err)
	}
}
func (g *taskLease) check(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.err != nil {
		return g.err
	}
	if g.ctx.Err() != nil {
		return context.Cause(g.ctx)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	renewCtx, cancel := context.WithTimeout(ctx, g.renewTimeout)
	defer cancel()
	if err := g.renew(renewCtx); err != nil {
		if ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			return ctx.Err()
		}
		if g.finishing.Load() && errors.Is(err, context.Canceled) {
			return err
		}
		if !errors.Is(err, ErrLeaseLost) && !isUnsupportedWork(err) {
			err = NewStoreFailure(err)
		}
		g.err = err
		g.cancel(err)
		return err
	}
	return nil
}
func (g *taskLease) finish(err error) error {
	g.finishing.Store(true)
	g.cancel(context.Canceled)
	<-g.done
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.err != nil {
		return g.err
	}
	return err
}
func (j *job) checkLease(ctx context.Context) error {
	if j.guard == nil {
		return ctx.Err()
	}
	return j.guard.check(ctx)
}
func (j *job) workContext() context.Context {
	if j.guard == nil {
		return context.Background()
	}
	return j.guard.ctx
}

func (r *Runtime) runLeasedTurn(ctx context.Context, j *job) error {
	if err := turnWorkError(j.Turn); err != nil {
		return err
	}
	lease := j.Lease
	j.guard = startTaskLease(ctx, r.options.LeaseTTL, func(callCtx context.Context) error {
		_, err := r.store.RenewTurn(callCtx, lease, r.options.LeaseTTL)
		return err
	})
	if j.guard.ctx.Err() != nil {
		return j.guard.finish(context.Cause(j.guard.ctx))
	}
	return j.guard.finish(r.processTurn(j.guard.ctx, j))
}
func (r *Runtime) runLeasedDelivery(ctx context.Context, delivery Delivery) error {
	if err := deliveryWorkError(delivery); err != nil {
		return err
	}
	guard := startTaskLease(ctx, r.options.LeaseTTL, func(callCtx context.Context) error {
		_, err := r.store.RenewDelivery(callCtx, delivery.Lease, r.options.LeaseTTL)
		return err
	})
	if guard.ctx.Err() != nil {
		return guard.finish(context.Cause(guard.ctx))
	}
	return guard.finish(r.processDelivery(context.WithValue(guard.ctx, deliveryLeaseKey{}, guard), delivery))
}

type deliveryLeaseKey struct{}

func checkDeliveryLease(ctx context.Context) error {
	if guard, ok := ctx.Value(deliveryLeaseKey{}).(*taskLease); ok {
		return guard.check(ctx)
	}
	return ctx.Err()
}
