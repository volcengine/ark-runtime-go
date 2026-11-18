// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

func (o RuntimeOptions) staged() bool {
	return o.CreateConcurrency != 0 || o.SubmitConcurrency != 0 || o.ObserveConcurrency != 0
}

func (o *RuntimeOptions) normalizePools() error {
	if !o.staged() {
		return nil
	}
	if o.Workers != 0 || o.AccountLimit != 0 || o.CreationLimit != 0 {
		return ErrRuntimeConfig
	}
	for _, f := range []struct {
		value             *int
		fallback, maximum int
	}{
		{&o.CreateConcurrency, 8, 128}, {&o.SubmitConcurrency, 32, 256}, {&o.ObserveConcurrency, 256, 4096},
	} {
		if *f.value < 0 || *f.value > f.maximum {
			return ErrRuntimeConfig
		}
		if *f.value == 0 {
			*f.value = f.fallback
		}
	}
	return nil
}

// dispatchTurns 只有一个调度器扫描当前池；有名额才领取，不在内存中积压已受理任务。
func (r *Runtime) dispatchTurns(parent context.Context, stage TurnStage, limit int, execute func(context.Context, *TurnLease, TurnStage) error) error {
	ctx, cancel := context.WithCancel(parent)
	var wg sync.WaitGroup
	defer func() { cancel(); wg.Wait() }()
	results := make(chan error, limit)
	active := 0
	sequence := uint64(0)
	for ctx.Err() == nil {
		// 优先传播已完成任务的故障，避免持续有新任务时饿死错误处理。
		select {
		case err := <-results:
			active--
			if err != nil {
				return err
			}
			continue
		default:
		}
		if active == limit {
			select {
			case <-ctx.Done():
				return nil
			case err := <-results:
				active--
				if err != nil {
					return err
				}
			}
			continue
		}
		sequence++
		claim := ClaimOptions{Stage: stage, UserAuth: r.supportsUserAuth(), Owner: fmt.Sprintf("%s-%s-%d", stage, r.instanceID, sequence), TTL: r.options.LeaseTTL, MessageDeadline: r.options.MessageDeadline}
		leased, err := r.store.LeaseTurn(ctx, claim)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if !isUnsupportedWork(err) && !errors.Is(err, ErrLeaseLost) {
				return err
			}
		}
		if leased == nil || err != nil {
			timer := time.NewTimer(r.options.PollInterval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil
			case result := <-results:
				active--
				if result != nil {
					timer.Stop()
					return result
				}
			case <-timer.C:
			}
			timer.Stop()
			continue
		}
		active++
		wg.Add(1)
		go func() {
			var result error
			defer wg.Done()
			defer func() {
				if recover() != nil {
					result = fmt.Errorf("%s task panic", stage)
				}
				results <- result
			}()
			result = execute(ctx, leased, stage)
		}()
	}
	return nil
}

// releaseStage 在下一阶段已持久化后归还租约，下一执行池重新领取并建立新 fence。
func (r *Runtime) releaseStage(ctx context.Context, j *job) error {
	return r.store.ReleaseTurn(ctx, j.Lease)
}

// trySubmit 与授权恢复续跑共享短操作预算；没有名额时不登记远端写入意图。
func (r *Runtime) trySubmit(ctx context.Context) (func(), bool) {
	if ctx.Err() != nil {
		return nil, false
	}
	if r.submitSlots == nil {
		return func() {}, true
	}
	select {
	case r.submitSlots <- struct{}{}:
		return func() { <-r.submitSlots }, true
	default:
		return nil, false
	}
}
