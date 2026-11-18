// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConnectionCacheBoundsAndCoalesces(t *testing.T) {
	cache, err := NewConnectionCache(1, time.Minute)
	require.NoError(t, err)
	var calls atomic.Int32
	gate := make(chan struct{})
	loader := func(context.Context) (Connection, error) {
		calls.Add(1)
		<-gate
		return Connection{Binding: Binding{ID: "one"}}, nil
	}
	var wg sync.WaitGroup
	results := make(chan Connection, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := cache.Acquire(context.Background(), "one", loader)
			if err == nil {
				results <- c
			}
		}()
	}
	close(gate)
	wg.Wait()
	require.Equal(t, int32(1), calls.Load())
	require.Len(t, results, 8)
	_, err = cache.Acquire(context.Background(), "two", loader)
	require.ErrorIs(t, err, ErrConnectionCacheFull)
	close(results)
	for c := range results {
		c.Release()
		c.Release()
	}
	c, err := cache.Acquire(context.Background(), "two", func(context.Context) (Connection, error) { return Connection{Binding: Binding{ID: "two"}}, nil })
	require.NoError(t, err)
	c.Release()
}

func TestConnectionCacheExpiresSameIdleKey(t *testing.T) {
	c, err := NewConnectionCache(1, time.Minute)
	require.NoError(t, err)
	calls := 0
	loader := func(context.Context) (Connection, error) { calls++; return Connection{}, nil }
	first, err := c.Acquire(context.Background(), "key", loader)
	require.NoError(t, err)
	first.Release()
	c.mu.Lock()
	c.entries["key"].idleAt = time.Now().Add(-2 * time.Minute)
	c.mu.Unlock()
	second, err := c.Acquire(context.Background(), "key", loader)
	require.NoError(t, err)
	second.Release()
	require.Equal(t, 2, calls)
}
