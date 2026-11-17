// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package secret

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemoryResolverInjectsAndRedacts(t *testing.T) {
	r := NewMemoryResolver()
	require.NoError(t, r.Set("memory:channel/epoch/app", "sensitive-value"))
	got, err := r.Resolve("memory:channel/epoch/app")
	require.NoError(t, err)
	require.Equal(t, "sensitive-value", got)
	for _, format := range []string{"%v", "%+v", "%#v", "%s"} {
		require.NotContains(t, fmt.Sprintf(format, r), "sensitive-value")
	}
	require.NoError(t, r.Delete("memory:channel/epoch/app"))
	_, err = r.Resolve("memory:channel/epoch/app")
	require.Error(t, err)
	require.NotContains(t, err.Error(), "channel/epoch/app")
	require.Error(t, r.Set("env:OTHER", "value"))
	t.Setenv("CHANNEL_TEST_SECRET", "external")
	got, err = r.Resolve("env:CHANNEL_TEST_SECRET")
	require.NoError(t, err)
	require.Equal(t, "external", got)
}

func TestMemoryResolverConcurrentAccess(t *testing.T) {
	r := NewMemoryResolver()
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20 {
				_ = r.Set("memory:test", "secret")
				_, _ = r.Resolve("memory:test")
				_ = r.Delete("memory:test")
			}
		}()
	}
	wg.Wait()
}
