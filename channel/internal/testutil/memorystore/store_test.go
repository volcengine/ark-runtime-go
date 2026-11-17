// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package memory_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	memory "github.com/volcengine/ark-runtime-go/channel/internal/testutil/memorystore"
)

func TestMemoryBotVaultConformance(t *testing.T) {
	conformance.RunBotVaultStore(t, func(t *testing.T, config conformance.StoreConfig) conformance.TestStore {
		s, err := memory.New(memory.Options{Clock: config.Clock})
		require.NoError(t, err)
		return s
	})
}

func TestMemoryStoresAreIsolated(t *testing.T) {
	a, err := memory.New(memory.Options{})
	require.NoError(t, err)
	defer func() { require.NoError(t, a.Close()) }()
	b, err := memory.New(memory.Options{})
	require.NoError(t, err)
	defer func() { require.NoError(t, b.Close()) }()
	_, err = a.Ingest(t.Context(), conformance.SampleVersion(), conformance.SampleMessage("one"))
	require.NoError(t, err)
	stats, err := b.Stats(t.Context())
	require.NoError(t, err)
	require.Zero(t, stats.QueuedTurns)
}
