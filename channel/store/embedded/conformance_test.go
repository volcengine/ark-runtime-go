// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
)

func TestEmbeddedStoreConformance(t *testing.T) {
	factory := func(t *testing.T, config conformance.StoreConfig) conformance.TestStore {
		s, err := openInstalledFixture(filepath.Join(t.TempDir(), "store.db"), Options{Clock: config.Clock, MaxOpenTurns: config.MaxOpenTurns, MaxMessageBytes: config.MaxMessageBytes, QueueTTL: config.QueueTTL, NoSync: true})
		require.NoError(t, err)
		return s
	}
	conformance.RunStore(t, factory)
}

func TestEmbeddedBotVaultConformance(t *testing.T) {
	conformance.RunBotVaultStore(t, func(t *testing.T, config conformance.StoreConfig) conformance.TestStore {
		s, err := openInstalledFixture(filepath.Join(t.TempDir(), "vault.db"), Options{Clock: config.Clock, NoSync: true})
		require.NoError(t, err)
		return s
	})
}
