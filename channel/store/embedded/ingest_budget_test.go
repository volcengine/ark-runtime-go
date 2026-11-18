// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
)

func TestEmbeddedIngestBudget(t *testing.T) {
	factory := func(t *testing.T, c conformance.StoreConfig) conformance.TestStore {
		s, err := openInstalledFixture(filepath.Join(t.TempDir(), "budget.db"), Options{MaxOpenTurns: c.MaxOpenTurns, Clock: c.Clock, MaxChannelOpenTurns: c.MaxChannelOpenTurns, MaxRejectedMessages: c.MaxRejectedMessages, RejectNoticeInterval: c.RejectNoticeInterval, NoSync: true})
		require.NoError(t, err)
		return s
	}
	conformance.RunIngestBudget(t, factory)
	conformance.IngestBudgetScope(t, factory)
	conformance.IngestBudgetConcurrentChannels(t, factory)
	conformance.IngestBudgetConcurrentRejects(t, factory)
}

func TestEmbeddedIngestBudgetRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restart.db")
	conformance.RunIngestBudgetRestart(t, func() conformance.TestStore {
		s, err := openInstalledFixture(path, Options{MaxChannelOpenTurns: 1, MaxRejectedMessages: 1, NoSync: true})
		require.NoError(t, err)
		return s
	})
}
