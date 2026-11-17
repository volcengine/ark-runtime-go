// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package memory_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	memory "github.com/volcengine/ark-runtime-go/channel/internal/testutil/memorystore"
)

func TestMemoryIngestBudget(t *testing.T) {
	factory := func(t *testing.T, c conformance.StoreConfig) conformance.TestStore {
		s, err := memory.New(memory.Options{MaxOpenTurns: c.MaxOpenTurns, Clock: c.Clock, MaxChannelOpenTurns: c.MaxChannelOpenTurns, MaxRejectedMessages: c.MaxRejectedMessages, RejectNoticeInterval: c.RejectNoticeInterval})
		require.NoError(t, err)
		return s
	}
	conformance.RunIngestBudget(t, factory)
	conformance.IngestBudgetScope(t, factory)
	conformance.IngestBudgetConcurrentChannels(t, factory)
	conformance.IngestBudgetConcurrentRejects(t, factory)
}
