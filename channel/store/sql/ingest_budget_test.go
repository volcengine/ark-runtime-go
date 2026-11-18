// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
)

func TestSQLiteIngestBudget(t *testing.T) {
	factory := func(t *testing.T, c conformance.StoreConfig) conformance.TestStore {
		db := openSQLite(t, filepath.Join(t.TempDir(), "budget.db"))
		s, err := openInstalledSQLFixture(t.Context(), db, SQLite, Options{MaxOpenTurns: c.MaxOpenTurns, Clock: c.Clock, MaxChannelOpenTurns: c.MaxChannelOpenTurns, MaxRejectedMessages: c.MaxRejectedMessages, RejectNoticeInterval: c.RejectNoticeInterval})
		require.NoError(t, err)
		return s
	}
	conformance.RunIngestBudget(t, factory)
	conformance.IngestBudgetScope(t, factory)
	conformance.IngestBudgetConcurrentChannels(t, factory)
	conformance.IngestBudgetConcurrentRejects(t, factory)
}

func TestSQLiteIngestBudgetRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restart.db")
	conformance.RunIngestBudgetRestart(t, func() conformance.TestStore {
		s, err := openInstalledSQLFixture(t.Context(), openSQLite(t, path), SQLite, Options{MaxChannelOpenTurns: 1, MaxRejectedMessages: 1})
		require.NoError(t, err)
		return s
	})
}
func TestSQLiteIngestBudgetRollbackDeliveryFailure(t *testing.T) {
	db := openSQLite(t, filepath.Join(t.TempDir(), "rollback.db"))
	s, err := openInstalledSQLFixture(t.Context(), db, SQLite, Options{MaxChannelOpenTurns: 1, MaxRejectedMessages: 1})
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	_, err = s.Ingest(t.Context(), conformance.SampleVersion(), conformance.SampleMessage("open"))
	require.NoError(t, err)
	_, err = db.Exec("CREATE TRIGGER reject_budget_abort BEFORE INSERT ON channel_deliveries BEGIN SELECT RAISE(ABORT,'injected delivery failure'); END")
	require.NoError(t, err)
	_, err = s.Ingest(t.Context(), conformance.SampleVersion(), conformance.SampleMessage("reject"))
	require.ErrorContains(t, err, "injected delivery failure")
	e, err := s.Export(t.Context())
	require.NoError(t, err)
	require.Len(t, e.Messages, 1)
	require.Empty(t, e.Controls)
	require.Empty(t, e.Deliveries)
	var slots int
	require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM channel_rejected_messages").Scan(&slots))
	require.Zero(t, slots)
	_, err = db.Exec("DROP TRIGGER reject_budget_abort")
	require.NoError(t, err)
	r, err := s.Ingest(t.Context(), conformance.SampleVersion(), conformance.SampleMessage("reject"))
	require.NoError(t, err)
	require.True(t, r.Rejected)
}
