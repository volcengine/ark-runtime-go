// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

//go:build mysqlintegration

package sqlstore

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestMySQLIngestBudget(t *testing.T) {
	factory := func(t *testing.T, c conformance.StoreConfig) conformance.TestStore {
		prefix := fmt.Sprintf("ib%d_%d_", time.Now().UnixNano(), mysqlTestCounter.Add(1))
		s, err := openInstalledSQLFixture(context.Background(), mysqlTestDB(t), MySQL, Options{TablePrefix: prefix, Clock: c.Clock, MaxOpenTurns: c.MaxOpenTurns, MaxChannelOpenTurns: c.MaxChannelOpenTurns, MaxRejectedMessages: c.MaxRejectedMessages, RejectNoticeInterval: c.RejectNoticeInterval})
		require.NoError(t, err)
		return s
	}
	conformance.RunIngestBudget(t, factory)
	conformance.IngestBudgetScope(t, factory)
	conformance.IngestBudgetConcurrentChannels(t, factory)
	conformance.IngestBudgetConcurrentRejects(t, factory)
}

func TestMySQLIngestBudgetPruneSerializesWithAdmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("ip%d_", time.Now().UnixNano())
	db := mysqlTestDB(t)
	s, err := openInstalledSQLFixture(ctx, db, MySQL, Options{TablePrefix: prefix, MaxOpenTurns: 1, MaxRejectedMessages: 2})
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	_, err = s.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("open"))
	require.NoError(t, err)
	r, err := s.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("reject"))
	require.NoError(t, err)
	require.True(t, r.Rejected)
	d, err := s.LeaseDelivery(ctx, "sender", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, d)
	require.NoError(t, s.CompleteDelivery(ctx, d.Lease, core.DeliveryResult{Status: core.DeliveryDone}))
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var id string
	require.NoError(t, tx.QueryRowContext(ctx, "SELECT id FROM "+prefix+"channel_ingest_locks WHERE id=? FOR UPDATE", core.IngestChannelScope(conformance.SampleBinding())).Scan(&id))
	done := make(chan error, 1)
	go func() { _, err := s.Prune(ctx, time.Now().Add(time.Minute)); done <- err }()
	select {
	case err := <-done:
		require.NoError(t, err)
		t.Fatal("prune passed held admission scope lock")
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, tx.Commit())
	require.NoError(t, <-done)
}

func TestMySQLIngestBudgetRestart(t *testing.T) {
	prefix := fmt.Sprintf("ir%d_", time.Now().UnixNano())
	conformance.RunIngestBudgetRestart(t, func() conformance.TestStore {
		s, err := openInstalledSQLFixture(t.Context(), mysqlTestDB(t), MySQL, Options{TablePrefix: prefix, MaxChannelOpenTurns: 1, MaxRejectedMessages: 1})
		require.NoError(t, err)
		return s
	})
}
