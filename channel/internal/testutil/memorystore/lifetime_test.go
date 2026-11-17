// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
)

func lifetimeDatabase(t *testing.T) (*sql.DB, string) {
	t.Helper()
	name := fmt.Sprintf("file:memory-lifetime-%d?mode=memory&cache=shared&_txlock=immediate&_pragma=busy_timeout(5000)", time.Now().UnixNano())
	db, err := sql.Open("sqlite", name)
	require.NoError(t, err)
	return db, name
}

func TestWorkingConnectionReplacementRetainsMemoryStoreUntilClose(t *testing.T) {
	ctx := context.Background()
	db, name := lifetimeDatabase(t)
	store, err := openMemory(db, name, Options{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	first, err := store.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("retained"))
	require.NoError(t, err)
	connection, err := db.Conn(ctx)
	require.NoError(t, err)
	require.ErrorIs(t, connection.Raw(func(any) error { return driver.ErrBadConn }), driver.ErrBadConn)
	_ = connection.Close() // ErrBadConn已使sql.Conn不可再次使用。
	saved, err := store.GetTurn(ctx, first.Turn.ID)
	require.NoError(t, err, "replacing the only working connection must not destroy metadata")
	require.NotNil(t, saved)
	require.Equal(t, first.Turn.ID, saved.ID)
	replay, err := store.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("retained"))
	require.NoError(t, err)
	require.True(t, replay.Duplicate)
	require.NoError(t, store.Close())
	require.NoError(t, store.Close())
	require.Error(t, db.PingContext(ctx))
	require.Error(t, store.keeperDB.PingContext(ctx))
	require.ErrorIs(t, store.keeper.Raw(func(any) error { return nil }), sql.ErrConnDone)
	probe, err := sql.Open("sqlite", name)
	require.NoError(t, err)
	defer func() { require.NoError(t, probe.Close()) }()
	var tables int
	require.NoError(t, probe.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type='table'").Scan(&tables))
	require.Zero(t, tables, "Close must close the keeper as well and destroy the in-memory database")
}

func TestFailedMemoryConstructionClosesEveryConnection(t *testing.T) {
	ctx := context.Background()
	db, name := lifetimeDatabase(t)
	_, err := db.ExecContext(ctx, "CREATE TABLE lifetime_marker (id INTEGER)")
	require.NoError(t, err)
	store, err := openMemory(db, name, Options{MaxOpenTurns: -1})
	require.Error(t, err)
	require.Nil(t, store)
	require.Error(t, db.PingContext(ctx))
	probe, err := sql.Open("sqlite", name)
	require.NoError(t, err)
	defer func() { require.NoError(t, probe.Close()) }()
	var tables int
	require.NoError(t, probe.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type='table'").Scan(&tables))
	require.Zero(t, tables, "failed construction must not leave a keeper retaining the database")
}
