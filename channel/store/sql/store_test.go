// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func openSQLite(t *testing.T, path string) *sql.DB {
	t.Helper()
	// 临时测试库验证事务和重开语义；WAL NORMAL 避免每次提交 fsync 耗尽 CI 时间。
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_txlock=immediate", path))
	require.NoError(t, err)
	return db
}

func TestSQLiteStoreConformance(t *testing.T) {
	factory := func(t *testing.T, config conformance.StoreConfig) conformance.TestStore {
		db := openSQLite(t, filepath.Join(t.TempDir(), "store.db"))
		s, err := openInstalledSQLFixture(context.Background(), db, SQLite, Options{Clock: config.Clock, MaxOpenTurns: config.MaxOpenTurns, MaxMessageBytes: config.MaxMessageBytes, QueueTTL: config.QueueTTL})
		require.NoError(t, err)
		return s
	}
	conformance.RunStore(t, factory)
}

func TestSQLiteBotVaultConformance(t *testing.T) {
	conformance.RunBotVaultStore(t, func(t *testing.T, config conformance.StoreConfig) conformance.TestStore {
		db := openSQLite(t, filepath.Join(t.TempDir(), "vault.db"))
		s, err := openInstalledSQLFixture(context.Background(), db, SQLite, Options{Clock: config.Clock})
		require.NoError(t, err)
		return s
	})
}

func TestSQLiteStoreSharedAcrossInstances(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "shared.db")
	a, err := openInstalledSQLFixture(ctx, openSQLite(t, path), SQLite, Options{})
	require.NoError(t, err)
	defer func() { require.NoError(t, a.Close()) }()
	b, err := openInstalledSQLFixture(ctx, openSQLite(t, path), SQLite, Options{})
	require.NoError(t, err)
	defer func() { require.NoError(t, b.Close()) }()
	first, err := a.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("one"))
	require.NoError(t, err)
	again, err := b.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("one"))
	require.NoError(t, err)
	require.True(t, again.Duplicate, "dedupe is shared across instances")
	require.Equal(t, first.Turn.ID, again.Turn.ID)
	_, err = b.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("two"))
	require.NoError(t, err)
	// 并发领取：同一会话只能有一个实例拿到队首。
	results := make(chan *core.TurnLease, 8)
	for i := 0; i < 8; i++ {
		store := a
		if i%2 == 1 {
			store = b
		}
		go func(i int) {
			leased, err := store.LeaseTurn(ctx, conformance.Claim(fmt.Sprintf("worker-%d", i)))
			require.NoError(t, err)
			results <- leased
		}(i)
	}
	claimed := 0
	for i := 0; i < 8; i++ {
		if leased := <-results; leased != nil {
			claimed++
			require.Equal(t, first.Turn.ID, leased.Turn.ID)
		}
	}
	require.Equal(t, 1, claimed, "only one instance leases the conversation head")
}

func TestSQLiteRejectsIncompleteStructureWithoutChanges(t *testing.T) {
	path := filepath.Join(t.TempDir(), "schema.db")
	db := openSQLite(t, path)
	// 不完整结构只有元数据与无关表。
	_, err := db.Exec("CREATE TABLE channel_meta (k VARCHAR(64) NOT NULL PRIMARY KEY, v VARCHAR(64) NOT NULL)")
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE channel_tasks (id TEXT PRIMARY KEY)")
	require.NoError(t, err)
	require.NoError(t, db.Close())
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	_, err = openInstalledSQLFixture(context.Background(), openSQLite(t, path), SQLite, Options{})
	require.ErrorIs(t, err, errSchema)
	check := openSQLite(t, path)
	defer func() { require.NoError(t, check.Close()) }()
	var tables int
	require.NoError(t, check.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'channel_turns'").Scan(&tables))
	require.Zero(t, tables, "an old database is not modified")
	require.NotEmpty(t, before)
	_, err = openInstalledSQLFixture(context.Background(), openSQLite(t, filepath.Join(t.TempDir(), "x.db")), SQLite, Options{TablePrefix: "bad-prefix"})
	require.ErrorIs(t, err, core.ErrInvalid)
}

func TestTablePrefixIsolatesDeployments(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "prefix.db")
	a, err := openInstalledSQLFixture(ctx, openSQLite(t, path), SQLite, Options{TablePrefix: "a_"})
	require.NoError(t, err)
	defer func() { require.NoError(t, a.Close()) }()
	b, err := openInstalledSQLFixture(ctx, openSQLite(t, path), SQLite, Options{TablePrefix: "b_"})
	require.NoError(t, err)
	defer func() { require.NoError(t, b.Close()) }()
	_, err = a.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("one"))
	require.NoError(t, err)
	stats, err := b.Stats(ctx)
	require.NoError(t, err)
	require.Zero(t, stats.QueuedTurns)
}

func TestReadyQueueIgnoresHistory(t *testing.T) {
	// 领取只查询队首索引：大量已结算历史不影响结果。
	ctx := context.Background()
	clock := conformance.NewClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	s, err := openInstalledSQLFixture(ctx, openSQLite(t, filepath.Join(t.TempDir(), "q.db")), SQLite, Options{Clock: clock.Now})
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	for i := 0; i < 50; i++ {
		_, err := s.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage(fmt.Sprint(i)))
		require.NoError(t, err)
		leased, err := s.LeaseTurn(ctx, conformance.Claim("w"))
		require.NoError(t, err)
		require.NoError(t, s.SettleTurn(ctx, leased.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, nil))
	}
	none, err := s.LeaseTurn(ctx, conformance.Claim("w"))
	require.NoError(t, err)
	require.Nil(t, none)
}

func TestMySQLDDLUsesRowLockAndLongText(t *testing.T) {
	ddl := ddl(MySQL, "x_")
	joined := fmt.Sprint(ddl)
	require.Contains(t, joined, "LONGTEXT")
	require.Contains(t, joined, "x_channel_turns")
	require.Contains(t, joined, "x_channel_interactions")
	require.Equal(t, " FOR UPDATE", MySQL.RowLockSuffix)
	require.Equal(t, " SKIP LOCKED", MySQL.SkipLockedSuffix)
}

func TestSQLiteOpenRejectsEmptyDB(t *testing.T) {
	db := openSQLite(t, filepath.Join(t.TempDir(), "empty.db"))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err := open(context.Background(), db, SQLite, Options{})
	require.ErrorIs(t, err, errSchema)
}
