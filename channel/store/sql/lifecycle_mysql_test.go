// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

//go:build mysqlintegration

package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestMySQLLifecycleSelectOnly(t *testing.T) {
	ctx := context.Background()
	db := mysqlTestDB(t)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	prefix := fmt.Sprintf("lc%d_", time.Now().UnixNano())
	options := Options{TablePrefix: prefix}
	require.NoError(t, Install(ctx, db, MySQL, options))
	var databaseName string
	require.NoError(t, db.QueryRow("SELECT DATABASE()").Scan(&databaseName))
	// 此 suite 仅针对显式配置的本地隔离 MySQL；独立只读账号证明启动零 ddl/DML。
	username := fmt.Sprintf("lc%d", time.Now().UnixNano())
	_, err := db.Exec("CREATE USER '" + username + "'@'%' IDENTIFIED BY ''")
	require.NoError(t, err)
	t.Cleanup(func() { _, err := db.Exec("DROP USER IF EXISTS '" + username + "'@'%'"); require.NoError(t, err) })
	_, err = db.Exec("GRANT SELECT ON `" + databaseName + "`.* TO '" + username + "'@'%'")
	require.NoError(t, err)
	config, err := mysql.ParseDSN(os.Getenv("CHANNEL_TEST_MYSQL_DSN"))
	require.NoError(t, err)
	config.User = username
	config.Passwd = ""
	readonly, err := sql.Open("mysql", config.FormatDSN())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, readonly.Close()) })
	_, err = open(ctx, readonly, MySQL, options)
	require.NoError(t, err)
	_, err = db.Exec("ALTER TABLE " + prefix + "channel_turns ADD COLUMN work_kind VARCHAR(32) NOT NULL DEFAULT 'future'")
	require.NoError(t, err)
	_, err = db.Exec("CREATE INDEX " + prefix + "channel_turns_work_kind ON " + prefix + "channel_turns(work_kind)")
	require.NoError(t, err)
	_, err = open(ctx, readonly, MySQL, options)
	require.NoError(t, err, "extra retired columns and indexes do not become startup dependencies")
	_, err = db.Exec("ALTER TABLE " + prefix + "channel_turns DROP COLUMN stop_requested_at")
	require.NoError(t, err)
	report, err := inspect(ctx, readonly, MySQL, options)
	require.NoError(t, err)
	require.Contains(t, report.Missing, "column:"+prefix+"channel_turns.stop_requested_at")
	_, err = open(ctx, readonly, MySQL, options)
	require.ErrorIs(t, err, errSchema)
	// 新空 namespace 同样只读拒绝，不尝试建表。
	_, err = open(ctx, readonly, MySQL, Options{TablePrefix: prefix + "e"})
	require.ErrorIs(t, err, errSchema)
}

func TestMySQLStorageWithoutWorkKind(t *testing.T) {
	ctx := context.Background()
	db := mysqlTestDB(t)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	prefix := fmt.Sprintf("kw%d_", time.Now().UnixNano())
	options := Options{TablePrefix: prefix}
	require.NoError(t, Install(ctx, db, MySQL, options))
	var count int
	require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND LEFT(table_name,?)=? AND column_name='work_kind'", len(prefix), prefix).Scan(&count))
	require.Zero(t, count)
	require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND LEFT(table_name,?)=? AND index_name IN (?,?,?)", len(prefix), prefix, prefix+"channel_turns_work_kind", prefix+"channel_deliveries_work_kind", prefix+"channel_remote_operations_work_kind").Scan(&count))
	require.Zero(t, count)
	store, err := open(ctx, db, MySQL, options)
	require.NoError(t, err)
	blocked := workKindSettle(t, store, "blocked", 2)
	allowed := workKindSettle(t, store, "allowed", 1)
	var original string
	require.NoError(t, db.QueryRow("SELECT data FROM "+prefix+"channel_deliveries WHERE id=?", blocked[0].ID).Scan(&original))
	var wire map[string]any
	require.NoError(t, json.Unmarshal([]byte(original), &wire))
	wire["body"] = map[string]any{"type": "future_body", "future": true}
	raw, err := json.Marshal(wire)
	require.NoError(t, err)
	_, err = db.Exec("UPDATE "+prefix+"channel_deliveries SET data=? WHERE id=?", string(raw), blocked[0].ID)
	require.NoError(t, err)
	claimed, err := store.LeaseDelivery(ctx, "worker", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.Equal(t, allowed[0].ID, claimed.ID)
	require.NoError(t, store.CompleteDelivery(ctx, claimed.Lease, core.DeliveryResult{Status: core.DeliveryDone}))
	claimed, err = store.LeaseDelivery(ctx, "worker", time.Minute)
	require.NoError(t, err)
	require.Nil(t, claimed, "unknown body blocks its source without leasing the next delivery")
	var after string
	require.NoError(t, db.QueryRow("SELECT data FROM "+prefix+"channel_deliveries WHERE id=?", blocked[0].ID).Scan(&after))
	require.Equal(t, string(raw), after)
}
