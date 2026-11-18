// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/payload"
)

func lifecycleDB(t *testing.T) *sql.DB {
	t.Helper()
	db := openSQLite(t, filepath.Join(t.TempDir(), "lifecycle.db"))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

func lifecycleSchema(t *testing.T, db *sql.DB) {
	t.Helper()
	require.NoError(t, Install(context.Background(), db, SQLite, Options{}))
}

func TestLifecycleOpenInstalledStoreIsReadOnly(t *testing.T) {
	db := lifecycleDB(t)
	lifecycleSchema(t, db)
	_, err := db.Exec("PRAGMA query_only=ON")
	require.NoError(t, err)
	_, err = open(context.Background(), db, SQLite, Options{})
	require.NoError(t, err)
}

func TestLifecycleOpenEmptyAndIncompleteAreReadOnly(t *testing.T) {
	for _, installed := range []bool{false, true} {
		t.Run(fmt.Sprint(installed), func(t *testing.T) {
			db := lifecycleDB(t)
			if installed {
				lifecycleSchema(t, db)
				_, err := db.Exec("ALTER TABLE channel_turns DROP COLUMN stop_requested_at")
				require.NoError(t, err)
			}
			var before int
			require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM sqlite_master").Scan(&before))
			_, err := db.Exec("PRAGMA query_only=ON")
			require.NoError(t, err)
			_, err = open(context.Background(), db, SQLite, Options{})
			require.ErrorIs(t, err, errSchema)
			var after int
			require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM sqlite_master").Scan(&after))
			require.Equal(t, before, after)
		})
	}
}

func TestLifecycleInstallAndExplicitMetadataInitialization(t *testing.T) {
	ctx := context.Background()
	db := lifecycleDB(t)
	require.NoError(t, Install(ctx, db, SQLite, Options{}))
	report, err := inspect(ctx, db, SQLite, Options{})
	require.NoError(t, err)
	require.True(t, report.Ready)
	_, err = db.Exec("DELETE FROM channel_meta WHERE k='payload_mode'")
	require.NoError(t, err)
	initialized, err := initializeInstalled(ctx, db, SQLite, Options{RequirePayloadEncryption: true, PayloadNamespace: "owner/project"})
	require.NoError(t, err)
	require.True(t, initialized.Ready)
	require.Contains(t, initialized.PayloadMode, "enc-v1:")
	_, err = open(ctx, db, SQLite, Options{})
	require.Error(t, err, "an encrypted database cannot be reopened as plaintext")
	mismatch, err := inspect(ctx, db, SQLite, Options{RequirePayloadEncryption: true, PayloadNamespace: "other"})
	require.NoError(t, err)
	require.False(t, mismatch.Ready)
	require.False(t, mismatch.NamespaceMatches)
}

func TestLifecycleInspectReportsActualMissingIndexAndColumn(t *testing.T) {
	ctx := context.Background()
	db := lifecycleDB(t)
	lifecycleSchema(t, db)
	_, err := db.Exec("DROP INDEX channel_turns_ready")
	require.NoError(t, err)
	_, err = db.Exec("ALTER TABLE channel_turns DROP COLUMN stop_requested_at")
	require.NoError(t, err)
	_, err = db.Exec("PRAGMA query_only=ON")
	require.NoError(t, err)
	report, err := inspect(ctx, db, SQLite, Options{})
	require.NoError(t, err)
	require.False(t, report.Ready)
	require.Contains(t, report.Missing, "column:channel_turns.stop_requested_at")
	require.Contains(t, report.Missing, "index:channel_turns(queue_head,eligible_at,enqueue_order)")
}

func TestLifecycleIncompleteMetadataIsReportedWithoutWrites(t *testing.T) {
	db := lifecycleDB(t)
	lifecycleSchema(t, db)
	_, err := db.Exec("ALTER TABLE channel_meta DROP COLUMN v")
	require.NoError(t, err)
	_, err = db.Exec("PRAGMA query_only=ON")
	require.NoError(t, err)
	report, err := inspect(context.Background(), db, SQLite, Options{})
	require.NoError(t, err)
	require.False(t, report.Ready)
	require.Contains(t, report.Missing, "column:channel_meta.v")
}

func TestLifecycleInitializeExistingPolicyIsIdempotent(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprint(encrypted), func(t *testing.T) {
			ctx := context.Background()
			db := lifecycleDB(t)
			codec := newPayloadTestCodec(t)
			options := Options{}
			if encrypted {
				options.PayloadCodec = codec
				options.PayloadNamespace = "idempotent"
				options.RequirePayloadEncryption = true
			}
			require.NoError(t, Install(ctx, db, SQLite, options))
			store, err := open(ctx, db, SQLite, options)
			require.NoError(t, err)
			_, err = store.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("existing"))
			require.NoError(t, err)
			seals, opens := codec.seals.Load(), codec.opens.Load()
			report, err := initializeInstalled(ctx, db, SQLite, options)
			require.NoError(t, err)
			require.True(t, report.Ready)
			require.Equal(t, seals, codec.seals.Load())
			require.Equal(t, opens, codec.opens.Load())
		})
	}
}

func TestLifecycleOpenChecksEncryptionPolicyWithoutReadingPayloads(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"missing", "incomplete", "wrong", "ready"} {
		t.Run(mode, func(t *testing.T) {
			db := lifecycleDB(t)
			lifecycleSchema(t, db)
			codec := newPayloadTestCodec(t)
			options := Options{PayloadCodec: codec, PayloadNamespace: "lifecycle", RequirePayloadEncryption: true}
			var err error
			if mode != "missing" {
				_, err = initializeInstalled(ctx, db, SQLite, options)
				require.NoError(t, err)
				switch mode {
				case "incomplete":
					_, err = db.Exec("UPDATE channel_meta SET v='enc-v1:' WHERE k='payload_mode'")
				case "wrong":
					_, err = db.Exec("UPDATE channel_meta SET v='enc-v1:other' WHERE k='payload_mode'")
				}
				require.NoError(t, err)
			}
			// 无法解码的记录在业务读取时失败；open 只检查策略。
			_, err = db.Exec("INSERT INTO channel_sensitive_payloads(id,data) VALUES ('bad','not-json')")
			require.NoError(t, err)
			_, err = db.Exec("PRAGMA query_only=ON")
			require.NoError(t, err)
			_, err = open(ctx, db, SQLite, options)
			if mode == "ready" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Zero(t, codec.seals.Load())
			require.Zero(t, codec.opens.Load())
		})
	}
}

func TestLifecycleOpenWithoutWorkKindOrPerformanceIndexes(t *testing.T) {
	ctx := context.Background()
	db := lifecycleDB(t)
	for _, statement := range ddl(SQLite, "") {
		if strings.Contains(statement, "_work_kind ON ") {
			continue
		}
		statement = strings.ReplaceAll(statement, "work_kind VARCHAR(32) NOT NULL DEFAULT 'legacy', ", "")
		_, err := db.Exec(statement)
		require.NoError(t, err)
	}
	_, err := db.Exec("PRAGMA query_only=ON")
	require.NoError(t, err)
	report, err := inspect(ctx, db, SQLite, Options{})
	require.NoError(t, err)
	require.True(t, report.Ready, report.Missing)
	_, err = open(ctx, db, SQLite, Options{})
	require.NoError(t, err)
}

func TestLifecycleOpenAcceptsExtraRetiredColumnAndIndex(t *testing.T) {
	ctx := context.Background()
	db := lifecycleDB(t)
	for _, statement := range ddl(SQLite, "") {
		if strings.Contains(statement, "_work_kind ON ") {
			continue
		}
		statement = strings.ReplaceAll(statement, "work_kind VARCHAR(32) NOT NULL DEFAULT 'legacy', ", "")
		_, err := db.Exec(statement)
		require.NoError(t, err)
	}
	_, err := db.Exec("ALTER TABLE channel_turns ADD COLUMN work_kind VARCHAR(32) NOT NULL DEFAULT 'future'")
	require.NoError(t, err)
	_, err = db.Exec("CREATE INDEX channel_turns_work_kind ON channel_turns(work_kind)")
	require.NoError(t, err)
	report, err := inspect(ctx, db, SQLite, Options{})
	require.NoError(t, err)
	require.True(t, report.Ready, report.Missing)
	_, err = open(ctx, db, SQLite, Options{})
	require.NoError(t, err)
}

func TestLifecycleInitializeRejectsNonemptyPayloadNamespace(t *testing.T) {
	ctx := context.Background()
	db := lifecycleDB(t)
	lifecycleSchema(t, db)
	_, err := db.Exec("INSERT INTO channel_sensitive_payloads(id,data) VALUES ('orphan','{}')")
	require.NoError(t, err)
	_, err = initializeInstalled(ctx, db, SQLite, Options{RequirePayloadEncryption: true, PayloadNamespace: "deployment"})
	require.Error(t, err, "initialization is only for an empty namespace, including retained payloads")
	var mode string
	require.NoError(t, db.QueryRow("SELECT v FROM channel_meta WHERE k='payload_mode'").Scan(&mode))
	require.Empty(t, mode)
}

func TestLifecycleInstallCreatesUsablePlainAndEncryptedStore(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprint(encrypted), func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "installed.db")
			db := openSQLite(t, path)
			options := Options{}
			if encrypted {
				options.PayloadCodec = newPayloadTestCodec(t)
				options.PayloadNamespace = "fresh-install"
				options.RequirePayloadEncryption = true
			}
			require.NoError(t, Install(ctx, db, SQLite, options))
			s, err := open(ctx, db, SQLite, options)
			require.NoError(t, err)
			_, err = s.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("fresh"))
			require.NoError(t, err)
			require.NoError(t, s.Close())
			reopened, err := open(ctx, openSQLite(t, path), SQLite, options)
			require.NoError(t, err)
			require.NoError(t, reopened.Close())
		})
	}
}

func TestLifecycleInstallChecksExistingNamespace(t *testing.T) {
	for _, test := range []struct {
		name   string
		schema string
		want   error
	}{
		{name: "partial empty installation", schema: "CREATE TABLE channel_meta(k VARCHAR(64) NOT NULL PRIMARY KEY, v VARCHAR(64) NOT NULL)"},
		{name: "incomplete known table", schema: "CREATE TABLE channel_meta(k VARCHAR(64) NOT NULL PRIMARY KEY)", want: errSchema},
		{name: "unknown table", schema: "CREATE TABLE channel_unknown(id INTEGER)", want: errSchema},
		{name: "nonempty installation", schema: "CREATE TABLE channel_meta(k VARCHAR(64) NOT NULL PRIMARY KEY, v VARCHAR(64) NOT NULL); INSERT INTO channel_meta(k,v) VALUES ('existing','value')", want: core.ErrConflict},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			db := lifecycleDB(t)
			_, err := db.Exec(test.schema)
			require.NoError(t, err)
			err = Install(ctx, db, SQLite, Options{})
			if test.want != nil {
				require.ErrorIs(t, err, test.want)
				return
			}
			require.NoError(t, err)
			report, err := inspect(ctx, db, SQLite, Options{})
			require.NoError(t, err)
			require.True(t, report.Ready, report.Missing)
		})
	}
}

func TestLifecycleInitializationRejectsIncompleteStructureAndWrongPolicy(t *testing.T) {
	for _, test := range []struct {
		name   string
		change string
		want   error
	}{
		{name: "incomplete structure", change: "ALTER TABLE channel_turns DROP COLUMN stop_requested_at", want: errSchema},
		{name: "wrong namespace", change: "UPDATE channel_meta SET v='enc-v1:other' WHERE k='payload_mode'", want: payload.ErrInvalid},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			db := lifecycleDB(t)
			options := Options{PayloadCodec: newPayloadTestCodec(t), PayloadNamespace: "deployment", RequirePayloadEncryption: true}
			require.NoError(t, Install(ctx, db, SQLite, options))
			_, err := db.Exec(test.change)
			require.NoError(t, err)
			_, err = initializeInstalled(ctx, db, SQLite, options)
			require.ErrorIs(t, err, test.want)
		})
	}
}
