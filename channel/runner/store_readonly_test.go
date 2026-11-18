// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package runner

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/volcengine/ark-runtime-go/channel/internal/storage"

	"github.com/stretchr/testify/require"
)

func TestRunDoesNotCreateMissingStorage(t *testing.T) {
	for _, driver := range []string{"embedded", "sqlite"} {
		t.Run(driver, func(t *testing.T) {
			cfg := testConfig(t)
			cfg.Store.Driver = driver
			err := Run(t.Context(), cfg)
			require.Error(t, err)
			require.NoFileExists(t, cfg.DatabasePath, "run must not install or create a placeholder database")
		})
	}
}

func TestSQLiteReadonlyIncompleteStructureIsReportedWithoutChangingDatabase(t *testing.T) {
	for _, journal := range []string{"DELETE", "WAL"} {
		t.Run(journal, func(t *testing.T) {
			cfg := testConfig(t)
			cfg.Store.Driver = "sqlite"
			cfg.Store.TablePrefix = "ro_"
			db, err := sql.Open("sqlite", cfg.DatabasePath)
			require.NoError(t, err)
			_, err = db.Exec("PRAGMA journal_mode=" + journal)
			require.NoError(t, err)
			_, err = db.Exec("CREATE TABLE ro_channel_meta (k TEXT PRIMARY KEY, v TEXT NOT NULL)")
			require.NoError(t, err)
			_, err = db.Exec("CREATE TABLE historical_data (value TEXT NOT NULL)")
			require.NoError(t, err)
			_, err = db.Exec("INSERT INTO historical_data VALUES ('retained')")
			require.NoError(t, err)
			require.NoError(t, db.Close())
			before, err := os.ReadFile(cfg.DatabasePath)
			require.NoError(t, err)
			_, err = Status(context.Background(), cfg)
			require.ErrorContains(t, err, "database schema unsupported")
			require.ErrorIs(t, err, storage.ErrSQLSchema)
			checks := Doctor(context.Background(), cfg, DoctorOptions{Resolver: staticResolver{}})
			for _, check := range checks {
				if check.Name == "database" {
					require.False(t, check.OK)
					require.Contains(t, check.Detail, "schema unsupported")
				}
			}
			after, err := os.ReadFile(cfg.DatabasePath)
			require.NoError(t, err)
			require.Equal(t, before, after, "read-only status/doctor cannot change schema, journal mode or historical content")
		})
	}
}
