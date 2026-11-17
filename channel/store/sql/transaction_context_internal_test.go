// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func TestTransactionCancellationPreservesCallerError(t *testing.T) {
	for _, phase := range []string{"statement", "wrapped_statement", "commit", "deadline_statement", "deadline_commit"} {
		t.Run(phase, func(t *testing.T) {
			db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "cancel.db"))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			db.SetMaxOpenConns(1)
			_, err = db.Exec("CREATE TABLE retained (id INTEGER PRIMARY KEY)")
			require.NoError(t, err)
			s := &store{db: db, dialect: SQLite}
			ctx, cancel := context.WithCancel(t.Context())
			deadline := phase == "deadline_statement" || phase == "deadline_commit"
			if deadline {
				cancel()
				ctx, cancel = context.WithTimeout(t.Context(), time.Second)
			}
			defer cancel()
			entered := false
			err = s.runTxOnce(ctx, func(ctx context.Context, tx *sql.Tx) error {
				entered = true
				_, err := tx.ExecContext(ctx, "INSERT INTO retained VALUES (1)")
				require.NoError(t, err)
				if deadline {
					<-ctx.Done()
				} else {
					cancel()
				}
				// 等待 database/sql 自动回滚，确定性覆盖取消与事务状态检查之间的竞态。
				require.Eventually(t, func() bool { return db.Stats().InUse == 0 }, time.Second, time.Millisecond)
				if phase == "commit" || phase == "deadline_commit" {
					return nil
				}
				_, err = tx.ExecContext(context.Background(), "INSERT INTO retained VALUES (2)")
				require.ErrorIs(t, err, sql.ErrTxDone)
				if phase == "wrapped_statement" {
					return fmt.Errorf("write retained: %w", err)
				}
				return err
			})
			require.True(t, entered, "必须覆盖已开始事务后的取消")
			want := context.Canceled
			if deadline {
				want = context.DeadlineExceeded
			}
			require.ErrorIs(t, err, want)
			require.NotErrorIs(t, err, sql.ErrTxDone)
			var count int
			require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM retained").Scan(&count))
			require.Zero(t, count, "取消的事务不得留下写入")
		})
	}
}

func TestTransactionCancellationDoesNotMaskOtherFailures(t *testing.T) {
	failure := errors.New("storage unavailable")
	for _, tc := range []struct {
		name   string
		cancel bool
		err    error
	}{{"live_transaction_done", false, sql.ErrTxDone}, {"live_store_failure", false, failure}, {"canceled_store_failure", true, failure}} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := sql.Open("sqlite", ":memory:")
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			s := &store{db: db, dialect: SQLite}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			err = s.runTxOnce(ctx, func(context.Context, *sql.Tx) error {
				if tc.cancel {
					cancel()
				}
				return tc.err
			})
			require.ErrorIs(t, err, tc.err)
		})
	}
}
