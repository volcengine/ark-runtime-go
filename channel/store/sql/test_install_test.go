// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"context"
	"database/sql"
)

func openInstalledSQLFixture(ctx context.Context, db *sql.DB, dialect Dialect, options Options) (*store, error) {
	var count int
	var err error
	if dialect.Name == SQLite.Name {
		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", options.TablePrefix+"channel_meta").Scan(&count)
	} else {
		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name=?", options.TablePrefix+"channel_meta").Scan(&count)
	}
	if err != nil {
		return nil, err
	}
	if count == 0 {
		if err = Install(ctx, db, dialect, options); err != nil {
			return nil, err
		}
	}
	return open(ctx, db, dialect, options)
}
