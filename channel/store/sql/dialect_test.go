// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	sqlstore "github.com/volcengine/ark-runtime-go/channel/store/sql"
)

func TestDialectDefaultsCannotRedefineSupportedDrivers(t *testing.T) {
	mysql, sqlite := sqlstore.MySQL, sqlstore.SQLite
	t.Cleanup(func() { sqlstore.MySQL, sqlstore.SQLite = mysql, sqlite })
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	sqlstore.MySQL.Name = "unsupported_mysql"
	sqlstore.SQLite.Name = "unsupported_sqlite"
	for _, dialect := range []sqlstore.Dialect{sqlstore.MySQL, sqlstore.SQLite} {
		_, err := sqlstore.Open(t.Context(), db, dialect, sqlstore.Options{})
		require.ErrorIs(t, err, core.ErrInvalid)
	}
}
