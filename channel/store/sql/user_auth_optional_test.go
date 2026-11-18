// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
)

func TestUserAuthMissingOptionalTableDoesNotBlockBotOpen(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "optional.db")
	db := openSQLite(t, path)
	s, e := openInstalledSQLFixture(ctx, db, SQLite, Options{})
	require.NoError(t, e)
	_, e = db.Exec("DROP TABLE channel_user_auth")
	require.NoError(t, e)
	require.NoError(t, s.Close())
	db = openSQLite(t, path)
	s, e = open(ctx, db, SQLite, Options{})
	require.NoError(t, e)
	defer func() { require.NoError(t, s.Close()) }()
	_, e = s.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("bot"))
	require.NoError(t, e)
	require.NoError(t, expandUserAuth(ctx, db, SQLite, ""))
}
