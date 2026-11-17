// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
)

func TestUserAuthStore(t *testing.T) {
	conformance.RunUserAuthStore(t, func(t *testing.T, c conformance.StoreConfig) conformance.TestStore {
		s, e := openInstalledSQLFixture(context.Background(), openSQLite(t, filepath.Join(t.TempDir(), "auth.db")), SQLite, Options{Clock: c.Clock})
		require.NoError(t, e)
		return s
	})
}

func TestUserAuthWait(t *testing.T) {
	conformance.RunUserAuthWait(t, func(t *testing.T, c conformance.StoreConfig) conformance.TestStore {
		s, e := openInstalledSQLFixture(context.Background(), openSQLite(t, filepath.Join(t.TempDir(), "auth.db")), SQLite, Options{Clock: c.Clock, PayloadCodec: &conformance.UserAuthTestCodec{}, PayloadNamespace: "test", RequirePayloadEncryption: true})
		require.NoError(t, e)
		return s
	})
}

func TestUserAuthRace(t *testing.T) {
	conformance.RunUserAuthRace(t, func(t *testing.T, c conformance.StoreConfig) conformance.TestStore {
		s, e := openInstalledSQLFixture(context.Background(), openSQLite(t, filepath.Join(t.TempDir(), "auth.db")), SQLite, Options{Clock: c.Clock})
		require.NoError(t, e)
		return s
	})
}

func TestUserAuthPayload(t *testing.T) {
	c := &conformance.UserAuthTestCodec{}
	conformance.RunUserAuthPayload(t, c, func(t *testing.T, config conformance.StoreConfig) conformance.TestStore {
		s, e := openInstalledSQLFixture(context.Background(), openSQLite(t, filepath.Join(t.TempDir(), "auth.db")), SQLite, Options{Clock: config.Clock, PayloadCodec: c, PayloadNamespace: "test", RequirePayloadEncryption: true})
		require.NoError(t, e)
		return s
	})
}

func TestUserAuthIngest(t *testing.T) {
	conformance.RunUserAuthIngest(t, func(t *testing.T, c conformance.StoreConfig) conformance.TestStore {
		s, e := openInstalledSQLFixture(context.Background(), openSQLite(t, filepath.Join(t.TempDir(), "auth.db")), SQLite, Options{Clock: c.Clock})
		require.NoError(t, e)
		return s
	})
}

func TestUserAuthRefresh(t *testing.T) {
	conformance.RunUserAuthRefresh(t, func(t *testing.T, c conformance.StoreConfig) conformance.TestStore {
		s, e := openInstalledSQLFixture(context.Background(), openSQLite(t, filepath.Join(t.TempDir(), "auth.db")), SQLite, Options{Clock: c.Clock})
		require.NoError(t, e)
		return s
	})
}

func TestUserAuthUncertainFailure(t *testing.T) {
	conformance.RunUserAuthUncertainFailure(t, func(t *testing.T, c conformance.StoreConfig) conformance.TestStore {
		s, e := openInstalledSQLFixture(context.Background(), openSQLite(t, filepath.Join(t.TempDir(), "auth.db")), SQLite, Options{Clock: c.Clock, PayloadCodec: &conformance.UserAuthTestCodec{}, PayloadNamespace: "test", RequirePayloadEncryption: true})
		require.NoError(t, e)
		return s
	})
}
