// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

//go:build mysqlintegration

package sqlstore

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
)

func TestMySQLUserAuthStore(t *testing.T) {
	codec := &conformance.UserAuthTestCodec{}
	factory := func(t *testing.T, c conformance.StoreConfig) conformance.TestStore {
		db := mysqlTestDB(t)
		prefix := fmt.Sprintf("ua%d_", time.Now().UnixNano())
		s, e := openInstalledSQLFixture(t.Context(), db, MySQL, Options{Clock: c.Clock, TablePrefix: prefix, PayloadCodec: codec, PayloadNamespace: "user-without-kind", RequirePayloadEncryption: true})
		require.NoError(t, e)
		var columns int
		require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name LIKE ? AND column_name='work_kind'", prefix+"%").Scan(&columns))
		require.Zero(t, columns)
		return s
	}
	conformance.RunUserAuthStore(t, factory)
	conformance.RunUserAuthRace(t, factory)
	conformance.RunUserAuthIngest(t, factory)
	conformance.RunUserAuthWait(t, factory)
	conformance.RunUserAuthPayload(t, codec, factory)
	conformance.RunUserAuthRefresh(t, factory)
	conformance.RunUserAuthUncertainFailure(t, factory)
}
