// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

//go:build mysqlintegration

package sqlstore

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
)

func TestMySQLRemoteOperations(t *testing.T) {
	conformance.RunRemoteOperationStore(t, func(t *testing.T, c conformance.StoreConfig) conformance.TestStore {
		s, e := openInstalledSQLFixture(context.Background(), mysqlTestDB(t), MySQL, Options{Clock: c.Clock, TablePrefix: fmt.Sprintf("ro%d_", time.Now().UnixNano())})
		require.NoError(t, e)
		return s
	})
	conformance.RunRemoteOperationFences(t, func(t *testing.T, c conformance.StoreConfig) conformance.TestStore {
		s, e := openInstalledSQLFixture(context.Background(), mysqlTestDB(t), MySQL, Options{Clock: c.Clock, TablePrefix: fmt.Sprintf("ro%d_", time.Now().UnixNano())})
		require.NoError(t, e)
		return s
	})
	conformance.RunRemoteCreationBinding(t, func(t *testing.T, c conformance.StoreConfig) conformance.TestStore {
		s, e := openInstalledSQLFixture(context.Background(), mysqlTestDB(t), MySQL, Options{Clock: c.Clock, TablePrefix: fmt.Sprintf("ro%d_", time.Now().UnixNano())})
		require.NoError(t, e)
		return s
	})
}
