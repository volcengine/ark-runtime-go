// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

//go:build mysqlintegration

package sqlstore

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMySQLDeliveryBatchPrecedesLaterSource(t *testing.T) {
	s, err := openInstalledSQLFixture(t.Context(), mysqlTestDB(t), MySQL, Options{TablePrefix: fmt.Sprintf("bf%d_", time.Now().UnixNano())})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	sqlDeliveryBatchPrecedesLaterSource(t, s)
}
