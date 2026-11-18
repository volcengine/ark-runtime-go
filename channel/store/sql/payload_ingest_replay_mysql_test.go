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

func TestMySQLPayloadIngestResultTracksCommittedBranch(t *testing.T) {
	payloadIngestResultTracksCommittedBranch(t, func(t *testing.T, c *payloadTestCodec) *store {
		s, err := openInstalledSQLFixture(t.Context(), mysqlTestDB(t), MySQL, Options{TablePrefix: fmt.Sprintf("ir%d_%d_", time.Now().UnixNano(), mysqlTestCounter.Add(1)), PayloadCodec: c, PayloadNamespace: "deployment", RequirePayloadEncryption: true, MaxChannelOpenTurns: 1, MaxRejectedMessages: 8})
		require.NoError(t, err)
		return s
	})
}
