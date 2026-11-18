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

func TestMySQLPayloadEncryptedStore(t *testing.T) {
	factory := func(t *testing.T, c conformance.StoreConfig) conformance.TestStore {
		s, e := openInstalledSQLFixture(context.Background(), mysqlTestDB(t), MySQL, Options{Clock: c.Clock, MaxOpenTurns: c.MaxOpenTurns, MaxMessageBytes: c.MaxMessageBytes, QueueTTL: c.QueueTTL, TablePrefix: fmt.Sprintf("pe%d_%d_", time.Now().UnixNano(), mysqlTestCounter.Add(1)), PayloadCodec: newPayloadTestCodec(t), PayloadNamespace: "deployment", RequirePayloadEncryption: true})
		require.NoError(t, e)
		return s
	}
	conformance.RunStore(t, factory)
	conformance.RunRemoteOperationStore(t, factory)
	conformance.RunRemoteOperationFences(t, factory)
	conformance.RunRemoteCreationBinding(t, factory)
}
