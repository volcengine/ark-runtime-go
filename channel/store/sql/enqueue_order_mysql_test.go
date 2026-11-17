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
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestMySQLTurnOrderComesFromMetadataColumn(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprintf("encrypted_%v", encrypted), func(t *testing.T) {
			ctx := t.Context()
			db := mysqlTestDB(t)
			prefix := fmt.Sprintf("eq%d_%d_", time.Now().UnixNano(), mysqlTestCounter.Add(1))
			options := Options{TablePrefix: prefix}
			var c *payloadTestCodec
			if encrypted {
				c = newPayloadTestCodec(t)
				options.PayloadCodec = c
				options.PayloadNamespace = "deployment"
				options.RequirePayloadEncryption = true
			}
			s, err := openInstalledSQLFixture(ctx, db, MySQL, options)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, s.Close()) })
			result, err := s.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("order"))
			require.NoError(t, err)
			require.NotNil(t, result.Turn)
			var expected uint64
			require.NoError(t, db.QueryRowContext(ctx, "SELECT enqueue_order FROM "+prefix+"channel_turns WHERE id=?", result.Turn.ID).Scan(&expected))
			require.Positive(t, expected)
			require.Equal(t, expected, result.Turn.EnqueueOrder, "fresh Ingest returns the actual globally allocated order")
			_, err = db.ExecContext(ctx, "UPDATE "+prefix+"channel_turns SET data=JSON_SET(data,'$.enqueue_order',0) WHERE id=?", result.Turn.ID)
			require.NoError(t, err)
			got, err := s.GetTurn(ctx, result.Turn.ID)
			require.NoError(t, err)
			require.Equal(t, expected, got.EnqueueOrder, "legacy JSON zero does not override the authoritative column")
			listed, err := s.ListTurns(ctx, core.TurnQuery{})
			require.NoError(t, err)
			require.Len(t, listed, 1)
			require.Equal(t, expected, listed[0].EnqueueOrder)
			exported, err := s.Export(ctx)
			require.NoError(t, err)
			require.Len(t, exported.Turns, 1)
			require.Equal(t, expected, exported.Turns[0].EnqueueOrder)
			var before int64
			if c != nil {
				before = c.seals.Load()
			}
			lease, err := s.LeaseTurn(ctx, conformance.Claim("worker"))
			require.NoError(t, err)
			require.NotNil(t, lease)
			require.Equal(t, expected, lease.Turn.EnqueueOrder)
			if c != nil {
				require.Equal(t, before, c.seals.Load(), "order overlay is metadata-only")
			}
		})
	}
}
