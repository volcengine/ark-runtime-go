// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func sqlDeliveryBatchPrecedesLaterSource(t *testing.T, s *store) {
	t.Helper()
	ctx := t.Context()
	var expected []string
	for batch, size := range []int{3, 1} {
		_, err := s.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage(fmt.Sprintf("source-%d", batch)))
		require.NoError(t, err)
		turn, err := s.LeaseTurn(ctx, conformance.Claim("worker"))
		require.NoError(t, err)
		require.NotNil(t, turn)
		source := core.DeliverySource{Kind: core.SourceTurn, ID: turn.Turn.ID}
		deliveries := make([]core.Delivery, 0, size)
		for i := range size {
			d := core.NewDelivery(source, i, fmt.Sprintf("part-%d", i), turn.Turn.BindingID, core.ReplyTarget{ChatID: "chat"}, core.TextBody(fmt.Sprintf("batch %d part %d", batch, i)))
			deliveries = append(deliveries, d)
			expected = append(expected, d.ID)
		}
		require.NoError(t, s.SettleTurn(ctx, turn.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, deliveries))
	}
	for _, id := range expected {
		d, err := s.LeaseDelivery(ctx, "sender", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, d)
		require.Equal(t, id, d.ID, "an earlier atomic batch must precede a later source in the same conversation")
		require.NoError(t, s.CompleteDelivery(ctx, d.Lease, core.DeliveryResult{Status: core.DeliveryDone}))
	}
}
func TestSQLiteDeliveryBatchPrecedesLaterSource(t *testing.T) {
	s, err := openInstalledSQLFixture(t.Context(), openSQLite(t, filepath.Join(t.TempDir(), "batch.db")), SQLite, Options{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	sqlDeliveryBatchPrecedesLaterSource(t, s)
}
