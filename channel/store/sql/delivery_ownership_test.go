// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestSQLiteSnapshotlessDeliveryUsesSourceOwnership(t *testing.T) {
	ctx := context.Background()
	db := openSQLite(t, filepath.Join(t.TempDir(), "owner.db"))
	s, e := openInstalledSQLFixture(ctx, db, SQLite, Options{PayloadCodec: newPayloadTestCodec(t), PayloadNamespace: "review", RequirePayloadEncryption: true})
	require.NoError(t, e)
	defer func() { require.NoError(t, s.Close()) }()
	b := conformance.SampleBinding()
	b.Owner = "real-owner"
	b.Project = "real-project"
	_, e = s.Ingest(ctx, core.NewBindingVersion(b), conformance.SampleMessage("owner"))
	require.NoError(t, e)
	l, e := s.LeaseTurn(ctx, conformance.Claim("worker"))
	require.NoError(t, e)
	d := core.NewDelivery(core.DeliverySource{Kind: core.SourceTurn, ID: l.Turn.ID}, 0, "answer", b.ID, core.ReplyTarget{ChatID: "chat"}, core.TextBody("answer"))
	require.NoError(t, s.SettleTurn(ctx, l.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, []core.Delivery{d}))
	leased, e := s.LeaseDelivery(ctx, "worker", time.Minute)
	require.NoError(t, e)
	require.NotNil(t, leased)
	require.Equal(t, "answer", leased.Body.Text)
	var raw string
	require.NoError(t, db.QueryRow("SELECT data FROM channel_deliveries WHERE id=?", d.ID).Scan(&raw))
	var stored core.Delivery
	require.NoError(t, json.Unmarshal([]byte(raw), &stored))
	require.Equal(t, b.Owner, stored.Payloads.Owner)
	require.Equal(t, b.Project, stored.Payloads.Project)
	require.NoError(t, s.CompleteDelivery(ctx, leased.Lease, core.DeliveryResult{Status: core.DeliveryDone}))
}
