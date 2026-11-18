// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

//go:build mysqlintegration

package sqlstore

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestMySQLUnknownProtocolWithoutWorkKind(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprint(encrypted), func(t *testing.T) {
			ctx := context.Background()
			db := mysqlTestDB(t)
			prefix := fmt.Sprintf("nk%d_", time.Now().UnixNano())
			clock := conformance.NewClock(time.Now().UTC())
			options := Options{Clock: clock.Now, TablePrefix: prefix, QueueTTL: time.Minute}
			if encrypted {
				options.PayloadCodec = newPayloadTestCodec(t)
				options.PayloadNamespace = "without-kind-mysql"
				options.RequirePayloadEncryption = true
			}
			store, err := openInstalledSQLFixture(ctx, db, MySQL, options)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			var columns int
			require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name LIKE ? AND column_name='work_kind'", prefix+"%").Scan(&columns))
			require.Zero(t, columns)
			head, err := store.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("head"))
			require.NoError(t, err)
			tail, err := store.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("tail"))
			require.NoError(t, err)
			var raw string
			require.NoError(t, db.QueryRowContext(ctx, "SELECT data FROM "+prefix+"channel_turns WHERE id=?", head.Turn.ID).Scan(&raw))
			var object map[string]any
			require.NoError(t, json.Unmarshal([]byte(raw), &object))
			object["_work_kind"] = "future"
			object["lease"] = map[string]any{"owner": 99}
			wire, err := json.Marshal(object)
			require.NoError(t, err)
			_, err = db.ExecContext(ctx, "UPDATE "+prefix+"channel_turns SET data=?,eligible_at=? WHERE id=?", string(wire), clock.Now().Add(time.Hour).UnixNano(), head.Turn.ID)
			require.NoError(t, err)
			clock.Advance(2 * time.Minute)
			claim, err := store.LeaseTurn(ctx, conformance.Claim("worker"))
			require.NoError(t, err)
			require.Nil(t, claim, "expired tail cannot cross the unsupported head")
			tailAfter, err := store.GetTurn(ctx, tail.Turn.ID)
			require.NoError(t, err)
			require.Zero(t, tailAfter.Lease.Fence)
			var after string
			require.NoError(t, db.QueryRowContext(ctx, "SELECT data FROM "+prefix+"channel_turns WHERE id=?", head.Turn.ID).Scan(&after))
			require.Equal(t, string(wire), after)
			// 未知会话不能阻塞另一会话；普通答案继续使用无列路径。
			safe := conformance.SampleMessage("safe")
			safe.ChatID = "safe"
			accepted, err := store.Ingest(ctx, conformance.SampleVersion(), safe)
			require.NoError(t, err)
			claim, err = store.LeaseTurn(ctx, conformance.Claim("worker"))
			require.NoError(t, err)
			require.NotNil(t, claim)
			require.Equal(t, accepted.Turn.ID, claim.Turn.ID)
			delivery := core.NewDelivery(core.DeliverySource{Kind: core.SourceTurn, ID: claim.Turn.ID}, 0, "answer", claim.Turn.BindingID, core.ReplyTarget{}, core.TextBody("answer"))
			require.NoError(t, store.SettleTurn(ctx, claim.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, []core.Delivery{delivery}))
			out, err := store.LeaseDelivery(ctx, "delivery", time.Minute)
			require.NoError(t, err)
			require.NotNil(t, out)
			require.Equal(t, delivery.ID, out.ID)
			require.NoError(t, store.CompleteDelivery(ctx, out.Lease, core.DeliveryResult{Status: core.DeliveryDone}))
			removed, err := store.Prune(ctx, clock.Now().Add(time.Hour))
			require.NoError(t, err)
			require.Equal(t, 1, removed.Turns)
		})
	}
}
