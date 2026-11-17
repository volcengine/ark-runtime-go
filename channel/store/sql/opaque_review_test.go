// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

type sqlOpaqueStartupProvider struct{ core.Provider }
type sqlOpaqueStartupClient struct{ core.MAClient }

func TestSQLOpaqueUnknownJSONKindDoesNotDecodeFutureStructure(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprint(encrypted), func(t *testing.T) {
			ctx := context.Background()
			db := lifecycleDB(t)
			options := Options{}
			codec := newPayloadTestCodec(t)
			if encrypted {
				options.PayloadCodec = codec
				options.PayloadNamespace = "opaque-view"
				options.RequirePayloadEncryption = true
			}
			require.NoError(t, Install(ctx, db, SQLite, options))
			store, err := open(ctx, db, SQLite, options)
			require.NoError(t, err)
			delivery := workKindSettle(t, store, "opaque-delivery", 1)[0]
			accepted, err := store.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("opaque-turn"))
			require.NoError(t, err)
			require.NotNil(t, accepted.Turn)
			raws := map[string]string{}
			for table, id := range map[string]string{"channel_turns": accepted.Turn.ID, "channel_deliveries": delivery.ID} {
				var raw string
				require.NoError(t, db.QueryRow("SELECT data FROM "+table+" WHERE id=?", id).Scan(&raw))
				var object map[string]any
				require.NoError(t, json.Unmarshal([]byte(raw), &object))
				object["_work_kind"] = "future_kind"
				object["id"] = 99
				object["lease"] = map[string]any{"owner": 99, "fence": "future"}
				object["body"] = []any{"future", "shape"}
				object["_payloads"] = map[string]any{"refs": []any{"unsupported"}}
				object["snapshot"] = []any{"unknown"}
				future, err := json.Marshal(object)
				require.NoError(t, err)
				_, err = db.Exec("UPDATE "+table+" SET data=? WHERE id=?", string(future), id)
				require.NoError(t, err)
				raws[table] = string(future)
			}
			beforeOpens := codec.opens.Load()
			deliveries, err := store.ListDeliveries(ctx, core.DeliveryQuery{OpenOnly: true})
			require.NoError(t, err)
			require.Len(t, deliveries, 1)
			require.Equal(t, delivery.ID, deliveries[0].ID, "pagination ID comes from the authoritative SQL row")
			require.True(t, deliveries[0].IsOpaqueStored())
			require.Equal(t, "future_kind", deliveries[0].StoredWorkKind())
			require.Zero(t, deliveries[0].Lease)
			require.Nil(t, deliveries[0].Payloads)
			data, err := core.EncodeStored(deliveries[0])
			require.NoError(t, err)
			require.Equal(t, raws["channel_deliveries"], string(data))
			require.Equal(t, beforeOpens, codec.opens.Load(), "opaque records never enter payload hydration")
			turns, err := store.ListTurns(ctx, core.TurnQuery{OpenOnly: true})
			require.NoError(t, err)
			require.Len(t, turns, 1)
			require.Equal(t, accepted.Turn.ID, turns[0].ID)
			require.True(t, turns[0].IsOpaqueStored())
			require.Zero(t, turns[0].Lease)
			require.Nil(t, turns[0].Snapshot)
			data, err = core.EncodeStored(turns[0])
			require.NoError(t, err)
			require.Equal(t, raws["channel_turns"], string(data))
			one, err := store.GetTurn(ctx, accepted.Turn.ID)
			require.NoError(t, err)
			require.NotNil(t, one)
			require.True(t, one.IsOpaqueStored())
			require.Equal(t, accepted.Turn.ID, one.ID)
			runtime, err := core.NewRuntime(store, []core.Connection{{Binding: conformance.SampleBinding(), Provider: &sqlOpaqueStartupProvider{}, MAClient: &sqlOpaqueStartupClient{}}}, core.RuntimeOptions{})
			require.NoError(t, err, "actual Runtime startup can inspect future row formats without decoding them")
			require.NotNil(t, runtime)
		})
	}
}

func TestSQLOpaqueRawHeaderDoesNotExecuteAsLegacy(t *testing.T) {
	ctx := context.Background()
	db, store := workKindStore(t)
	accepted, err := store.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("opaque-head"))
	require.NoError(t, err)
	raw := fmt.Sprintf(`{"id":%q,"_work_kind":"future_header","lease":{"owner":99},"snapshot":["new"]}`, accepted.Turn.ID)
	_, err = db.Exec("UPDATE channel_turns SET data=? WHERE id=?", raw, accepted.Turn.ID)
	require.NoError(t, err)
	list, err := store.ListTurns(ctx, core.TurnQuery{OpenOnly: true})
	require.NoError(t, err)
	require.Len(t, list, 1)
	require.True(t, list[0].IsOpaqueStored())
	claimed, err := store.LeaseTurn(ctx, conformance.Claim("worker"))
	require.NoError(t, err)
	require.Nil(t, claimed)
	var after string
	require.NoError(t, db.QueryRow("SELECT data FROM channel_turns WHERE id=?", accepted.Turn.ID).Scan(&after))
	require.Equal(t, raw, after)
}

func TestSQLOpaquePaginationUsesSQLIdentityAcrossFullPages(t *testing.T) {
	ctx := context.Background()
	db, store := workKindStore(t)
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	for i := 0; i < core.MaxPageSize+3; i++ {
		id := fmt.Sprintf("future-%04d", i)
		object := map[string]any{"_work_kind": "future_kind", "lease": map[string]any{"owner": 99}, "body": []any{"new", "shape"}}
		switch i % 3 {
		case 0:
			object["id"] = 99
		case 1:
		case 2:
			object["id"] = "different-logical-id"
		}
		raw, err := json.Marshal(object)
		require.NoError(t, err)
		_, err = tx.ExecContext(ctx, "INSERT INTO channel_turns(id,seq,conversation_id,message_id,binding_id,binding_version,status,phase,queue_head,eligible_at,queue_deadline,stop_requested_at,created_at,settled_at,revision,data) VALUES (?,?,'conversation',?,'binding','version','queued','',0,0,0,0,0,0,1,?)", id, i, "message-"+id, string(raw))
		require.NoError(t, err)
		_, err = tx.ExecContext(ctx, "INSERT INTO channel_deliveries(id,source_kind,source_id,seq,binding_id,uuid,status,queue_head,eligible_at,data) VALUES (?,'turn',?,0,'binding',?,'pending',1,0,?)", id, id, id, string(raw))
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())
	for _, entity := range []string{"turn", "delivery"} {
		seen := map[string]bool{}
		after := ""
		for page := 0; page < 3; page++ {
			var ids []string
			if entity == "turn" {
				records, err := store.ListTurns(ctx, core.TurnQuery{OpenOnly: true, After: after, Limit: core.MaxPageSize})
				require.NoError(t, err)
				for _, record := range records {
					require.True(t, record.IsOpaqueStored())
					ids = append(ids, record.ID)
				}
			} else {
				records, err := store.ListDeliveries(ctx, core.DeliveryQuery{OpenOnly: true, After: after, Limit: core.MaxPageSize})
				require.NoError(t, err)
				for _, record := range records {
					require.True(t, record.IsOpaqueStored())
					ids = append(ids, record.ID)
				}
			}
			if len(ids) == 0 {
				break
			}
			for _, id := range ids {
				require.NotEmpty(t, id)
				require.False(t, seen[id], "opaque pagination must not rescan the same physical row")
				seen[id] = true
			}
			require.Greater(t, ids[len(ids)-1], after)
			after = ids[len(ids)-1]
		}
		require.Len(t, seen, core.MaxPageSize+3)
	}
	runtime, err := core.NewRuntime(store, []core.Connection{{Binding: conformance.SampleBinding(), Provider: &sqlOpaqueStartupProvider{}, MAClient: &sqlOpaqueStartupClient{}}}, core.RuntimeOptions{})
	require.NoError(t, err)
	require.NotNil(t, runtime)
}
