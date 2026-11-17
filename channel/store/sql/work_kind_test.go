// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/payload"
)

func workKindStore(t *testing.T) (*sql.DB, *store) {
	t.Helper()
	db := lifecycleDB(t)
	require.NoError(t, Install(context.Background(), db, SQLite, Options{}))
	store, err := open(context.Background(), db, SQLite, Options{})
	require.NoError(t, err)
	return db, store
}
func workKindSettle(t *testing.T, store *store, id string, count int) []core.Delivery {
	t.Helper()
	ctx := context.Background()
	msg := conformance.SampleMessage(id)
	msg.ChatID = id
	_, err := store.Ingest(ctx, conformance.SampleVersion(), msg)
	require.NoError(t, err)
	turn, err := store.LeaseTurn(ctx, conformance.Claim("setup"))
	require.NoError(t, err)
	require.NotNil(t, turn)
	source := core.DeliverySource{Kind: core.SourceTurn, ID: turn.Turn.ID}
	var deliveries []core.Delivery
	for seq := 0; seq < count; seq++ {
		deliveries = append(deliveries, core.NewDelivery(source, seq, fmt.Sprint(seq), turn.Turn.BindingID, core.ReplyTarget{}, core.TextBody("answer")))
	}
	require.NoError(t, store.SettleTurn(ctx, turn.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, deliveries))
	return deliveries
}
func replaceDeliveryBody(t *testing.T, db *sql.DB, id string, status string) string {
	t.Helper()
	var raw string
	require.NoError(t, db.QueryRow("SELECT data FROM channel_deliveries WHERE id=?", id).Scan(&raw))
	var wire map[string]any
	require.NoError(t, json.Unmarshal([]byte(raw), &wire))
	wire["body"] = map[string]any{"type": "future_body", "private_future": map[string]any{"id": "opaque"}}
	if status != "" {
		wire["status"] = status
	}
	data, err := json.Marshal(wire)
	require.NoError(t, err)
	_, err = db.Exec("UPDATE channel_deliveries SET data=? WHERE id=?", string(data), id)
	require.NoError(t, err)
	return string(data)
}

func TestWorkKindTurnUnknownHeadDoesNotDecodeOrBlockOtherConversation(t *testing.T) {
	ctx := context.Background()
	db, store := workKindStore(t)
	first, err := store.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("unknown"))
	require.NoError(t, err)
	_, err = store.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("behind"))
	require.NoError(t, err)
	raw := `{"_work_kind":"future","future_record":{"opaque":true},"lease":{"owner":99}}`
	_, err = db.Exec("UPDATE channel_turns SET data=? WHERE id=?", raw, first.Turn.ID)
	require.NoError(t, err)
	other := conformance.SampleMessage("other")
	other.ChatID = "other"
	accepted, err := store.Ingest(ctx, conformance.SampleVersion(), other)
	require.NoError(t, err)
	leased, err := store.LeaseTurn(ctx, conformance.Claim("worker"))
	require.NoError(t, err)
	require.NotNil(t, leased)
	require.Equal(t, accepted.Turn.ID, leased.Turn.ID)
	var after string
	require.NoError(t, db.QueryRow("SELECT data FROM channel_turns WHERE id=?", first.Turn.ID).Scan(&after))
	require.Equal(t, raw, after)
}

func TestWorkKindDeliveryUnknownHeadPreservesSourceFIFO(t *testing.T) {
	for _, mode := range []string{"kind", "body"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			db, store := workKindStore(t)
			blocked := workKindSettle(t, store, "blocked", 2)
			allowed := workKindSettle(t, store, "allowed", 1)
			var before string
			if mode == "kind" {
				before = `{"_work_kind":"future","future":"opaque","lease":{"owner":99}}`
				_, err := db.Exec("UPDATE channel_deliveries SET data=?,inbox_seq=0 WHERE id=?", before, blocked[0].ID)
				require.NoError(t, err)
			} else {
				before = replaceDeliveryBody(t, db, blocked[0].ID, "")
				_, err := db.Exec("UPDATE channel_deliveries SET inbox_seq=0 WHERE id=?", blocked[0].ID)
				require.NoError(t, err)
			}
			delivery, err := store.LeaseDelivery(ctx, "worker", time.Minute)
			require.NoError(t, err)
			require.NotNil(t, delivery)
			require.Equal(t, allowed[0].ID, delivery.ID)
			require.NoError(t, store.CompleteDelivery(ctx, delivery.Lease, core.DeliveryResult{Status: core.DeliveryDone}))
			for range 3 {
				none, err := store.LeaseDelivery(ctx, "worker", time.Minute)
				require.NoError(t, err)
				require.Nil(t, none)
			}
			var after string
			require.NoError(t, db.QueryRow("SELECT data FROM channel_deliveries WHERE id=?", blocked[0].ID).Scan(&after))
			require.Equal(t, before, after)
			var tailAttempts int
			require.NoError(t, db.QueryRow("SELECT json_extract(data,'$.attempt') FROM channel_deliveries WHERE id=?", blocked[1].ID).Scan(&tailAttempts))
			require.Zero(t, tailAttempts)
		})
	}
}

func TestWorkKindUnknownBodyIsOpaqueInAdminAndConservativelyRetained(t *testing.T) {
	ctx := context.Background()
	db, store := workKindStore(t)
	blocked := workKindSettle(t, store, "blocked", 1)
	safe := workKindSettle(t, store, "safe", 1)
	raw := replaceDeliveryBody(t, db, blocked[0].ID, "done")
	_, err := db.Exec("UPDATE channel_deliveries SET status='done',queue_head=0 WHERE id=?", blocked[0].ID)
	require.NoError(t, err)
	delivery, err := store.LeaseDelivery(ctx, "worker", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, delivery)
	require.Equal(t, safe[0].ID, delivery.ID)
	require.NoError(t, store.CompleteDelivery(ctx, delivery.Lease, core.DeliveryResult{Status: core.DeliveryDone}))
	list, err := store.ListDeliveries(ctx, core.DeliveryQuery{})
	require.NoError(t, err)
	found := false
	for _, item := range list {
		if item.ID == blocked[0].ID {
			found = true
			require.Equal(t, core.BodyType("future_body"), item.Body.Type)
			wire, err := core.EncodeStored(item)
			require.NoError(t, err)
			require.Contains(t, string(wire), "private_future")
		}
	}
	require.True(t, found)
	result, err := store.Prune(ctx, time.Now().Add(time.Hour))
	require.NoError(t, err)
	require.Equal(t, 1, result.Turns, "supported sources can still be pruned")
	var after string
	require.NoError(t, db.QueryRow("SELECT data FROM channel_deliveries WHERE id=?", blocked[0].ID).Scan(&after))
	require.Equal(t, raw, after)
}

func TestWorkKindDeliveryManyUnknownBodiesDoNotStarveOtherSource(t *testing.T) {
	ctx := context.Background()
	db, store := workKindStore(t)
	for i := 0; i < 33; i++ {
		delivery := workKindSettle(t, store, fmt.Sprintf("blocked-%d", i), 1)[0]
		replaceDeliveryBody(t, db, delivery.ID, "")
		_, err := db.Exec("UPDATE channel_deliveries SET inbox_seq=0 WHERE id=?", delivery.ID)
		require.NoError(t, err)
	}
	allowed := workKindSettle(t, store, "allowed", 1)[0]
	var found *core.Delivery
	for range 3 {
		delivery, err := store.LeaseDelivery(ctx, "worker", time.Minute)
		require.NoError(t, err)
		if delivery != nil {
			found = delivery
			break
		}
	}
	require.NotNil(t, found)
	require.Equal(t, allowed.ID, found.ID)
}

func TestWorkKindRemoteUnknownPayloadDoesNotDecode(t *testing.T) {
	ctx := context.Background()
	db, store := workKindStore(t)
	workKindSettle(t, store, "remote", 0)
	turns, err := store.ListTurns(ctx, core.TurnQuery{})
	require.NoError(t, err)
	require.Len(t, turns, 1)
	known := core.NewRemoteOperation(turns[0], "owner", core.RemoteInput)
	known.ID = "known"
	data, err := core.EncodeStored(known)
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO channel_remote_operations(id,owner,turn_id,session_id,resolved,eligible_at,data) VALUES ('future','owner',?,'',0,0,'{\"_work_kind\":\"future\",\"lease\":{\"owner\":99}}'),('known','owner',?,'',0,0,?)", turns[0].ID, turns[0].ID, string(data))
	require.NoError(t, err)
	leased, err := store.LeaseRemoteOperation(ctx, core.RemoteClaim{Owner: "worker", TTL: time.Minute})
	require.NoError(t, err)
	require.NotNil(t, leased)
	require.Equal(t, "known", leased.ID)
}

func TestWorkKindEncryptedUnknownBodyNeverClaimsOrRewritesMetadata(t *testing.T) {
	ctx := context.Background()
	db := lifecycleDB(t)
	codec := newPayloadTestCodec(t)
	options := Options{PayloadCodec: codec, PayloadNamespace: "work-kind", RequirePayloadEncryption: true}
	require.NoError(t, Install(ctx, db, SQLite, options))
	store, err := open(ctx, db, SQLite, options)
	require.NoError(t, err)
	unknown := workKindSettle(t, store, "unknown", 1)[0]
	allowed := workKindSettle(t, store, "allowed", 1)[0]
	var raw string
	require.NoError(t, db.QueryRow("SELECT p.data FROM channel_sensitive_payloads p JOIN channel_payload_refs r ON r.payload_id=p.id WHERE r.record_kind='delivery' AND r.record_id=?", unknown.ID).Scan(&raw))
	var record payload.Record
	require.NoError(t, json.Unmarshal([]byte(raw), &record))
	record.Envelope, err = codec.Seal(ctx, record.Scope, []byte(`{"type":"future_body","private_future":"opaque"}`))
	require.NoError(t, err)
	wire, err := json.Marshal(record)
	require.NoError(t, err)
	_, err = db.Exec("UPDATE channel_sensitive_payloads SET data=? WHERE id=?", string(wire), record.ID)
	require.NoError(t, err)
	_, err = db.Exec("UPDATE channel_deliveries SET inbox_seq=0 WHERE id=?", unknown.ID)
	require.NoError(t, err)
	var before string
	require.NoError(t, db.QueryRow("SELECT data FROM channel_deliveries WHERE id=?", unknown.ID).Scan(&before))
	beforeSeals := codec.seals.Load()
	claimed, err := store.LeaseDelivery(ctx, "worker", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.Equal(t, allowed.ID, claimed.ID)
	require.Equal(t, beforeSeals, codec.seals.Load())
	var after string
	require.NoError(t, db.QueryRow("SELECT data FROM channel_deliveries WHERE id=?", unknown.ID).Scan(&after))
	require.Equal(t, before, after)
}

func TestWorkKindEncryptedOpaquePruneKeepsOnlyUnsupportedSource(t *testing.T) {
	ctx := context.Background()
	db := lifecycleDB(t)
	codec := newPayloadTestCodec(t)
	options := Options{PayloadCodec: codec, PayloadNamespace: "opaque-prune", RequirePayloadEncryption: true}
	require.NoError(t, Install(ctx, db, SQLite, options))
	store, err := open(ctx, db, SQLite, options)
	require.NoError(t, err)
	unknown := workKindSettle(t, store, "unknown", 1)[0]
	allowed := workKindSettle(t, store, "allowed", 1)[0]
	// 先按原协议完成投递，再模拟未来版本已持久化的不可执行密文。
	for range 2 {
		leased, err := store.LeaseDelivery(ctx, "setup", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, leased)
		require.NoError(t, store.CompleteDelivery(ctx, leased.Lease, core.DeliveryResult{Status: core.DeliveryDone}))
	}
	var raw string
	require.NoError(t, db.QueryRow("SELECT p.data FROM channel_sensitive_payloads p JOIN channel_payload_refs r ON r.payload_id=p.id WHERE r.record_kind='delivery' AND r.record_id=?", unknown.ID).Scan(&raw))
	var record payload.Record
	require.NoError(t, json.Unmarshal([]byte(raw), &record))
	record.Envelope, err = codec.Seal(ctx, record.Scope, []byte(`{"type":"future_body","private_future":"opaque"}`))
	require.NoError(t, err)
	wire, err := json.Marshal(record)
	require.NoError(t, err)
	_, err = db.Exec("UPDATE channel_sensitive_payloads SET data=? WHERE id=?", string(wire), record.ID)
	require.NoError(t, err)
	removed, err := store.Prune(ctx, time.Now().Add(time.Hour))
	require.NoError(t, err)
	require.Equal(t, 1, removed.Turns)
	var count int
	require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM channel_deliveries WHERE id=?", unknown.ID).Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM channel_deliveries WHERE id=?", allowed.ID).Scan(&count))
	require.Zero(t, count)
}
