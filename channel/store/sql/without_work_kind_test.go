// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// removeFixtureWorkKind 只撤掉尚未发布的列，完整保留业务表和维护表。
func removeFixtureWorkKind(t *testing.T, db *sql.DB) {
	t.Helper()
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='index' AND sql LIKE '%work_kind%'")
	require.NoError(t, err)
	var indexes []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		indexes = append(indexes, name)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	for _, name := range indexes {
		_, err = db.Exec("DROP INDEX " + name)
		require.NoError(t, err)
	}
	rows, err = db.Query("SELECT name,sql FROM sqlite_master WHERE type='table'")
	require.NoError(t, err)
	var tables []string
	for rows.Next() {
		var name, ddl string
		require.NoError(t, rows.Scan(&name, &ddl))
		if strings.Contains(ddl, "work_kind") {
			tables = append(tables, name)
		}
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	for _, table := range tables {
		_, err = db.Exec("ALTER TABLE " + table + " DROP COLUMN work_kind")
		require.NoError(t, err)
	}
}

func TestBusinessStoreWithoutWorkKind(t *testing.T) {
	for _, mode := range []string{"plaintext", "encrypted", "extra_legacy_column"} {
		t.Run(mode, func(t *testing.T) {
			factory := func(t *testing.T, c conformance.StoreConfig) conformance.TestStore {
				db := lifecycleDB(t)
				options := Options{Clock: c.Clock, MaxChannelOpenTurns: c.MaxChannelOpenTurns, MaxRejectedMessages: c.MaxRejectedMessages, RejectNoticeInterval: c.RejectNoticeInterval, MaxOpenTurns: c.MaxOpenTurns, MaxMessageBytes: c.MaxMessageBytes, QueueTTL: c.QueueTTL}
				if mode == "encrypted" {
					options.PayloadCodec = newPayloadTestCodec(t)
					options.PayloadNamespace = "without-kind"
					options.RequirePayloadEncryption = true
				}
				require.NoError(t, Install(context.Background(), db, SQLite, options))
				removeFixtureWorkKind(t, db)
				var err error
				if mode == "extra_legacy_column" {
					for _, table := range []string{"channel_turns", "channel_deliveries", "channel_remote_operations"} {
						_, err = db.Exec("ALTER TABLE " + table + " ADD COLUMN work_kind VARCHAR(32) NOT NULL DEFAULT 'legacy'")
						require.NoError(t, err)
					}
				}
				require.ErrorIs(t, Install(context.Background(), db, SQLite, options), core.ErrConflict)
				store, err := open(context.Background(), db, SQLite, options)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, store.Close()) })
				t.Cleanup(func() {
					for _, table := range []string{"channel_turns", "channel_deliveries", "channel_remote_operations"} {
						rows, err := db.Query("SELECT work_kind FROM " + table)
						if mode != "extra_legacy_column" {
							require.Error(t, err)
							continue
						}
						require.NoError(t, err)
						for rows.Next() {
							var kind string
							require.NoError(t, rows.Scan(&kind))
							require.Equal(t, "legacy", kind)
						}
						require.NoError(t, rows.Err())
						require.NoError(t, rows.Close())
					}
				})
				if mode == "encrypted" {
					t.Cleanup(func() {
						for _, table := range []string{"channel_messages", "channel_turns", "channel_deliveries", "channel_sensitive_payloads"} {
							rows, err := db.Query("SELECT data FROM " + table)
							require.NoError(t, err)
							for rows.Next() {
								var data string
								require.NoError(t, rows.Scan(&data))
								require.NotContains(t, data, "schema-secret-input")
								require.NotContains(t, data, "schema-secret-output")
							}
							require.NoError(t, rows.Err())
							require.NoError(t, rows.Close())
						}
					})
				}
				return store
			}
			testWorkKindSchemaWrites(t, factory)
		})
	}
}

// testWorkKindSchemaWrites 覆盖依赖 SQL 列布局的关键写路径；完整领域契约由普通/加密套件负责。
func testWorkKindSchemaWrites(t *testing.T, factory conformance.StoreFactory) {
	ctx := context.Background()
	clock := conformance.NewClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	s := factory(t, conformance.StoreConfig{Clock: clock.Now})
	message := conformance.SampleMessage("schema")
	message.Text = "schema-secret-input"
	accepted, err := s.Ingest(ctx, conformance.SampleVersion(), message)
	require.NoError(t, err)
	duplicate, err := s.Ingest(ctx, conformance.SampleVersion(), message)
	require.NoError(t, err)
	require.True(t, duplicate.Duplicate)
	first, err := s.LeaseTurn(ctx, conformance.Claim("old"))
	require.NoError(t, err)
	require.NotNil(t, first)
	require.Equal(t, message.Text, first.Message.Message.Text)
	clock.Advance(2 * time.Minute)
	current, err := s.LeaseTurn(ctx, conformance.Claim("new"))
	require.NoError(t, err)
	require.NotNil(t, current)
	require.Greater(t, current.Turn.Lease.Fence, first.Turn.Lease.Fence)
	_, err = s.UpdateTurn(ctx, first.Turn.Lease, core.TurnUpdate{Phase: core.PhaseCreate})
	require.ErrorIs(t, err, core.ErrLeaseLost)
	_, err = s.UpdateTurn(ctx, current.Turn.Lease, core.TurnUpdate{Phase: core.PhaseSend, Session: core.TurnSession{Generation: 1, SessionID: "schema-session"}, Submit: core.Submission{WriteAttempts: 1, SubmittedAt: clock.Now()}, RemoteIntent: &core.RemoteIntent{Kind: core.RemoteInput, Limit: 1}})
	require.NoError(t, err)
	delivery := core.NewDelivery(core.DeliverySource{Kind: core.SourceTurn, ID: current.Turn.ID}, 0, "answer", current.Turn.BindingID, core.ReplyTarget{Mode: "reply", ChatID: "chat"}, core.TextBody("schema-secret-output"))
	require.NoError(t, s.SettleTurn(ctx, current.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, []core.Delivery{delivery}))
	remote := s.(core.RemoteOperationStore)
	op, err := remote.LeaseRemoteOperation(ctx, core.RemoteClaim{Owner: "old", TTL: time.Minute})
	require.NoError(t, err)
	require.NotNil(t, op)
	old := op.Lease
	clock.Advance(2 * time.Minute)
	op, err = remote.LeaseRemoteOperation(ctx, core.RemoteClaim{Owner: "new", TTL: time.Minute})
	require.NoError(t, err)
	require.NotNil(t, op)
	require.Greater(t, op.Lease.Fence, old.Fence)
	_, err = remote.UpdateRemoteOperation(ctx, old, core.RemoteUpdate{Proof: &core.RemoteProof{Kind: core.RemoteGone, Reference: "gone"}})
	require.ErrorIs(t, err, core.ErrLeaseLost)
	_, err = remote.UpdateRemoteOperation(ctx, op.Lease, core.RemoteUpdate{Proof: &core.RemoteProof{Kind: core.RemoteGone, Reference: "gone"}})
	require.NoError(t, err)

	got, err := s.GetTurn(ctx, accepted.Turn.ID)
	require.NoError(t, err)
	require.Equal(t, core.TurnDone, got.Status)
	out, err := s.LeaseDelivery(ctx, "sender", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, out)
	require.Equal(t, delivery.Body, out.Body)
	require.NoError(t, s.CompleteDelivery(ctx, out.Lease, core.DeliveryResult{Status: core.DeliveryDone}))
	require.ErrorIs(t, s.CompleteDelivery(ctx, out.Lease, core.DeliveryResult{Status: core.DeliveryDone}), core.ErrLeaseLost)
}

func TestQueueDeadlineDoesNotCrossUnsupportedPredecessor(t *testing.T) {
	for _, mutation := range []string{"header", "snapshot", "phase", "input", "blocks", "mounts", "tail_identity"} {
		t.Run(mutation, func(t *testing.T) {
			ctx := context.Background()
			clock := conformance.NewClock(time.Now().UTC())
			db := lifecycleDB(t)
			options := Options{Clock: clock.Now, QueueTTL: time.Minute}
			require.NoError(t, Install(ctx, db, SQLite, options))
			store, err := open(ctx, db, SQLite, options)
			require.NoError(t, err)
			head, err := store.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("head"))
			require.NoError(t, err)
			tail, err := store.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("tail"))
			require.NoError(t, err)
			var raw string
			require.NoError(t, db.QueryRow("SELECT data FROM channel_turns WHERE id=?", head.Turn.ID).Scan(&raw))
			var object map[string]any
			require.NoError(t, json.Unmarshal([]byte(raw), &object))
			switch mutation {
			case "header", "tail_identity":
				object["_work_kind"] = "future"
			case "snapshot":
				object["snapshot"] = map[string]any{"schema_version": 99}
			case "phase":
				object["phase"] = "future"
			case "input":
				object["input"] = map[string]any{"_work_kind": "future-input", "blocks": []any{"future-shape"}}
			case "blocks":
				object["input"] = map[string]any{"blocks": []any{map[string]any{"_work_kind": "future-block", "file_id": []int{1, 2}}}}
			case "mounts":
				object["input"] = map[string]any{"mounts": []any{map[string]any{"_work_kind": "future-mount", "file_id": []int{1, 2}}}}
			}
			wire, err := json.Marshal(object)
			require.NoError(t, err)
			var decoded core.Turn
			require.NoError(t, core.DecodeStored(wire, &decoded))
			switch mutation {
			case "input":
				require.True(t, decoded.Input.IsOpaqueStored())
			case "blocks":
				require.True(t, decoded.Input.Blocks[0].IsOpaqueStored())
			case "mounts":
				require.True(t, decoded.Input.Mounts[0].IsOpaqueStored())
			}
			_, err = db.Exec("UPDATE channel_turns SET data=?,eligible_at=? WHERE id=?", string(wire), clock.Now().Add(time.Hour).UnixNano(), head.Turn.ID)
			require.NoError(t, err)
			if mutation == "tail_identity" {
				var tailRaw string
				require.NoError(t, db.QueryRow("SELECT data FROM channel_turns WHERE id=?", tail.Turn.ID).Scan(&tailRaw))
				var tailObject map[string]any
				require.NoError(t, json.Unmarshal([]byte(tailRaw), &tailObject))
				tailObject["id"] = head.Turn.ID
				tailWire, err := json.Marshal(tailObject)
				require.NoError(t, err)
				_, err = db.Exec("UPDATE channel_turns SET data=? WHERE id=?", string(tailWire), tail.Turn.ID)
				require.NoError(t, err)
			}
			var tailBefore string
			require.NoError(t, db.QueryRow("SELECT data FROM channel_turns WHERE id=?", tail.Turn.ID).Scan(&tailBefore))
			clock.Advance(2 * time.Minute)
			claim, err := store.LeaseTurn(ctx, conformance.Claim("worker"))
			require.NoError(t, err)
			if claim != nil {
				t.Errorf("expired tail crossed unsupported predecessor: %s", claim.Turn.ID)
			}
			var after string
			require.NoError(t, db.QueryRow("SELECT data FROM channel_turns WHERE id=?", head.Turn.ID).Scan(&after))
			require.Equal(t, string(wire), after)
			require.NoError(t, db.QueryRow("SELECT data FROM channel_turns WHERE id=?", tail.Turn.ID).Scan(&after))
			require.Equal(t, tailBefore, after)
			safe := conformance.SampleMessage("other-conversation")
			safe.ChatID = "other-conversation"
			accepted, err := store.Ingest(ctx, conformance.SampleVersion(), safe)
			require.NoError(t, err)
			claim, err = store.LeaseTurn(ctx, conformance.Claim("worker"))
			require.NoError(t, err)
			require.NotNil(t, claim)
			require.Equal(t, accepted.Turn.ID, claim.Turn.ID)
			require.NoError(t, db.QueryRow("SELECT data FROM channel_turns WHERE id=?", head.Turn.ID).Scan(&after))
			require.Equal(t, string(wire), after)
			require.NoError(t, db.QueryRow("SELECT data FROM channel_turns WHERE id=?", tail.Turn.ID).Scan(&after))
			require.Equal(t, tailBefore, after)
		})
	}
}

func TestReactionSourceBodyPairIsRequiredForClaim(t *testing.T) {
	for _, mutation := range []string{"reaction_text", "answer_reaction"} {
		t.Run(mutation, func(t *testing.T) {
			ctx := context.Background()
			db, store := workKindStore(t)
			delivery := workKindSettle(t, store, "mismatched", 1)[0]
			var raw string
			require.NoError(t, db.QueryRow("SELECT data FROM channel_deliveries WHERE id=?", delivery.ID).Scan(&raw))
			var object map[string]any
			require.NoError(t, json.Unmarshal([]byte(raw), &object))
			source := "turn"
			if mutation == "reaction_text" {
				source = "reaction"
				object["source"] = map[string]any{"kind": "reaction", "id": delivery.Source.ID}
			} else {
				object["body"] = map[string]any{"type": "reaction"}
			}
			wire, err := json.Marshal(object)
			require.NoError(t, err)
			_, err = db.Exec("UPDATE channel_deliveries SET source_kind=?,data=? WHERE id=?", source, string(wire), delivery.ID)
			require.NoError(t, err)
			claim, err := store.LeaseDelivery(ctx, "worker", time.Minute)
			require.NoError(t, err)
			require.Nil(t, claim)
		})
	}
}

func TestRemoteOperationDoesNotClaimUnsupportedTurn(t *testing.T) {
	ctx := context.Background()
	db, store := workKindStore(t)
	workKindSettle(t, store, "remote-future-turn", 0)
	turns, err := store.ListTurns(ctx, core.TurnQuery{})
	require.NoError(t, err)
	require.Len(t, turns, 1)
	operation := core.NewRemoteOperation(turns[0], "owner", core.RemoteInput)
	data, err := core.EncodeStored(operation)
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO channel_remote_operations(id,owner,turn_id,session_id,resolved,eligible_at,data) VALUES (?,'owner',?,'',0,0,?)", operation.ID, turns[0].ID, string(data))
	require.NoError(t, err)
	var raw string
	require.NoError(t, db.QueryRow("SELECT data FROM channel_turns WHERE id=?", turns[0].ID).Scan(&raw))
	var object map[string]any
	require.NoError(t, json.Unmarshal([]byte(raw), &object))
	object["_work_kind"] = "future"
	wire, err := json.Marshal(object)
	require.NoError(t, err)
	_, err = db.Exec("UPDATE channel_turns SET data=? WHERE id=?", string(wire), turns[0].ID)
	require.NoError(t, err)
	claimed, err := store.LeaseRemoteOperation(ctx, core.RemoteClaim{Owner: "worker", TTL: time.Minute})
	require.NoError(t, err)
	require.Nil(t, claimed)
}

// TestSQLiteEncryptedRemoteOperationConformance 保留从 schema 矩阵移出的加密远端完整契约。
func TestSQLiteEncryptedRemoteOperationConformance(t *testing.T) {
	factory := func(t *testing.T, c conformance.StoreConfig) conformance.TestStore {
		db := lifecycleDB(t)
		options := Options{Clock: c.Clock, PayloadCodec: newPayloadTestCodec(t), PayloadNamespace: "remote-conformance", RequirePayloadEncryption: true}
		require.NoError(t, Install(context.Background(), db, SQLite, options))
		store, err := open(context.Background(), db, SQLite, options)
		require.NoError(t, err)
		return store
	}
	conformance.RunRemoteOperationStore(t, factory)
	conformance.RunRemoteOperationFences(t, factory)
}
