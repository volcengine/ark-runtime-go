// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// Reachability comes from live records, including opaque roots, rather than stale link rows.
func TestEncryptedPruneRebuildsPayloadReachability(t *testing.T) {
	for _, mode := range []string{"orphan", "opaque-shared", "malformed", "absent-envelope", "missing-content", "wrong-scope", "opaque-uninspectable"} {
		t.Run(mode, func(t *testing.T) {
			ctx := t.Context()
			clock := conformance.NewClock(time.Now())
			db := openSQLite(t, filepath.Join(t.TempDir(), "gc.db"))
			s, err := openInstalledSQLFixture(ctx, db, SQLite, Options{Clock: clock.Now, PayloadCodec: newPayloadTestCodec(t), PayloadNamespace: "gc", RequirePayloadEncryption: true})
			require.NoError(t, err)
			defer func() { require.NoError(t, s.Close()) }()
			readRaw := func(id string) []byte {
				var raw string
				require.NoError(t, db.QueryRow("SELECT data FROM channel_messages WHERE id=?", id).Scan(&raw))
				return []byte(raw)
			}
			writeRaw := func(id string, raw []byte) {
				_, err := db.Exec("UPDATE channel_messages SET data=? WHERE id=?", string(raw), id)
				require.NoError(t, err)
			}
			payloadCount := func(id string) int {
				var n int
				require.NoError(t, db.QueryRow("SELECT count(*) FROM channel_sensitive_payloads WHERE id=?", id).Scan(&n))
				return n
			}
			dead, err := s.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("dead"))
			require.NoError(t, err)
			lease, err := s.LeaseTurn(ctx, conformance.Claim("worker"))
			require.NoError(t, err)
			require.NoError(t, s.SettleTurn(ctx, lease.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, nil))
			var deadWire struct {
				Payloads struct {
					Refs map[string]string `json:"refs"`
				} `json:"_payloads"`
			}
			require.NoError(t, json.Unmarshal(readRaw(dead.Message.ID), &deadWire))
			deadRef := deadWire.Payloads.Refs["content"]
			require.NotEmpty(t, deadRef)
			liveMessage := conformance.SampleMessage("live")
			liveMessage.ChatID = "live-chat"
			liveMessage.Text = "still-live-private-content"
			live, err := s.Ingest(ctx, conformance.SampleVersion(), liveMessage)
			require.NoError(t, err)
			var liveWire map[string]any
			require.NoError(t, json.Unmarshal(readRaw(live.Message.ID), &liveWire))
			liveRef := liveWire["_payloads"].(map[string]any)["refs"].(map[string]any)["content"].(string)
			switch mode {
			case "orphan":
				_, err = db.Exec("DELETE FROM channel_payload_refs WHERE record_kind='message' AND record_id=?", live.Message.ID)
				require.NoError(t, err)
			case "opaque-shared":
				liveWire["_work_kind"] = "future_protocol"
				liveWire["_payloads"].(map[string]any)["refs"].(map[string]any)["future_shared"] = deadRef
				raw, e := json.Marshal(liveWire)
				require.NoError(t, e)
				writeRaw(live.Message.ID, raw)
			case "opaque-uninspectable":
				delete(liveWire, "_payloads")
				liveWire["_work_kind"] = "future_protocol"
				raw, e := json.Marshal(liveWire)
				require.NoError(t, e)
				writeRaw(live.Message.ID, raw)
			case "absent-envelope":
				delete(liveWire, "_payloads")
				raw, e := json.Marshal(liveWire)
				require.NoError(t, e)
				writeRaw(live.Message.ID, raw)
			case "missing-content", "wrong-scope":
				refs := liveWire["_payloads"].(map[string]any)["refs"].(map[string]any)
				if mode == "missing-content" {
					delete(refs, "content")
				} else {
					refs["content"] = deadRef
				}
				raw, e := json.Marshal(liveWire)
				require.NoError(t, e)
				writeRaw(live.Message.ID, raw)
			case "malformed":
				liveWire["_payloads"] = "future-unreadable-payload-state"
				raw, e := json.Marshal(liveWire)
				require.NoError(t, e)
				writeRaw(live.Message.ID, raw)
			}
			before := string(readRaw(live.Message.ID))
			clock.Advance(time.Hour)
			_, err = s.Prune(ctx, clock.Now())
			if mode == "malformed" || mode == "absent-envelope" || mode == "missing-content" || mode == "wrong-scope" {
				require.Equal(t, 1, payloadCount(liveRef), "live ciphertext must remain recoverable")
				require.Error(t, err)
				require.Equal(t, 1, payloadCount(deadRef))
				return
			}
			require.NoError(t, err)
			require.Equal(t, before, string(readRaw(live.Message.ID)), "maintenance must not rewrite live or opaque records")
			require.Equal(t, 1, payloadCount(liveRef))
			if mode == "opaque-shared" || mode == "opaque-uninspectable" {
				require.Equal(t, 1, payloadCount(deadRef))
				return
			}
			require.Zero(t, payloadCount(deadRef), "no surviving record references the deleted message payload")
			var linked string
			require.NoError(t, db.QueryRow("SELECT payload_id FROM channel_payload_refs WHERE record_kind='message' AND record_id=? AND field='content'", live.Message.ID).Scan(&linked))
			require.Equal(t, liveRef, linked)
			duplicate, err := s.Ingest(ctx, conformance.SampleVersion(), liveMessage)
			require.NoError(t, err)
			require.True(t, duplicate.Duplicate)
			require.Equal(t, liveMessage.Text, duplicate.Message.Message.Text)
		})
	}
}

func TestEncryptedPrunePreservesOpaqueSettledChild(t *testing.T) {
	for _, mode := range []string{"message", "interaction", "absent-message", "absent-interaction", "missing-preview"} {
		t.Run(mode, func(t *testing.T) {
			ctx := t.Context()
			clock := conformance.NewClock(time.Now())
			db := openSQLite(t, filepath.Join(t.TempDir(), "gc.db"))
			s, err := openInstalledSQLFixture(ctx, db, SQLite, Options{Clock: clock.Now, PayloadCodec: newPayloadTestCodec(t), PayloadNamespace: "gc", RequirePayloadEncryption: true})
			require.NoError(t, err)
			defer func() { require.NoError(t, s.Close()) }()
			table := "channel_messages"
			if mode == "interaction" || mode == "absent-interaction" || mode == "missing-preview" {
				table = "channel_interactions"
			}
			readRaw := func(id string) []byte {
				var raw string
				require.NoError(t, db.QueryRow("SELECT data FROM "+table+" WHERE id=?", id).Scan(&raw))
				return []byte(raw)
			}
			writeRaw := func(id string, raw []byte) {
				_, err := db.Exec("UPDATE "+table+" SET data=? WHERE id=?", string(raw), id)
				require.NoError(t, err)
			}
			payloadCount := func(id string) int {
				var n int
				require.NoError(t, db.QueryRow("SELECT count(*) FROM channel_sensitive_payloads WHERE id=?", id).Scan(&n))
				return n
			}
			dead, err := s.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("dead"))
			require.NoError(t, err)
			lease, err := s.LeaseTurn(ctx, conformance.Claim("worker"))
			require.NoError(t, err)
			childID, field := dead.Message.ID, "content"
			if mode == "interaction" || mode == "absent-interaction" || mode == "missing-preview" {
				input := core.Interaction{ID: core.InteractionID(lease.Turn.ID, "tool"), TurnID: lease.Turn.ID, Kind: core.InteractionApproval, Status: core.InteractionPending, ToolUseID: "tool", ApproverID: "user", ConfirmEventID: "confirm", BindingID: lease.Turn.BindingID, AppID: "app", TenantKey: "tenant", ExpiresAt: clock.Now().Add(time.Hour), InputPreview: "OPAQUE_APPROVAL_PRIVATE"}
				_, err = s.PutInteraction(ctx, lease.Turn.Lease, input)
				require.NoError(t, err)
				childID, field = input.ID, "preview"
			}
			require.NoError(t, s.SettleTurn(ctx, lease.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, nil))
			var deadWire struct {
				Payloads struct {
					Refs map[string]string `json:"refs"`
				} `json:"_payloads"`
			}
			require.NoError(t, json.Unmarshal(readRaw(childID), &deadWire))
			deadRef := deadWire.Payloads.Refs[field]
			require.NotEmpty(t, deadRef)

			var deadRoot map[string]any
			require.NoError(t, json.Unmarshal(readRaw(childID), &deadRoot))
			switch mode {
			case "absent-message", "absent-interaction":
				delete(deadRoot, "_payloads")
			case "missing-preview":
				delete(deadRoot["_payloads"].(map[string]any)["refs"].(map[string]any), "preview")
			default:
				deadRoot["_work_kind"] = "future_protocol"
			}
			raw, err := json.Marshal(deadRoot)
			require.NoError(t, err)
			writeRaw(childID, raw)
			clock.Advance(time.Hour)
			_, err = s.Prune(ctx, clock.Now())
			if mode == "absent-message" || mode == "absent-interaction" || mode == "missing-preview" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, 1, payloadCount(deadRef), "opaque child ciphertext must remain recoverable")
		})
	}
}
