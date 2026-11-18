// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/payload"
)

func TestEmbeddedUnsupportedDeliveryDoesNotBlockOtherSources(t *testing.T) {
	for _, future := range []string{"kind", "body", "encrypted_body"} {
		t.Run(future, func(t *testing.T) {
			options := Options{}
			codec := &payloadTestCodec{}
			if future == "encrypted_body" {
				options.PayloadCodec = codec
				options.PayloadNamespace = "future-body"
				options.RequirePayloadEncryption = true
			}
			s, err := openInstalledFixture(filepath.Join(t.TempDir(), "kind.db"), options)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, s.Close()) })
			create := func(chat string) core.Delivery {
				m := conformance.SampleMessage(chat)
				m.ChatID = chat
				_, e := s.Ingest(t.Context(), conformance.SampleVersion(), m)
				require.NoError(t, e)
				turn, e := s.LeaseTurn(t.Context(), conformance.Claim("turn-worker"))
				require.NoError(t, e)
				require.NotNil(t, turn)
				d := core.NewDelivery(core.DeliverySource{Kind: core.SourceTurn, ID: turn.Turn.ID}, 0, "answer", turn.Turn.BindingID, core.ReplyTarget{ChatID: chat}, core.TextBody(chat))
				attachment := core.NewDelivery(d.Source, 1, "attachment", d.BindingID, d.ReplyTarget, core.ArtifactBody(core.Artifact{Kind: "document", FileID: "file"}))
				require.NoError(t, s.SettleTurn(t.Context(), turn.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, []core.Delivery{d, attachment}))
				return d
			}
			unsupported := create("first")
			if future == "encrypted_body" {
				var sealed payload.Record
				require.NoError(t, s.db.View(func(tx *bbolt.Tx) error {
					r, e := getDelivery(tx, unsupported.ID)
					if e != nil {
						return e
					}
					return json.Unmarshal(tx.Bucket(bucketPayloads).Get([]byte(r.Payloads.Refs["body"])), &sealed)
				}))
				sealed.Envelope, err = codec.Seal(t.Context(), sealed.Scope, []byte(`{"type":"future_body","future":"encrypted-preserve"}`))
				require.NoError(t, err)
				require.NoError(t, s.db.Update(func(tx *bbolt.Tx) error {
					raw, e := json.Marshal(sealed)
					if e != nil {
						return e
					}
					return tx.Bucket(bucketPayloads).Put([]byte(sealed.ID), raw)
				}))
			}
			require.NoError(t, s.db.Update(func(tx *bbolt.Tx) error {
				b := tx.Bucket(bucketDeliveries)
				var row map[string]any
				if e := json.Unmarshal(b.Get([]byte(unsupported.ID)), &row); e != nil {
					return e
				}
				switch future {
				case "kind":
					row["_work_kind"] = "future.v1"
				case "body":
					row["body"] = map[string]any{"type": "future_body", "future": "preserve"}
				}
				raw, e := json.Marshal(row)
				if e != nil {
					return e
				}
				return b.Put([]byte(unsupported.ID), raw)
			}))
			other := create("second")
			require.NoError(t, s.update(t.Context(), func(tx storeTx) error {
				r, e := getDelivery(tx, other.ID)
				if e != nil {
					return e
				}
				old := *r
				r.NextAttemptAt = time.Now().Add(-time.Hour)
				return s.writeDelivery(tx, &old, *r)
			}))
			require.NoError(t, s.db.View(func(tx *bbolt.Tx) error {
				_, id := tx.Bucket(bucketDeliveryReady).Cursor().First()
				require.Equal(t, unsupported.ID, string(id), "fixture must put the unsupported source first")
				return nil
			}))
			leased, err := s.LeaseDelivery(t.Context(), "sender", time.Minute)
			require.NoError(t, err)
			require.NotNil(t, leased)
			require.Equal(t, other.ID, leased.ID)
			require.NoError(t, s.db.View(func(tx *bbolt.Tx) error {
				var row map[string]any
				require.NoError(t, json.Unmarshal(tx.Bucket(bucketDeliveries).Get([]byte(unsupported.ID)), &row))
				require.Zero(t, row["attempt"], "unsupported task must not be claimed")
				return nil
			}))
			if future == "encrypted_body" {
				require.NoError(t, s.update(t.Context(), func(tx storeTx) error {
					prefix := sourcePrefix(unsupported.Source)
					c := tx.Bucket(bucketDeliveryBySrc).Cursor()
					for k, id := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, id = c.Next() {
						r, e := getDelivery(tx, string(id))
						if e != nil {
							return e
						}
						old := *r
						r.Status = core.DeliveryDone
						if e = s.writeDelivery(tx, &old, *r); e != nil {
							return e
						}
					}
					return nil
				}))
				_, err = s.Prune(t.Context(), time.Now().Add(48*time.Hour))
				require.NoError(t, err)
				require.NoError(t, s.db.View(func(tx *bbolt.Tx) error {
					require.NotEmpty(t, tx.Bucket(bucketDeliveries).Get([]byte(unsupported.ID)), "prune must preserve an unknown encrypted body and its refs")
					return nil
				}))
			}
		})
	}
}
