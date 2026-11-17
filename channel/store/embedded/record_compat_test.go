// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestEmbeddedRecordUpdatesPreserveFutureFields(t *testing.T) {
	s, err := openInstalledFixture(filepath.Join(t.TempDir(), "future.db"), Options{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	_, err = s.Ingest(t.Context(), conformance.SampleVersion(), conformance.SampleMessage("future"))
	require.NoError(t, err)
	leased, err := s.LeaseTurn(t.Context(), conformance.Claim("current-worker"))
	require.NoError(t, err)
	require.NoError(t, s.db.Update(func(tx *bbolt.Tx) error {
		var record map[string]any
		if e := json.Unmarshal(tx.Bucket(bucketTurns).Get([]byte(leased.Turn.ID)), &record); e != nil {
			return e
		}
		record["future_record"] = map[string]any{"release": "next"}
		record["session"].(map[string]any)["future_session"] = map[string]any{"proof": "preserve"}
		raw, e := json.Marshal(record)
		if e != nil {
			return e
		}
		return tx.Bucket(bucketTurns).Put([]byte(leased.Turn.ID), raw)
	}))
	_, err = s.RenewTurn(t.Context(), leased.Turn.Lease, time.Minute)
	require.NoError(t, err)
	_, err = s.UpdateTurn(t.Context(), leased.Turn.Lease, core.TurnUpdate{Phase: core.PhaseCreate})
	require.NoError(t, err)
	require.NoError(t, s.db.View(func(tx *bbolt.Tx) error {
		raw := string(tx.Bucket(bucketTurns).Get([]byte(leased.Turn.ID)))
		require.Contains(t, raw, "future_record")
		require.Contains(t, raw, "future_session")
		require.Contains(t, raw, "preserve")
		return nil
	}))
}

func TestEmbeddedFutureRecordsRemainVisibleWithoutCurrentDecode(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "encrypted"}[encrypted], func(t *testing.T) {
			options := Options{}
			if encrypted {
				options = Options{PayloadCodec: &payloadTestCodec{}, PayloadNamespace: "opaque", RequirePayloadEncryption: true}
			}
			s, err := openInstalledFixture(filepath.Join(t.TempDir(), "future.db"), options)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, s.Close()) })
			_, err = s.Ingest(t.Context(), conformance.SampleVersion(), conformance.SampleMessage("delivery"))
			require.NoError(t, err)
			leased, err := s.LeaseTurn(t.Context(), conformance.Claim("worker"))
			require.NoError(t, err)
			delivery := core.NewDelivery(core.DeliverySource{Kind: core.SourceTurn, ID: leased.Turn.ID}, 0, "answer", leased.Turn.BindingID, core.ReplyTarget{ChatID: "chat"}, core.TextBody("answer"))
			require.NoError(t, s.SettleTurn(t.Context(), leased.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, []core.Delivery{delivery}))
			input, err := s.Ingest(t.Context(), conformance.SampleVersion(), conformance.SampleMessage("turn"))
			require.NoError(t, err)
			original := map[string]string{}
			require.NoError(t, s.db.Update(func(tx *bbolt.Tx) error {
				for _, item := range []struct {
					bucket []byte
					id     string
				}{{bucketTurns, input.Turn.ID}, {bucketDeliveries, delivery.ID}} {
					var row map[string]any
					if e := json.Unmarshal(tx.Bucket(item.bucket).Get([]byte(item.id)), &row); e != nil {
						return e
					}
					row["_work_kind"] = "future.v2"
					row["id"] = 99
					row["lease"] = map[string]any{"owner": 99}
					row["_payloads"] = map[string]any{"refs": "future-layout"}
					raw, e := json.Marshal(row)
					if e != nil {
						return e
					}
					original[item.id] = string(raw)
					if e = tx.Bucket(item.bucket).Put([]byte(item.id), raw); e != nil {
						return e
					}
				}
				return nil
			}))
			turns, err := s.ListTurns(t.Context(), core.TurnQuery{OpenOnly: true})
			require.NoError(t, err)
			require.Len(t, turns, 1)
			require.Equal(t, input.Turn.ID, turns[0].ID)
			raw, err := core.EncodeStored(turns[0])
			require.NoError(t, err)
			require.Equal(t, original[input.Turn.ID], string(raw))
			deliveries, err := s.ListDeliveries(t.Context(), core.DeliveryQuery{OpenOnly: true})
			require.NoError(t, err)
			require.Len(t, deliveries, 1)
			require.Equal(t, delivery.ID, deliveries[0].ID)
			raw, err = core.EncodeStored(deliveries[0])
			require.NoError(t, err)
			require.Equal(t, original[delivery.ID], string(raw))
			nextTurns, err := s.ListTurns(t.Context(), core.TurnQuery{OpenOnly: true, After: turns[0].ID, Limit: 1})
			require.NoError(t, err)
			require.Empty(t, nextTurns)
			nextDeliveries, err := s.ListDeliveries(t.Context(), core.DeliveryQuery{OpenOnly: true, After: deliveries[0].ID, Limit: 1})
			require.NoError(t, err)
			require.Empty(t, nextDeliveries)
			_, err = core.NewRuntime(s, []core.Connection{{Binding: conformance.SampleBinding(), MAClient: struct{ core.MAClient }{}, Provider: struct{ core.Provider }{}}}, core.RuntimeOptions{})
			require.NoError(t, err, "future work must not prevent a healthy runtime from starting")
		})
	}
}
