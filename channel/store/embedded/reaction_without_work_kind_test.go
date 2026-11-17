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

func TestEmbeddedReactionDoesNotRequireProtocolHeaderOnRestart(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "encrypted"}[encrypted], func(t *testing.T) {
			clock := conformance.NewClock(time.Now().UTC())
			options := Options{Clock: clock.Now}
			codec := &payloadTestCodec{}
			if encrypted {
				options.PayloadCodec, options.PayloadNamespace, options.RequirePayloadEncryption = codec, "reaction-without-kind", true
			}
			path := filepath.Join(t.TempDir(), "store.db")
			require.NoError(t, Install(path, options))
			store, err := open(path, options)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			message := conformance.P2PMessage("reaction")
			message.TrackReactions = true
			_, err = store.Ingest(t.Context(), conformance.SampleVersion(), message)
			require.NoError(t, err)
			leased, err := store.LeaseDelivery(t.Context(), "first", time.Minute)
			require.NoError(t, err)
			require.NotNil(t, leased)
			require.Equal(t, core.BodyReaction, leased.Body.Type)
			require.NoError(t, store.db.View(func(tx *bbolt.Tx) error {
				var fields map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(tx.Bucket(bucketDeliveries).Get([]byte(leased.ID)), &fields))
				require.JSONEq(t, `"message_reaction_v1"`, string(fields["_work_kind"]))
				return nil
			}))
			seals, opens := codec.counts()
			require.NoError(t, store.CompleteDelivery(t.Context(), leased.Lease, core.DeliveryResult{Status: core.DeliveryPending, NextAttemptAt: clock.Now(), ReactionState: &core.MessageReactionState{Applied: core.ReactionQueued}}))
			afterSeals, afterOpens := codec.counts()
			require.Equal(t, seals, afterSeals)
			require.Equal(t, opens, afterOpens, "checkpoint update must remain metadata-only")
			// 新 reader 不依赖早期 JSON 头，重开不得丢失检查点或重新迁移。
			require.NoError(t, store.db.Update(func(tx *bbolt.Tx) error {
				bucket := tx.Bucket(bucketDeliveries)
				var fields map[string]json.RawMessage
				if e := json.Unmarshal(bucket.Get([]byte(leased.ID)), &fields); e != nil {
					return e
				}
				delete(fields, "_work_kind")
				raw, e := json.Marshal(fields)
				if e != nil {
					return e
				}
				return bucket.Put([]byte(leased.ID), raw)
			}))
			require.NoError(t, store.Close())
			store, err = open(path, options)
			require.NoError(t, err)
			restored, err := store.LeaseDelivery(t.Context(), "restart", time.Minute)
			require.NoError(t, err)
			require.NotNil(t, restored)
			require.Equal(t, leased.ID, restored.ID)
			require.Equal(t, leased.UUID, restored.UUID)
			require.Equal(t, core.ReactionQueued, restored.ReactionState.Applied)
			require.Equal(t, core.BodyReaction, restored.Body.Type)
		})
	}
}

func TestEmbeddedReactionBodyMismatchDoesNotClaim(t *testing.T) {
	store, err := openInstalledFixture(filepath.Join(t.TempDir(), "store.db"), Options{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	message := conformance.P2PMessage("reaction")
	message.TrackReactions = true
	_, err = store.Ingest(t.Context(), conformance.SampleVersion(), message)
	require.NoError(t, err)
	var before string
	require.NoError(t, store.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketDeliveries)
		id, raw := bucket.Cursor().First()
		var fields map[string]json.RawMessage
		if e := json.Unmarshal(raw, &fields); e != nil {
			return e
		}
		fields["body"] = json.RawMessage(`{"type":"text","text":"wrong body"}`)
		next, e := json.Marshal(fields)
		if e != nil {
			return e
		}
		before = string(next)
		return bucket.Put(id, next)
	}))
	leased, err := store.LeaseDelivery(t.Context(), "worker", time.Minute)
	require.NoError(t, err)
	require.Nil(t, leased)
	require.NoError(t, store.db.View(func(tx *bbolt.Tx) error {
		_, raw := tx.Bucket(bucketDeliveries).Cursor().First()
		require.Equal(t, before, string(raw))
		return nil
	}))
}
