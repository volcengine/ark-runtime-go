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
)

func embeddedTurnRaw(t *testing.T, s *store, id string) []byte {
	t.Helper()
	var raw []byte
	require.NoError(t, s.db.View(func(tx *bbolt.Tx) error {
		raw = bytes.Clone(tx.Bucket(bucketTurns).Get([]byte(id)))
		return nil
	}))
	return raw
}

func TestEmbeddedQueueDeadlineDoesNotCrossOpaqueInputHead(t *testing.T) {
	for _, field := range []string{"input", "blocks", "mounts"} {
		t.Run(field, func(t *testing.T) {
			clock := conformance.NewClock(time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC))
			s, err := openInstalledFixture(filepath.Join(t.TempDir(), "opaque-head.db"), Options{Clock: clock.Now, QueueTTL: time.Minute})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, s.Close()) })
			head, err := s.Ingest(t.Context(), conformance.SampleVersion(), conformance.SampleMessage("head"))
			require.NoError(t, err)
			tail, err := s.Ingest(t.Context(), conformance.SampleVersion(), conformance.SampleMessage("tail"))
			require.NoError(t, err)
			require.NoError(t, s.db.Update(func(tx *bbolt.Tx) error {
				r, err := getTurn(tx, head.Turn.ID)
				if err != nil {
					return err
				}
				old := *r
				r.QueueDeadlineAt = clock.Now().Add(time.Hour)
				r.Schedule.NextAttemptAt = clock.Now().Add(time.Hour)
				if err = s.writeTurn(tx, &old, *r); err != nil {
					return err
				}
				var wire map[string]any
				if err = json.Unmarshal(tx.Bucket(bucketTurns).Get([]byte(r.ID)), &wire); err != nil {
					return err
				}
				switch field {
				case "input":
					wire["input"] = map[string]any{"_work_kind": "future-input", "blocks": []any{"future-shape"}}
				case "blocks":
					wire["input"] = map[string]any{"blocks": []any{map[string]any{"_work_kind": "future-block", "file_id": []int{1, 2}}}}
				case "mounts":
					wire["input"] = map[string]any{"mounts": []any{map[string]any{"_work_kind": "future-mount", "file_id": []int{1, 2}}}}
				}
				raw, err := json.Marshal(wire)
				if err != nil {
					return err
				}
				return tx.Bucket(bucketTurns).Put([]byte(r.ID), raw)
			}))
			headBefore := embeddedTurnRaw(t, s, head.Turn.ID)
			tailBefore := embeddedTurnRaw(t, s, tail.Turn.ID)
			var decoded core.Turn
			require.NoError(t, core.DecodeStored(headBefore, &decoded))
			switch field {
			case "input":
				require.True(t, decoded.Input.IsOpaqueStored())
			case "blocks":
				require.True(t, decoded.Input.Blocks[0].IsOpaqueStored())
			case "mounts":
				require.True(t, decoded.Input.Mounts[0].IsOpaqueStored())
			}
			clock.Advance(2 * time.Minute)
			require.NoError(t, s.db.View(func(tx *bbolt.Tx) error {
				_, id := tx.Bucket(bucketQueueDeadline).Cursor().First()
				require.Equal(t, tail.Turn.ID, string(id), "到期候选必须是队尾，不能依赖队首自身到期")
				return nil
			}))
			claimed, err := s.LeaseTurn(t.Context(), conformance.Claim("worker"))
			require.NoError(t, err)
			require.Nil(t, claimed, "到期队尾不能越过含未知输入协议的队首")
			other := conformance.SampleMessage("other")
			other.ChatID = "other-conversation"
			healthy, err := s.Ingest(t.Context(), conformance.SampleVersion(), other)
			require.NoError(t, err)
			claimed, err = s.LeaseTurn(t.Context(), conformance.Claim("worker"))
			require.NoError(t, err)
			require.NotNil(t, claimed)
			require.Equal(t, healthy.Turn.ID, claimed.Turn.ID)
			require.Equal(t, headBefore, embeddedTurnRaw(t, s, head.Turn.ID))
			require.Equal(t, tailBefore, embeddedTurnRaw(t, s, tail.Turn.ID))
		})
	}
}

func TestEmbeddedTypedTurnIdentityMustMatchBoltKey(t *testing.T) {
	for _, state := range []string{"queued", "running"} {
		t.Run(state, func(t *testing.T) {
			s, err := openInstalledFixture(filepath.Join(t.TempDir(), "identity.db"), Options{})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, s.Close()) })
			accepted, err := s.Ingest(t.Context(), conformance.SampleVersion(), conformance.SampleMessage("identity"))
			require.NoError(t, err)
			var lease core.Lease
			if state == "running" {
				claimed, err := s.LeaseTurn(t.Context(), conformance.Claim("worker"))
				require.NoError(t, err)
				require.NotNil(t, claimed)
				lease = claimed.Turn.Lease
			}
			const foreignID = "different-typed-id"
			require.NoError(t, s.db.Update(func(tx *bbolt.Tx) error {
				var wire map[string]any
				if err := json.Unmarshal(tx.Bucket(bucketTurns).Get([]byte(accepted.Turn.ID)), &wire); err != nil {
					return err
				}
				wire["id"] = foreignID
				raw, err := json.Marshal(wire)
				if err != nil {
					return err
				}
				return tx.Bucket(bucketTurns).Put([]byte(accepted.Turn.ID), raw)
			}))
			before := embeddedTurnRaw(t, s, accepted.Turn.ID)
			if state == "queued" {
				claimed, err := s.LeaseTurn(t.Context(), conformance.Claim("worker"))
				require.NoError(t, err)
				require.Nil(t, claimed, "typed ID 不得重定向 Bolt 主键")
			} else {
				_, err = s.UpdateTurn(t.Context(), lease, core.TurnUpdate{Phase: core.PhaseCreate})
				require.ErrorIs(t, err, core.ErrLeaseLost)
			}
			require.Equal(t, before, embeddedTurnRaw(t, s, accepted.Turn.ID))
			require.Empty(t, embeddedTurnRaw(t, s, foreignID), "不得按不可信 typed ID 新建记录")
		})
	}
}
