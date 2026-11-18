// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDecodeStoredFutureProtocolSkipsTypedBusinessShape(t *testing.T) {
	raw := []byte("{\n  \"_work_kind\": \"future_turn_v2\", \"id\":\"future-turn\", \"binding_id\":\"future-binding\", \"conversation_id\":\"future-route\", \"lease\":{\"owner\":99}, \"snapshot\":\"v2\", \"schedule\":[1,2], \"status\":{\"v\":2}\n}")
	var turn Turn
	require.NoError(t, DecodeStored(raw, &turn), "未知协议不得先typed解码lease/snapshot/schedule")
	require.Equal(t, "future-turn", turn.ID)
	require.Equal(t, "future-binding", turn.BindingID)
	require.Equal(t, RouteKey("future-route"), turn.ConversationID)
	require.Equal(t, Lease{}, turn.Lease)
	require.Nil(t, turn.Snapshot)
	encoded, err := EncodeStored(turn)
	require.NoError(t, err)
	require.Equal(t, raw, encoded, "opaque整记录必须逐字保留")
	require.ErrorIs(t, turnWorkError(turn), ErrUnsupportedWork)
	turn.ID = "view-only"
	encoded, err = EncodeStored(turn)
	require.NoError(t, err)
	require.Equal(t, raw, encoded, "metadata视图不能改写未知协议原文")
}

func TestDecodeStoredFutureDeliverySkipsCustomUnionShape(t *testing.T) {
	raw := []byte(`{"_work_kind":"future_delivery_v2","id":"future-delivery","binding_id":"future-binding","body":[1,2],"source":99,"lease":{"fence":"v2"},"_payloads":"new-envelope"}`)
	var delivery Delivery
	require.NoError(t, DecodeStored(raw, &delivery))
	require.Equal(t, "future-delivery", delivery.ID)
	require.Nil(t, delivery.Payloads)
	require.Empty(t, delivery.Body.Text)
	encoded, err := EncodeStored(delivery)
	require.NoError(t, err)
	require.Equal(t, raw, encoded)
	require.ErrorIs(t, deliveryWorkError(delivery), ErrUnsupportedWork)
	newValue := NewDelivery(DeliverySource{Kind: SourceTurn, ID: "turn"}, 0, "answer", "binding", ReplyTarget{}, TextBody("answer"))
	require.True(t, newValue.ValidNew(newValue.Source))
	newValue.StoredRecord = delivery.StoredRecord
	require.False(t, newValue.ValidNew(newValue.Source), "opaque状态不能升级为新任务")
}

type opaqueStartupStore struct{ unsupportedWorkStore }

func (*opaqueStartupStore) ListTurns(context.Context, TurnQuery) ([]Turn, error) {
	var turn Turn
	err := DecodeStored([]byte(`{"_work_kind":"future","id":"t","binding_id":"unregistered","lease":{"owner":99}}`), &turn)
	return []Turn{turn}, err
}

func (*opaqueStartupStore) ListDeliveries(context.Context, DeliveryQuery) ([]Delivery, error) {
	var delivery Delivery
	err := DecodeStored([]byte(`{"_work_kind":"future","id":"d","binding_id":"unregistered","body":[1,2],"lease":{"owner":99}}`), &delivery)
	return []Delivery{delivery}, err
}

func TestRuntimeStartupEnumeratesFutureShapesWithoutGlobalFailure(t *testing.T) {
	store := &opaqueStartupStore{}
	runtime, _, observer := unsupportedRuntime(t, &store.unsupportedWorkStore)
	runtime.store = store
	require.NoError(t, runtime.validatePersisted(context.Background()))
	require.Equal(t, int32(2), observer.blocked.Load())
	require.Zero(t, store.writes.Load())
}

func TestOpaqueStateCannotSatisfyFrozenValidation(t *testing.T) {
	var opaque Turn
	require.NoError(t, DecodeStored([]byte(`{"_work_kind":"future","id":"t"}`), &opaque))
	version := NewBindingVersion(Binding{ID: "binding"})
	version.StoredRecord = opaque.StoredRecord
	require.False(t, version.Valid())
	policy := NewTurnSnapshot(Binding{ID: "binding"}, time.Time{})
	policy.StoredRecord = opaque.StoredRecord
	require.False(t, policy.Valid())
	session := NewSessionSnapshot("route", 1, CreateSessionSpec{Binding: Binding{ID: "binding"}, CreationKey: "create", LookupKey: "lookup"})
	session.StoredRecord = opaque.StoredRecord
	require.False(t, session.Valid())
}

func TestDecodeOpaqueStoredColumnProtocolAndRawReadOnly(t *testing.T) {
	raw := []byte(" {\"id\":99,\"binding_id\":\"binding\",\"lease\":[1,2],\"body\":{\"secret\":\"private-future\"},\"_payloads\":{\"refs\":{\"new\":[1,2]}}} \n")
	var delivery Delivery
	require.NoError(t, DecodeOpaqueStored(raw, &delivery, "future-column-v2"))
	require.True(t, delivery.IsOpaqueStored())
	require.Equal(t, "future-column-v2", delivery.StoredWorkKind())
	require.Empty(t, delivery.ID, "未知形态ID必须由Store真实行键填视图，不能猜测")
	require.Equal(t, "binding", delivery.BindingID)
	require.Nil(t, delivery.Payloads)
	public, err := json.Marshal(delivery)
	require.NoError(t, err)
	require.NotContains(t, string(public), "private-future")
	delivery.ForgetStoredFields("body", "_payloads")
	delivery.ID = "authoritative-row-key"
	encoded, err := EncodeStored(delivery)
	require.NoError(t, err)
	require.Equal(t, raw, encoded)
	encoded[0] = '['
	second, err := EncodeStored(delivery)
	require.NoError(t, err)
	require.Equal(t, raw, second, "编码结果不得引用私有raw字节")
	require.ErrorIs(t, DecodeOpaqueStored(raw, &delivery, WorkKindLegacy), ErrInvalid)
	require.ErrorIs(t, DecodeOpaqueStored(raw, &delivery, ""), ErrInvalid)
	require.Error(t, DecodeStored([]byte(`{"_work_kind":"legacy","lease":{"owner":99}}`), &Turn{}), "支持的协议仍严格验形态")
}

func TestOpaqueStoredAnonymousAndContainerRecords(t *testing.T) {
	type turnRecord struct {
		*Turn
		Index string `json:"index"`
	}
	var turn turnRecord
	rawTurn := []byte(`{"_work_kind":"future","id":"turn","binding_id":"binding","lease":{"owner":99},"index":[1,2]}`)
	require.NoError(t, DecodeStored(rawTurn, &turn))
	require.NotNil(t, turn.Turn)
	ownedTurn := turn.Turn
	require.True(t, ownedTurn.IsOpaqueStored())
	require.Equal(t, "turn", ownedTurn.ID)
	encoded, err := EncodeStored(turn)
	require.NoError(t, err)
	require.Equal(t, rawTurn, encoded)
	type deliveryRecord struct {
		Delivery
		Index uint64 `json:"index"`
	}
	var delivery deliveryRecord
	rawDelivery := []byte(`{"_work_kind":"future","id":"delivery","body":[1,2],"index":"new"}`)
	require.NoError(t, DecodeStored(rawDelivery, &delivery))
	ownedDelivery := delivery.Delivery
	require.True(t, ownedDelivery.IsOpaqueStored())
	encoded, err = EncodeStored(delivery)
	require.NoError(t, err)
	require.Equal(t, rawDelivery, encoded)
	var page struct {
		Rows []Delivery `json:"rows"`
	}
	require.NoError(t, DecodeStored([]byte(`{"rows":[`+string(rawDelivery)+`]}`), &page))
	require.True(t, page.Rows[0].IsOpaqueStored())
	encoded, err = EncodeStored(page)
	require.NoError(t, err)
	require.JSONEq(t, `{"rows":[`+string(rawDelivery)+`]}`, string(encoded))
}

func TestOpaqueInputSubrecordCannotEnterSupportedTurn(t *testing.T) {
	turn := Turn{ID: "turn", Phase: PhaseSend}
	require.NoError(t, DecodeStored([]byte(`{"_work_kind":"future-input","blocks":["new"]}`), &turn.Input))
	require.ErrorIs(t, turnWorkError(turn), ErrUnsupportedWork)
	turn.Input = FrozenInput{}
	var block InputBlock
	require.NoError(t, DecodeStored([]byte(`{"_work_kind":"future-block","file_id":[1,2]}`), &block))
	turn.Input.Blocks = []InputBlock{block}
	require.ErrorIs(t, turnWorkError(turn), ErrUnsupportedWork)
}
