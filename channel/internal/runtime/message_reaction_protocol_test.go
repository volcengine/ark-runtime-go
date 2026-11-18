// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReactionUsesExistingDiscriminatorsWithoutDispatchHeader(t *testing.T) {
	delivery := NewDelivery(DeliverySource{Kind: SourceReaction, ID: "turn"}, 0, "reaction", "binding", ReplyTarget{ChatID: "chat", MessageID: "message"}, DeliveryBody{Type: BodyReaction})
	require.True(t, delivery.ValidNew(delivery.Source))
	require.NoError(t, deliveryWorkError(delivery))
	for _, encode := range []func(any) ([]byte, error){json.Marshal, EncodeStored} {
		raw, err := encode(delivery)
		require.NoError(t, err)
		var fields map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(raw, &fields))
		require.NotContains(t, fields, "_work_kind", "new work must use its existing source/body discriminators")
	}
}

func TestNewReactionRetainsPublishedJSONEncoding(t *testing.T) {
	delivery := NewMessageReactionDelivery(Turn{ID: "turn", BindingID: "binding"}, Message{ChatID: "chat", MessageID: "message"})
	raw, err := EncodeStored(delivery)
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &fields))
	require.JSONEq(t, `"message_reaction_v1"`, string(fields["_work_kind"]), "published readers require the existing reaction checkpoint header")
}

func TestReactionCheckpointDoesNotRequireWorkKind(t *testing.T) {
	for _, redacted := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "redacted"}[redacted], func(t *testing.T) {
			body := DeliveryBody{Type: BodyReaction}
			if redacted {
				body = DeliveryBody{}
			}
			delivery := NewDelivery(DeliverySource{Kind: SourceReaction, ID: "turn"}, 0, "reaction", "binding", ReplyTarget{ChatID: "chat", MessageID: "message"}, body)
			updated, err := delivery.Complete(DeliveryResult{Status: DeliveryPending, ReactionState: &MessageReactionState{Applied: ReactionQueued}})
			require.NoError(t, err)
			require.Equal(t, ReactionQueued, updated.ReactionState.Applied)
			delivery.Source.Kind = SourceTurn
			_, err = delivery.Complete(DeliveryResult{Status: DeliveryPending, ReactionState: &MessageReactionState{Applied: ReactionQueued}})
			require.ErrorIs(t, err, ErrInvalid)
		})
	}
}

func TestReactionLegacyHeaderRemainsReadable(t *testing.T) {
	delivery := NewMessageReactionDelivery(Turn{ID: "turn", BindingID: "binding"}, Message{ChatID: "chat", MessageID: "message"})
	raw, err := json.Marshal(delivery)
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &fields))
	fields["_work_kind"] = json.RawMessage(`"message_reaction_v1"`)
	raw, err = json.Marshal(fields)
	require.NoError(t, err)
	var restored Delivery
	require.NoError(t, DecodeStored(raw, &restored))
	require.NoError(t, deliveryWorkError(restored))
	require.Equal(t, BodyReaction, restored.Body.Type)
	restored.Body = TextBody("wrong body")
	require.ErrorIs(t, deliveryWorkError(restored), ErrUnsupportedWork)
}

func TestReactionSourceRequiresReactionBody(t *testing.T) {
	delivery := NewDelivery(DeliverySource{Kind: SourceReaction, ID: "turn"}, 0, "reaction", "binding", ReplyTarget{ChatID: "chat", MessageID: "message"}, TextBody("wrong body"))
	require.False(t, delivery.ValidNew(delivery.Source))
	require.ErrorIs(t, deliveryWorkError(delivery), ErrUnsupportedWork)
}
