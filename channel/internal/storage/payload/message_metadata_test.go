// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package payload

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestMessageMetadataIsEncryptedAndRestored(t *testing.T) {
	var message core.Message
	require.NoError(t, json.Unmarshal([]byte(`{"binding_id":"b","message_id":"m","text":"原文","metadata":{"root_message_id":"root","message_type":"text","mentions":[{"open_id":"ou_a","name":"私密姓名"}]}}`), &message))
	original := core.NewInboundMessage(message, time.Now())
	original.ID = "stored-message"
	s := NewSession(&testCodec{}, "deployment")
	wire, _, err := s.Protect(original, core.Binding{ID: "b"})
	require.NoError(t, err)
	require.NotContains(t, string(wire), "私密姓名")
	require.NoError(t, s.Prepare(context.Background()))
	var stored core.InboundMessage
	require.NoError(t, core.DecodeStored(wire, &stored))
	require.NoError(t, ValidateRedacted(&stored))
	require.NoError(t, s.Hydrate(context.Background(), &stored, func(context.Context, string) (Record, error) { return s.Records()[0], nil }))
	restored, err := json.Marshal(stored.Message)
	require.NoError(t, err)
	require.Contains(t, string(restored), "私密姓名")
	require.Equal(t, original.ContentHash, core.ContentHash(stored.Message))
	require.ErrorIs(t, ValidateRedacted(&stored), ErrInvalid)
}
