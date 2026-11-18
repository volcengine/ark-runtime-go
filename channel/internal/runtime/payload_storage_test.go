// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRemoteRequestHashSurvivesProtectedInputMetadata(t *testing.T) {
	input := FrozenInput{ContextFrozen: true, Context: "source-secret", BlocksFrozen: true, Blocks: []InputBlock{{Type: "document", FileID: "file-one"}}}
	turn := Turn{ID: "turn", Input: input, InputEventID: "event", Session: TurnSession{Generation: 1, SessionID: "session"}}
	plain := NewRemoteOperation(turn, "owner", RemoteInput)
	turn.Input = FrozenInput{ContextFrozen: true, BlocksFrozen: true, LogicalDigest: input.LogicalHash()}
	turn.Payloads = &StoredPayloads{Refs: map[string]string{"context": "ref-context", "blocks": "ref-blocks"}}
	metadata := NewRemoteOperation(turn, "owner", RemoteInput)
	require.Equal(t, plain.RequestHash, metadata.RequestHash)
	turn.Input = input
	turn.Input.Context = "other-secret"
	require.NotEqual(t, plain.RequestHash, NewRemoteOperation(turn, "owner", RemoteInput).RequestHash)
	turn.Input = input
	turn.Input.Blocks = []InputBlock{{Type: "document", FileID: "file-two"}}
	require.NotEqual(t, plain.RequestHash, NewRemoteOperation(turn, "owner", RemoteInput).RequestHash)
}
