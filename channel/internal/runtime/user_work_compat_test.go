// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUserWorkPreservesUnknownFields(t *testing.T) {
	turnRaw := []byte(`{"_work_kind":"user_auth_v2","id":"user-turn","future":{"value":7}}`)
	var turn Turn
	require.NoError(t, DecodeStored(turnRaw, &turn))
	require.False(t, turn.IsOpaqueStored())
	require.NoError(t, turnWorkError(turn))
	encoded, err := EncodeStored(turn)
	require.NoError(t, err)
	require.JSONEq(t, string(turnRaw), string(encoded))
	remoteRaw := []byte(`{"_work_kind":"user_auth_v2","id":"user-remote","kind":"input","future":{"value":7}}`)
	var remote RemoteOperation
	require.NoError(t, DecodeStored(remoteRaw, &remote))
	require.False(t, remote.IsOpaqueStored())
	require.NoError(t, remoteWorkError(remote))
	encoded, err = EncodeStored(remote)
	require.NoError(t, err)
	require.JSONEq(t, string(remoteRaw), string(encoded))
}
