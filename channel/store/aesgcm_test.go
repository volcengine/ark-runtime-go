// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	codeccontract "github.com/volcengine/ark-runtime-go/channel/internal/storage/codec"
)

func TestAESGCMScopeAndEnvelopeAuthentication(t *testing.T) {
	key := make([]byte, 32)
	key[0] = 7
	codec, err := NewAESGCM("key-1", key)
	require.NoError(t, err)
	scope := Scope{Owner: "owner", Project: "project", Namespace: "namespace", RecordKind: "auth", RecordID: "record", Field: "token", PayloadVersion: 1, LogicalSchema: 1}
	plaintext := []byte("token-material")
	first, err := codec.Seal(context.Background(), scope, plaintext)
	require.NoError(t, err)
	second, err := codec.Seal(context.Background(), scope, plaintext)
	require.NoError(t, err)
	require.NotEqual(t, first.Ciphertext, second.Ciphertext)
	require.EqualValues(t, 1, first.Version)
	require.Equal(t, "key-1", first.KeyID)
	opened, err := codec.Open(context.Background(), scope, first)
	require.NoError(t, err)
	require.Equal(t, plaintext, opened)
	for _, field := range []string{"owner", "project", "namespace", "kind", "record", "field", "payload", "schema"} {
		t.Run(field, func(t *testing.T) {
			bad := scope
			switch field {
			case "owner":
				bad.Owner += "x"
			case "project":
				bad.Project += "x"
			case "namespace":
				bad.Namespace += "x"
			case "kind":
				bad.RecordKind += "x"
			case "record":
				bad.RecordID += "x"
			case "field":
				bad.Field += "x"
			case "payload":
				bad.PayloadVersion++
			case "schema":
				bad.LogicalSchema++
			}
			out, err := codec.Open(context.Background(), bad, first)
			require.Error(t, err)
			require.Nil(t, out)
			require.NotContains(t, err.Error(), "token-material")
		})
	}
	for _, field := range []string{"key", "version", "nonce", "ciphertext", "short"} {
		t.Run(field, func(t *testing.T) {
			bad := Envelope{Version: first.Version, KeyID: first.KeyID, Metadata: append([]byte(nil), first.Metadata...), Ciphertext: append([]byte(nil), first.Ciphertext...)}
			switch field {
			case "key":
				bad.KeyID = "other-key"
			case "version":
				bad.Version++
			case "nonce":
				bad.Metadata[0] ^= 1
			case "ciphertext":
				bad.Ciphertext[0] ^= 1
			case "short":
				bad.Metadata = nil
			}
			out, err := codec.Open(context.Background(), scope, bad)
			require.Error(t, err)
			require.Nil(t, out)
			require.NotContains(t, err.Error(), "other-key")
		})
	}
	key[0] = 8
	opened, err = codec.Open(context.Background(), scope, first)
	require.NoError(t, err)
	require.Equal(t, plaintext, opened)
	wrong, err := NewAESGCM("key-1", key)
	require.NoError(t, err)
	_, err = wrong.Open(context.Background(), scope, first)
	require.Error(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = codec.Seal(ctx, scope, plaintext)
	require.ErrorIs(t, err, context.Canceled)
	_, err = codec.Open(ctx, scope, first)
	require.ErrorIs(t, err, context.Canceled)
	_, err = codec.Seal(context.Background(), Scope{}, plaintext)
	require.ErrorIs(t, err, codeccontract.ErrInvalidScope)
}

func TestAESGCMRejectsInvalidKey(t *testing.T) {
	for _, size := range []int{0, 16, 24, 31, 33} {
		_, err := NewAESGCM("key", make([]byte, size))
		require.Error(t, err)
	}
	_, err := NewAESGCM("", make([]byte, 32))
	require.Error(t, err)
}

func TestAESGCMRejectsKeyIDRelabelWithSameKey(t *testing.T) {
	key := make([]byte, 32)
	first, err := NewAESGCM("key-a", key)
	require.NoError(t, err)
	second, err := NewAESGCM("key-b", key)
	require.NoError(t, err)
	scope := Scope{Owner: "owner", Project: "project", Namespace: "namespace", RecordKind: "auth", RecordID: "record", Field: "token", PayloadVersion: 1, LogicalSchema: 1}
	envelope, err := first.Seal(context.Background(), scope, []byte("secret"))
	require.NoError(t, err)
	envelope.KeyID = "key-b"
	_, err = second.Open(context.Background(), scope, envelope)
	require.Error(t, err)
}
