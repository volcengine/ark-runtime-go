// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package memory_test

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/codec"
	memory "github.com/volcengine/ark-runtime-go/channel/internal/testutil/memorystore"
)

type memoryPayloadCodec struct{}

func (memoryPayloadCodec) Seal(_ context.Context, s codec.Scope, b []byte) (codec.Envelope, error) {
	block, _ := aes.NewCipher(bytes.Repeat([]byte{9}, 32))
	aead, _ := cipher.NewGCM(block)
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return codec.Envelope{}, err
	}
	aad, err := codec.CanonicalScope(s)
	if err != nil {
		return codec.Envelope{}, err
	}
	return codec.Envelope{Version: 1, KeyID: "test", Metadata: nonce, Ciphertext: aead.Seal(nil, nonce, b, aad)}, nil
}
func (memoryPayloadCodec) Open(_ context.Context, s codec.Scope, e codec.Envelope) ([]byte, error) {
	block, _ := aes.NewCipher(bytes.Repeat([]byte{9}, 32))
	aead, _ := cipher.NewGCM(block)
	aad, err := codec.CanonicalScope(s)
	if err != nil {
		return nil, err
	}
	return aead.Open(nil, e.Metadata, e.Ciphertext, aad)
}

// memory 复用 sqlstore，完整契约由 store/sql 执行；这里在共享内存连接上跑载荷子集。
func TestEncryptedMemoryConformance(t *testing.T) {
	conformance.RunStorePayload(t, func(t *testing.T, c conformance.StoreConfig) conformance.TestStore {
		s, err := memory.New(memory.Options{Clock: c.Clock, MaxOpenTurns: c.MaxOpenTurns, MaxMessageBytes: c.MaxMessageBytes, QueueTTL: c.QueueTTL, PayloadCodec: memoryPayloadCodec{}, PayloadNamespace: "memory-test", RequirePayloadEncryption: true})
		require.NoError(t, err)
		return s
	})
}
func TestMemoryEncryptionOptionsAreNotDropped(t *testing.T) {
	s, err := memory.New(memory.Options{RequirePayloadEncryption: true})
	if s != nil {
		_ = s.Close()
	}
	require.ErrorIs(t, err, core.ErrInvalid)
}
