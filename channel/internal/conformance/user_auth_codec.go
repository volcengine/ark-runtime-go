// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package conformance

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"sync/atomic"

	"github.com/volcengine/ark-runtime-go/channel/internal/storage/codec"
)

// UserAuthTestCodec 为授权 conformance 提供真实 AEAD 与确定性故障注入。
type UserAuthTestCodec struct {
	Fail  atomic.Bool
	Calls atomic.Int64
}

// Seal 加密提示正文；Fail 用于验证事务不能降级明文。
func (c *UserAuthTestCodec) Seal(_ context.Context, s codec.Scope, p []byte) (codec.Envelope, error) {
	c.Calls.Add(1)
	if c.Fail.Load() {
		return codec.Envelope{}, errors.New("injected seal failure")
	}
	b, _ := aes.NewCipher(make([]byte, 32))
	a, _ := cipher.NewGCM(b)
	n := make([]byte, a.NonceSize())
	if _, e := rand.Read(n); e != nil {
		return codec.Envelope{}, e
	}
	aad, e := codec.CanonicalScope(s)
	if e != nil {
		return codec.Envelope{}, e
	}
	return codec.Envelope{Version: 1, KeyID: "test", Ciphertext: a.Seal(n, n, p, aad)}, nil
}

// Open 解密测试提示，记录调用用于 metadata 零 KMS 断言。
func (c *UserAuthTestCodec) Open(_ context.Context, s codec.Scope, v codec.Envelope) ([]byte, error) {
	c.Calls.Add(1)
	if c.Fail.Load() {
		return nil, errors.New("injected open failure")
	}
	b, _ := aes.NewCipher(make([]byte, 32))
	a, _ := cipher.NewGCM(b)
	aad, e := codec.CanonicalScope(s)
	if e != nil {
		return nil, e
	}
	return a.Open(nil, v.Ciphertext[:a.NonceSize()], v.Ciphertext[a.NonceSize():], aad)
}
