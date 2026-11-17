// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/volcengine/ark-runtime-go/channel/internal/storage/codec"
)

var errInvalidKey = errors.New("payload codec: invalid aes-gcm key configuration")
var errAuthentication = errors.New("payload codec: authentication failed")

type aesGCM struct {
	keyID string
	aead  cipher.AEAD
}

// NewAESGCM 使用调用方提供的 256 位密钥创建认证加密 Codec，不生成替代密钥。
func NewAESGCM(keyID string, key []byte) (Codec, error) {
	if strings.TrimSpace(keyID) == "" || len(keyID) > 1024 || !utf8.ValidString(keyID) || len(key) != 32 {
		return nil, errInvalidKey
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, errInvalidKey
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, errInvalidKey
	}
	return &aesGCM{keyID: keyID, aead: aead}, nil
}

func (c *aesGCM) Seal(ctx context.Context, scope Scope, plaintext []byte) (Envelope, error) {
	if err := ctx.Err(); err != nil {
		return Envelope{}, err
	}
	aad, err := c.additionalData(scope)
	if err != nil {
		return Envelope{}, err
	}
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return Envelope{}, errors.New("payload codec: nonce generation failed")
	}
	ciphertext := c.aead.Seal(nil, nonce, plaintext, aad)
	if err := ctx.Err(); err != nil {
		return Envelope{}, err
	}
	return Envelope{Version: 1, KeyID: c.keyID, Metadata: nonce, Ciphertext: ciphertext}, nil
}

func (c *aesGCM) Open(ctx context.Context, scope Scope, envelope Envelope) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	aad, err := c.additionalData(scope)
	if err != nil {
		return nil, err
	}
	if envelope.Version != 1 || envelope.KeyID != c.keyID || len(envelope.Metadata) != c.aead.NonceSize() {
		return nil, errAuthentication
	}
	plaintext, err := c.aead.Open(nil, envelope.Metadata, envelope.Ciphertext, aad)
	if err != nil {
		return nil, errAuthentication
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return plaintext, nil
}

// String 返回不含密钥材料的安全视图。
func (c *aesGCM) String() string { return "AESGCMCodec{redacted}" }

// GoString 返回 Go 格式的安全视图。
func (c *aesGCM) GoString() string { return c.String() }

func (c *aesGCM) additionalData(scope Scope) ([]byte, error) {
	canonical, err := codec.CanonicalScope(scope)
	if err != nil {
		return nil, err
	}
	// 固定版本、KeyID 和规范身份一起认证，禁止同密钥在不同 KeyID 下重贴标签。
	return json.Marshal(struct {
		Version uint32          `json:"version"`
		KeyID   string          `json:"key_id"`
		Scope   json.RawMessage `json:"scope"`
	}{Version: 1, KeyID: c.keyID, Scope: canonical})
}
