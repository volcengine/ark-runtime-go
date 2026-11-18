// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Package codec 定义敏感持久载荷的加密契约，不依赖具体数据库或密钥服务。
package codec

import (
	"context"
	"encoding/json"
	"errors"
	"unicode/utf8"
)

// Scope 由调用方从已鉴权的领域记录推导，不能从密文自报身份恢复。
type Scope struct {
	Owner          string `json:"owner"`
	Project        string `json:"project"`
	Namespace      string `json:"namespace"`
	RecordKind     string `json:"record_kind"`
	RecordID       string `json:"record_id"`
	Field          string `json:"field"`
	PayloadVersion uint64 `json:"payload_version"`
	LogicalSchema  uint32 `json:"logical_schema"`
}

// Envelope 保存版本化密文；随机密文及其引用不得参与领域逻辑 hash。
type Envelope struct {
	Version    uint32 `json:"version"`
	KeyID      string `json:"key_id"`
	Metadata   []byte `json:"metadata"`
	Ciphertext []byte `json:"ciphertext"`
}

// Codec 在 Store 锁和事务之外执行；失败时禁止回退到明文。
type Codec interface {
	Seal(context.Context, Scope, []byte) (Envelope, error)
	Open(context.Context, Scope, Envelope) ([]byte, error)
}

// ErrInvalidScope 表示持久记录身份不完整或无法规范编码。
var ErrInvalidScope = errors.New("payload codec: invalid scope")

// CanonicalScope 使用带版本、固定字段顺序的 JSON 编码完整身份。
// 非法 UTF-8 会丢失身份信息，因此必须在 JSON 替换字符之前拒绝。
func CanonicalScope(scope Scope) ([]byte, error) {
	for _, value := range []string{scope.Owner, scope.Project, scope.Namespace, scope.RecordKind, scope.RecordID, scope.Field} {
		if value == "" || len(value) > 1024 || !utf8.ValidString(value) {
			return nil, ErrInvalidScope
		}
	}
	if scope.PayloadVersion == 0 || scope.LogicalSchema == 0 {
		return nil, ErrInvalidScope
	}
	return json.Marshal(struct {
		Version uint32 `json:"scope_version"`
		Scope
	}{Version: 1, Scope: scope})
}
