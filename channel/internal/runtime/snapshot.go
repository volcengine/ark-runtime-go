// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"
	"time"
)

// SessionSnapshot 是新一代 Session 在远程创建之前冻结的完整请求。
// 只保存非密配置及凭证引用，不保存访问令牌或 AppSecret。
type SessionSnapshot struct {
	StoredRecord  `json:"-"`
	frozenSpec    *frozenStoredJSON
	Payloads      *StoredPayloads   `json:"_payloads,omitempty"`
	ID            string            `json:"id"`
	SchemaVersion int               `json:"schema_version"`
	Route         RouteKey          `json:"route"`
	Generation    uint64            `json:"generation"`
	RequestHash   string            `json:"request_hash"`
	Spec          CreateSessionSpec `json:"spec"`
	CreatedAt     time.Time         `json:"created_at"`
}

func (s *SessionSnapshot) captureFrozenSpec(data []byte) {
	baseline, _ := json.Marshal(s.Spec)
	if !bytes.Equal(data, baseline) || s.RequestHash != digestJSON(s.Spec) {
		s.frozenSpec = &frozenStoredJSON{raw: bytes.Clone(data), baseline: baseline, digest: s.RequestHash}
	}
}

// NewSessionSnapshot 为会话代数生成稳定 ID，并计算完整创建请求哈希。
func NewSessionSnapshot(route RouteKey, generation uint64, spec CreateSessionSpec) SessionSnapshot {
	spec.Binding.UserAuthScopes = cloneStrings(spec.Binding.UserAuthScopes)
	if spec.UserCredential != nil {
		credential := *spec.UserCredential
		spec.UserCredential = &credential
	}
	return SessionSnapshot{ID: "snapshot:" + stableKey(string(route), strconv.FormatUint(generation, 10)), SchemaVersion: 1, Route: route, Generation: generation, RequestHash: digestJSON(spec), Spec: spec}
}

// Valid 验证快照身份及请求完整性。
func (s SessionSnapshot) Valid() bool {
	if s.IsOpaqueStored() {
		return false
	}
	expected := NewSessionSnapshot(s.Route, s.Generation, s.Spec)
	validHash := s.RequestHash == expected.RequestHash
	if s.frozenSpec != nil {
		baseline, _ := json.Marshal(s.Spec)
		validHash = s.RequestHash == s.frozenSpec.digest && digestBytes(s.frozenSpec.raw) == s.RequestHash && bytes.Equal(baseline, s.frozenSpec.baseline)
	}
	return s.SchemaVersion == 1 && s.Route != "" && s.Generation > 0 && s.Spec.Binding.ID != "" && s.Spec.CreationKey != "" && s.Spec.LookupKey != "" && s.ID == expected.ID && validHash
}

// StoredSpecJSON 返回已验真的完整冻结原文，供 payload codec 保留原始 hash 与未知字段。
// 不允许用当前 typed 视图覆盖已冻结请求。
func (s SessionSnapshot) StoredSpecJSON() ([]byte, error) {
	if s.IsOpaqueStored() {
		return nil, ErrUnsupportedWork
	}
	if s.frozenSpec != nil {
		baseline, _ := json.Marshal(s.Spec)
		if s.RequestHash != s.frozenSpec.digest || digestBytes(s.frozenSpec.raw) != s.RequestHash || !bytes.Equal(baseline, s.frozenSpec.baseline) {
			return nil, ErrInvalid
		}
		return bytes.Clone(s.frozenSpec.raw), nil
	}
	return json.Marshal(s.Spec)
}

// RestoreStoredSpecJSON 验证完整解密原文并保留原 hash，避免重写不可变载荷身份。
func (s *SessionSnapshot) RestoreStoredSpecJSON(data []byte) error {
	if s.IsOpaqueStored() {
		return ErrUnsupportedWork
	}
	var spec CreateSessionSpec
	if err := json.Unmarshal(data, &spec); err != nil {
		return err
	}
	if digestBytes(data) != s.RequestHash {
		return ErrInvalid
	}
	s.Spec = spec
	s.frozenSpec = nil
	s.captureFrozenSpec(data)
	return nil
}

// SnapshotStore 在持有 Turn 租约时冻结创建请求；相同代数的不同请求必须拒绝。
type SnapshotStore interface {
	FreezeSession(context.Context, Lease, SessionSnapshot) (SessionSnapshot, error)
	GetSessionSnapshot(context.Context, string) (*SessionSnapshot, error)
}

// CanFreeze 验证快照对应当前 Turn 的创建意图。
func (s SessionSnapshot) CanFreeze(t Turn) bool {
	return s.Valid() && t.Phase == PhaseCreate && s.Route == t.ConversationID && s.Generation == t.Session.Generation && s.Spec.CreationKey == t.Session.CreationKey && s.Spec.Binding.ID == t.BindingID && (t.Session.SnapshotID == "" || t.Session.SnapshotID == s.ID)
}
