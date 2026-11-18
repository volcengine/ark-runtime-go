// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"
)

// ErrAdmissionExpired 表示权威受理许可在入站事务执行前已过期。
var ErrAdmissionExpired = errors.New("channel admission expired")

// ErrChannelDisabled 表示权威配置已停用渠道，不允许开始新执行。
var ErrChannelDisabled = errors.New("channel disabled")

// TurnSnapshot 冻结一条消息的处理策略及新建默认值，不覆盖旧 Session 的执行身份。
type TurnSnapshot struct {
	StoredRecord  `json:"-"`
	frozenBinding *frozenStoredJSON
	ID            string    `json:"id"`
	SchemaVersion int       `json:"schema_version"`
	Binding       Binding   `json:"binding"`
	AdmitUntil    time.Time `json:"admit_until,omitempty"`
}

// Valid 验证冻结策略的完整性和身份。
func (s TurnSnapshot) Valid() bool {
	if s.IsOpaqueStored() {
		return false
	}
	validHash := s.ID == NewTurnSnapshot(s.Binding, s.AdmitUntil).ID
	if s.frozenBinding != nil {
		baseline, _ := json.Marshal(s.Binding)
		validHash = s.ID == "turn-snapshot:"+s.frozenBinding.digest && digestBytes(s.frozenBinding.raw) == s.frozenBinding.digest && bytes.Equal(baseline, s.frozenBinding.baseline)
	}
	return s.SchemaVersion == 1 && s.Binding.ID != "" && validHash
}

// NewTurnSnapshot 生成完整非密策略快照。
func NewTurnSnapshot(binding Binding, admitUntil time.Time) *TurnSnapshot {
	b := binding.clone()
	return &TurnSnapshot{ID: "turn-snapshot:" + digestJSON(b), SchemaVersion: 1, Binding: b, AdmitUntil: admitUntil}
}

// IngestScope 是托管入口在认证之后传入的渠道坐标。
type IngestScope struct {
	Owner        string
	BindingID    string
	BindingEpoch uint64
}

// ResolvedIngest 包含权威策略、短期许可和本次连接租约。
type ResolvedIngest struct {
	Connection Connection
	AdmitUntil time.Time
}

// ConnectionResolver 为入站、执行和投递分别解析连接。执行/投递必须尊重持久快照。
// 调用方每次使用后调用 Connection.Release；不允许返回持有 Secret 明文的 Binding。
type ConnectionResolver interface {
	ResolveIngest(context.Context, IngestScope) (ResolvedIngest, error)
	ResolveForTurn(context.Context, Turn, *SessionSnapshot) (Connection, error)
	ResolveForDelivery(context.Context, Delivery) (Connection, error)
}

// CleanupConnectionResolver 仅为已提交任务的停止/超时清理解析冻结 MA 身份。
// 停用渠道可返回仅含 Binding/MAClient 的连接，绝不能据此创建或提交新输入。
type CleanupConnectionResolver interface {
	ResolveForCleanup(context.Context, Turn, *SessionSnapshot) (Connection, error)
}

func releaseConnection(c Connection) {
	if c.Release != nil {
		c.Release()
	}
}

func (r *Runtime) resolveTurn(ctx context.Context, j *job) (Connection, error) {
	return r.resolveConnection(ctx, j, false)
}
func (r *Runtime) resolveCleanup(ctx context.Context, j *job) (Connection, error) {
	return r.resolveConnection(ctx, j, true)
}
func (r *Runtime) resolveConnection(ctx context.Context, j *job, cleanup bool) (Connection, error) {
	var snapshot *SessionSnapshot
	if j.Session.SnapshotID != "" {
		var err error
		snapshot, err = r.store.GetSessionSnapshot(ctx, j.Session.SnapshotID)
		if err != nil {
			if isUnsupportedWork(err) {
				return Connection{}, err
			}
			return Connection{}, NewStoreFailure(err)
		}
		if err = sessionWorkError(snapshot); err != nil {
			return Connection{}, err
		}
		if snapshot == nil || !snapshot.Valid() || snapshot.Route != j.ConversationID || snapshot.Generation != j.Session.Generation {
			return Connection{}, ErrConflict
		}
	}
	var c Connection
	var err error
	if r.options.Resolver != nil {
		if resolver, ok := r.options.Resolver.(CleanupConnectionResolver); cleanup && ok {
			c, err = resolver.ResolveForCleanup(ctx, j.Turn, snapshot)
		} else {
			c, err = r.options.Resolver.ResolveForTurn(ctx, j.Turn, snapshot)
		}
	} else {
		var ok bool
		c, ok = r.connections[j.BindingID]
		if !ok {
			return Connection{}, ErrRuntimeConfig
		}
	}
	if err != nil {
		return Connection{}, err
	}
	expected := j.binding
	if snapshot != nil {
		expected = snapshot.Spec.Binding
	}
	if !cleanup && c.Provider == nil || c.MAClient == nil || c.Binding.ID != expected.ID || c.Binding.AppID != expected.AppID || c.Binding.Owner != expected.Owner || c.Binding.BindingEpoch != expected.BindingEpoch {
		releaseConnection(c)
		return Connection{}, ErrRuntimeConfig
	}
	c.Binding = expected
	return c, nil
}

// WarmBotIdentity 显式准备指定连接的 Bot 凭据，不依赖 Provider.Start。
func (r *Runtime) WarmBotIdentity(ctx context.Context, c Connection, chatType string) error {
	return r.ensureBotIdentity(ctx, c, chatType)
}

func (r *Runtime) jobConnection(j *job) Connection {
	if j.connection.MAClient != nil {
		return j.connection
	}
	return r.connections[j.BindingID]
}
