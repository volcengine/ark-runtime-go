// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Package payload 将不可变敏感字段与事务元数据分开；本包不启动事务。
package payload

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strconv"
	"strings"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/codec"
)

// ErrPolicyMismatch 表示数据库的载荷加密策略与打开配置不一致。
var ErrPolicyMismatch = errors.New("payload encryption policy mismatch")

// ErrInvalid 表示载荷引用、认证上下文或不可变内容不一致。
var ErrInvalid = errors.New("invalid protected payload")

// ErrPrepare 表示事务必须先回滚，再在事务外准备密文。
var ErrPrepare = errors.New("payload preparation required")

// Record 是独立持久化的不可变密文。
type Record struct {
	ID       string         `json:"id"`
	Scope    codec.Scope    `json:"scope"`
	Envelope codec.Envelope `json:"envelope"`
}
type request struct {
	scope codec.Scope
	plain []byte
}

// Session 保存一次 Store 操作的有界准备集，不跨调用缓存明文。
type Session struct {
	codec     codec.Codec
	namespace string
	requests  map[string]request
	records   map[string]Record
	hashes    map[string][32]byte
}

// NewSession 创建单次操作的载荷准备器。
func NewSession(c codec.Codec, namespace string) *Session {
	return &Session{codec: c, namespace: namespace, requests: map[string]request{}, records: map[string]Record{}, hashes: map[string][32]byte{}}
}

// Pending 表示仍有必须在事务外加密的载荷。
func (s *Session) Pending() bool { return len(s.requests) > 0 }

// Prepare 必须在事务回滚之后调用；失败不会产生可提交的明文 fallback。
func (s *Session) Prepare(ctx context.Context) error {
	ids := make([]string, 0, len(s.requests))
	for id := range s.requests {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		r := s.requests[id]
		env, e := s.codec.Seal(ctx, r.scope, r.plain)
		if e != nil {
			return e
		}
		s.records[id] = Record{ID: id, Scope: r.scope, Envelope: env}
		clear(r.plain)
		delete(s.requests, id)
	}
	return nil
}

// Records 返回准备好的密文副本，不含明文。
func (s *Session) Records() []Record {
	out := make([]Record, 0, len(s.records))
	for _, r := range s.records {
		out = append(out, r)
	}
	return out
}

// Close 清除单次操作保留的待加密明文。
func (s *Session) Close() {
	for id, r := range s.requests {
		clear(r.plain)
		delete(s.requests, id)
	}
}
func cloneState(old *core.StoredPayloads, b core.Binding) *core.StoredPayloads {
	n := &core.StoredPayloads{Owner: b.Owner, Project: b.Project, Refs: map[string]string{}}
	if old != nil {
		n.StoredRecord = old.StoredRecord
		n.Owner, n.Project = old.Owner, old.Project
		for k, v := range old.Refs {
			n.Refs[k] = v
		}
	}
	if n.Owner == "" {
		n.Owner = "binding:" + b.ID
	}
	if n.Project == "" {
		n.Project = "default"
	}
	return n
}

// referenceIdentity is shared by writes, reads and maintenance. Every slot is
// bound to its record and namespace before mutable link rows are considered.
func referenceIdentity(namespace string, st *core.StoredPayloads, kind, id, field string) (codec.Scope, string, error) {
	scope := codec.Scope{Owner: st.Owner, Project: st.Project, Namespace: namespace, RecordKind: kind, RecordID: id, Field: field, PayloadVersion: 1, LogicalSchema: 1}
	canonical, err := codec.CanonicalScope(scope)
	if err != nil {
		return scope, "", err
	}
	sum := sha256.Sum256(canonical)
	return scope, "payload:" + hex.EncodeToString(sum[:]), nil
}
func (s *Session) reference(st *core.StoredPayloads, kind, id, field string, v any) (string, error) {
	scope, ref, e := referenceIdentity(s.namespace, st, kind, id, field)
	if e != nil {
		return "", e
	}
	if prior := st.Refs[field]; prior != "" {
		if prior != ref {
			return "", ErrInvalid
		}
		return ref, nil
	}
	plain, e := core.EncodeStored(v)
	if e != nil {
		return "", e
	}
	hash := sha256.Sum256(plain)
	if old, ok := s.hashes[ref]; ok && old != hash {
		clear(plain)
		return "", core.ErrConflict
	}
	s.hashes[ref] = hash
	if r, ok := s.requests[ref]; ok && !bytes.Equal(r.plain, plain) {
		return "", core.ErrConflict
	}
	if _, ok := s.records[ref]; !ok {
		if len(s.requests) >= core.MaxPageSize {
			return "", core.ErrInvalid
		}
		s.requests[ref] = request{scope: scope, plain: plain}
	} else {
		clear(plain)
	}
	st.Refs[field] = ref
	return ref, nil
}

// Protect 返回不含敏感字段的记录及强引用。此函数只做内存复制与编码，不调用 Codec。
func (s *Session) Protect(value any, b core.Binding) ([]byte, []string, error) {
	var st *core.StoredPayloads
	var e error
	switch v := value.(type) {
	case core.InboundMessage:
		st = cloneState(v.Payloads, b)
		_, e = s.reference(st, "message", v.ID, "content", struct {
			Text        string
			Attachments []core.Attachment
			Metadata    *core.MessageMetadata `json:",omitempty"`
		}{v.Message.Text, v.Message.Attachments, v.Message.Metadata})
		v.Payloads = st
		v.ForgetStoredFields("message.text", "message.attachments", "message.metadata")
		v.Message.Text = ""
		v.Message.Attachments = nil
		v.Message.Metadata = nil
		value = v
	case core.Turn:
		if v.Snapshot != nil {
			b = v.Snapshot.Binding
		}
		st = cloneState(v.Payloads, b)
		if v.Input.PayloadRefsReset() {
			// 背景和挂载随代数变化；Blocks 的上传 file_id 与 Session 无关，保留引用。
			for field := range st.Refs {
				if field == "context" || field == "mounts" || strings.HasPrefix(field, "context:") || strings.HasPrefix(field, "mounts:") {
					delete(st.Refs, field)
				}
			}
		}
		if v.Input.LogicalDigest == "" || v.Input.Context != "" || len(v.Input.Blocks) > 0 || len(st.Refs) == 0 {
			v.Input.LogicalDigest = v.Input.LogicalHash()
		}
		contextField := turnField(st, "context", v.Session.Generation)
		if v.Input.ContextFrozen || st.Refs[contextField] != "" {
			_, e = s.reference(st, "turn", v.ID, contextField, v.Input.Context)
			v.ForgetStoredFields("input.context")
			v.Input.Context = ""
		}
		if e == nil && (v.Input.BlocksFrozen || st.Refs["blocks"] != "") {
			_, e = s.reference(st, "turn", v.ID, "blocks", v.Input.Blocks)
			v.ForgetStoredFields("input.blocks")
			v.Input.Blocks = nil
		}
		mountField := turnField(st, "mounts", v.Session.Generation)
		if e == nil && v.Input.MountsFrozen {
			_, e = s.reference(st, "turn", v.ID, mountField, v.Input.Mounts)
			v.ForgetStoredFields("input.mounts")
			v.Input.Mounts = nil
		}
		v.Payloads = st
		value = v
	case core.SessionSnapshot:
		b = v.Spec.Binding
		st = cloneState(v.Payloads, b)
		rawSpec, specErr := v.StoredSpecJSON()
		if specErr != nil {
			return nil, nil, specErr
		}
		_, e = s.reference(st, "snapshot", v.ID, "spec", json.RawMessage(rawSpec))
		v.Payloads = st
		v.ForgetStoredFields("spec")
		v.Spec.Extras = core.SessionExtras{}
		v.Spec.EnvironmentOverride = nil
		value = v
	case core.Delivery:
		if v.Snapshot != nil {
			b = v.Snapshot.Binding
		}
		st = cloneState(v.Payloads, b)
		_, e = s.reference(st, "delivery", v.ID, "body", v.Body)
		v.Payloads = st
		v.ForgetStoredFields("body")
		v.Body = core.TextBody("")
		value = v
	case core.Interaction:
		st = cloneState(v.Payloads, b)
		// 决定只能从 pending 单向迁移；不同状态的理由不能复用不可变载荷。
		field := "reason:" + string(v.Status)
		for old := range st.Refs {
			if strings.HasPrefix(old, "reason:") && old != field {
				delete(st.Refs, old)
			}
		}
		_, e = s.reference(st, "interaction", v.ID, "preview", v.InputPreview)
		if e == nil {
			_, e = s.reference(st, "interaction", v.ID, field, v.DenyReason)
		}
		v.Payloads = st
		v.ForgetStoredFields("input_preview", "deny_reason")
		v.InputPreview, v.DenyReason = "", ""
		value = v
	default:
		return nil, nil, ErrInvalid
	}
	if e != nil {
		return nil, nil, e
	}
	data, e := core.EncodeStored(value)
	refs := make([]string, 0, len(st.Refs))
	for _, ref := range st.Refs {
		refs = append(refs, ref)
	}
	sort.Strings(refs)
	return data, refs, e
}

// Loader 在事务之外读取独立的密文记录。
type Loader func(context.Context, string) (Record, error)

func (s *Session) open(ctx context.Context, st *core.StoredPayloads, kind, id, field string, load Loader, out any) error {
	if st.Refs[field] == "" {
		return nil
	}
	r, expected, e := s.checkedRecord(ctx, st, kind, id, field, load)
	if e != nil {
		return e
	}
	plain, e := s.codec.Open(ctx, expected, r.Envelope)
	if e != nil {
		return e
	}
	defer clear(plain)
	if e = core.DecodeStored(plain, out); e != nil {
		return ErrInvalid
	}
	return nil
}

// Hydrate 在事务结束之后按记录的可信身份解密；不会信任 Envelope 自报 Scope。
func (s *Session) Hydrate(ctx context.Context, value any, load Loader) error {
	if e := ValidateRedacted(value); e != nil {
		return e
	}
	if _, _, _, _, _, e := metadata(value); e != nil {
		return e
	}
	switch v := value.(type) {
	case *core.InboundMessage:
		if v == nil {
			return nil
		}
		if v.Payloads == nil {
			return ErrPolicyMismatch
		}
		var content struct {
			Text        string
			Attachments []core.Attachment
			Metadata    *core.MessageMetadata `json:",omitempty"`
		}
		if e := s.open(ctx, v.Payloads, "message", v.ID, "content", load, &content); e != nil {
			return e
		}
		v.Message.Text, v.Message.Attachments = content.Text, content.Attachments
		v.Message.Metadata = content.Metadata
	case *core.Turn:
		if v == nil {
			return nil
		}
		if v.Payloads == nil {
			return ErrPolicyMismatch
		}
		if e := s.open(ctx, v.Payloads, "turn", v.ID, turnField(v.Payloads, "context", v.Session.Generation), load, &v.Input.Context); e != nil {
			return e
		}
		if e := s.open(ctx, v.Payloads, "turn", v.ID, "blocks", load, &v.Input.Blocks); e != nil {
			return e
		}
		if e := s.open(ctx, v.Payloads, "turn", v.ID, turnField(v.Payloads, "mounts", v.Session.Generation), load, &v.Input.Mounts); e != nil {
			return e
		}
		if v.Input.LogicalDigest != "" && v.Input.LogicalHash() != v.Input.LogicalDigest {
			return ErrInvalid
		}
		return nil
	case *core.SessionSnapshot:
		if v == nil {
			return nil
		}
		if v.Payloads == nil {
			return ErrPolicyMismatch
		}
		var raw json.RawMessage
		if e := s.open(ctx, v.Payloads, "snapshot", v.ID, "spec", load, &raw); e != nil {
			return e
		}
		return v.RestoreStoredSpecJSON(raw)
	case *core.Delivery:
		if v == nil {
			return nil
		}
		if v.Payloads == nil {
			return ErrPolicyMismatch
		}
		return s.open(ctx, v.Payloads, "delivery", v.ID, "body", load, &v.Body)
	case *core.Interaction:
		if v == nil {
			return nil
		}
		if e := s.open(ctx, v.Payloads, "interaction", v.ID, "preview", load, &v.InputPreview); e != nil {
			return e
		}
		return s.open(ctx, v.Payloads, "interaction", v.ID, "reason:"+string(v.Status), load, &v.DenyReason)
	default:
		return ErrInvalid
	}
	return nil
}

func metadata(value any) (*core.StoredPayloads, string, string, []string, core.Binding, error) {
	var st *core.StoredPayloads
	var kind, id string
	var fields []string
	b := core.Binding{}
	switch v := value.(type) {
	case *core.InboundMessage:
		if v == nil {
			return nil, "", "", nil, b, nil
		}
		st, kind, id, fields = v.Payloads, "message", v.ID, []string{"content"}
	case *core.Turn:
		if v == nil {
			return nil, "", "", nil, b, nil
		}
		st, kind, id = v.Payloads, "turn", v.ID
		if v.Input.ContextFrozen {
			fields = append(fields, turnField(v.Payloads, "context", v.Session.Generation))
		}
		if v.Input.BlocksFrozen {
			fields = append(fields, "blocks")
		}
		mountField := turnField(v.Payloads, "mounts", v.Session.Generation)
		// M2 前没有 mounts 引用，空挂载结果继续可读；有引用时必须完整验证。
		if v.Input.MountsFrozen && v.Payloads != nil && v.Payloads.Refs[mountField] != "" {
			fields = append(fields, mountField)
		}
		if v.Snapshot != nil {
			b = v.Snapshot.Binding
		}
	case *core.SessionSnapshot:
		if v == nil {
			return nil, "", "", nil, b, nil
		}
		st, kind, id, fields = v.Payloads, "snapshot", v.ID, []string{"spec"}
		b = v.Spec.Binding
	case *core.Delivery:
		if v == nil {
			return nil, "", "", nil, b, nil
		}
		st, kind, id, fields = v.Payloads, "delivery", v.ID, []string{"body"}
		if v.Snapshot != nil {
			b = v.Snapshot.Binding
		}
	case *core.Interaction:
		if v == nil {
			return nil, "", "", nil, b, nil
		}
		st, kind, id, fields = v.Payloads, "interaction", v.ID, []string{"preview", "reason:" + string(v.Status)}
	default:
		return nil, "", "", nil, b, ErrInvalid
	}
	if st == nil {
		return nil, "", "", nil, b, ErrPolicyMismatch
	}
	if st.Owner == "" || st.Project == "" || id == "" {
		return nil, "", "", nil, b, ErrInvalid
	}
	if b.ID != "" {
		expected := cloneState(nil, b)
		if st.Owner != expected.Owner || st.Project != expected.Project {
			return nil, "", "", nil, b, ErrInvalid
		}
	}
	for _, f := range fields {
		if st.Refs[f] == "" {
			return nil, "", "", nil, b, ErrInvalid
		}
	}
	if len(st.Refs) != len(fields) {
		return nil, "", "", nil, b, ErrInvalid
	}
	return st, kind, id, fields, b, nil
}
func (s *Session) checkedRecord(ctx context.Context, st *core.StoredPayloads, kind, id, field string, load Loader) (Record, codec.Scope, error) {
	ref := st.Refs[field]
	expected, expectedRef, e := referenceIdentity(s.namespace, st, kind, id, field)
	if e != nil {
		return Record{}, expected, e
	}
	if ref != expectedRef {
		return Record{}, expected, ErrInvalid
	}
	r, e := load(ctx, ref)
	if e != nil {
		return Record{}, expected, e
	}
	if r.ID != ref || r.Scope != expected {
		return Record{}, expected, ErrInvalid
	}
	return r, expected, nil
}

// Verify 只检查记录的不可变引用和完整Scope，不调用Codec，供事务内校验载荷引用。
func (s *Session) Verify(ctx context.Context, value any, load Loader) error {
	if e := ValidateRedacted(value); e != nil {
		return e
	}
	st, kind, id, fields, _, e := metadata(value)
	if e != nil {
		return e
	}
	if st == nil {
		return nil
	}
	for _, field := range fields {
		if _, _, e = s.checkedRecord(ctx, st, kind, id, field, load); e != nil {
			return e
		}
	}
	return nil
}

// Needs 返回指定载荷是否尚待事务外加密，不暴露待加密内容。
func (s *Session) Needs(id string) bool { _, ok := s.requests[id]; return ok }

// turnField 兼容旧背景引用；新背景和挂载按 Session 代数分域，避免重建后复活旧密文。
func turnField(st *core.StoredPayloads, field string, generation uint64) string {
	if generation == 0 || st != nil && st.Refs[field] != "" {
		return field
	}
	return field + ":" + strconv.FormatUint(generation, 10)
}

// ValidateRedacted 拒绝合法引用旁残留明文的混合记录；不会清除或覆盖坏数据。
func ValidateRedacted(value any) error {
	switch v := value.(type) {
	case *core.InboundMessage:
		if v != nil && (v.Message.Text != "" || len(v.Message.Attachments) != 0 || v.Message.Metadata != nil) {
			return ErrInvalid
		}
	case *core.Turn:
		if v != nil && (v.Input.Context != "" || len(v.Input.Blocks) != 0 || len(v.Input.Mounts) != 0) {
			return ErrInvalid
		}
	case *core.SessionSnapshot:
		if v != nil && (v.Spec.EnvironmentOverride != nil || !reflect.DeepEqual(v.Spec.Extras, core.SessionExtras{})) {
			return ErrInvalid
		}
	case *core.Delivery:
		if v != nil && (v.Body.Type != core.BodyText || v.Body.Text != "" || v.Body.ImageFallbackText != "" || v.Body.Artifact != (core.Artifact{}) || len(v.Body.Parts) != 0) {
			return ErrInvalid
		}
	case *core.Interaction:
		if v != nil && (v.InputPreview != "" || v.DenyReason != "") {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}
