// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/payload"
)

type payloadSessionKey struct{}
type payloadIDsKey struct{}
type payloadIDs struct {
	values []string
	index  int
}

// payloadDDL 定义不可变密文与强引用表。
func payloadDDL(d Dialect, prefix string) []string {
	return []string{
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %schannel_sensitive_payloads (id VARCHAR(128) NOT NULL PRIMARY KEY, data %s NOT NULL)", prefix, d.TextType),
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %schannel_payload_refs (record_kind VARCHAR(32) NOT NULL, record_id VARCHAR(128) NOT NULL, field VARCHAR(32) NOT NULL, payload_id VARCHAR(128) NOT NULL, PRIMARY KEY(record_kind,record_id,field))", prefix),
		fmt.Sprintf("CREATE INDEX %schannel_payload_refs_payload ON %schannel_payload_refs (payload_id)", prefix, prefix),
	}
}
func (s *store) payloadTable(suffix string) string {
	return s.t.meta[:len(s.t.meta)-len("channel_meta")] + "channel_" + suffix
}
func (s *store) runTx(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	if s.payloadCodec == nil {
		return s.runTxOnce(ctx, fn)
	}
	ps := payload.NewSession(s.payloadCodec, s.payloadNamespace)
	defer ps.Close()
	ids := &payloadIDs{}
	ctx = context.WithValue(ctx, payloadSessionKey{}, ps)
	ctx = context.WithValue(ctx, payloadIDsKey{}, ids)
	for range 3 {
		ids.index = 0
		err := s.runTxOnce(ctx, func(ctx context.Context, tx *sql.Tx) error {
			if err := s.lockPayloadMaintenance(ctx, tx, false); err != nil {
				return err
			}
			return fn(ctx, tx)
		})
		if !errors.Is(err, payload.ErrPrepare) {
			return err
		}
		if err = ps.Prepare(ctx); err != nil {
			return err
		}
	}
	return core.ErrConflict
}
func (s *store) randomID(ctx context.Context, prefix string, now time.Time) (string, error) {
	ids, _ := ctx.Value(payloadIDsKey{}).(*payloadIDs)
	if ids == nil {
		return rawRandomID(prefix, now)
	}
	i := ids.index
	ids.index++
	if i < len(ids.values) {
		return ids.values[i], nil
	}
	id, e := rawRandomID(prefix, now)
	if e == nil {
		ids.values = append(ids.values, id)
	}
	return id, e
}
func (s *store) protectedData(ctx context.Context, tx *sql.Tx, v any, b core.Binding) (string, error) {
	if record, ok := v.(interface{ IsOpaqueStored() bool }); ok && record.IsOpaqueStored() {
		return "", core.ErrUnsupportedWork
	}
	ps, _ := ctx.Value(payloadSessionKey{}).(*payload.Session)
	if ps == nil {
		return encode(v)
	}
	var prior *core.StoredPayloads
	switch value := v.(type) {
	case core.InboundMessage:
		prior = value.Payloads
	case core.Turn:
		prior = value.Payloads
	case core.SessionSnapshot:
		prior = value.Payloads
	case core.Delivery:
		prior = value.Payloads
	case core.Interaction:
		prior = value.Payloads
	}
	data, refs, e := ps.Protect(v, b)
	if e != nil {
		return "", e
	}
	records := map[string]payload.Record{}
	for _, r := range ps.Records() {
		records[r.ID] = r
	}
	var stored struct {
		ID       string               `json:"id"`
		Payloads *core.StoredPayloads `json:"_payloads"`
	}
	if e = json.Unmarshal(data, &stored); e != nil {
		return "", e
	}
	for _, id := range refs {
		if r, ok := records[id]; ok {
			wire, e := json.Marshal(r)
			if e != nil {
				return "", e
			}
			if _, e = tx.ExecContext(ctx, s.insertIgnore()+s.payloadTable("sensitive_payloads")+" (id,data) VALUES (?,?)", id, string(wire)); e != nil {
				return "", e
			}
		}
	}
	kind := ""
	switch v.(type) {
	case core.InboundMessage:
		kind = "message"
	case core.Turn:
		kind = "turn"
	case core.SessionSnapshot:
		kind = "snapshot"
	case core.Delivery:
		kind = "delivery"
	case core.Interaction:
		kind = "interaction"
	}

	for field, id := range stored.Payloads.Refs {
		if _, prepared := records[id]; !prepared && !ps.Needs(id) && (prior == nil || prior.Refs[field] != id) {
			return "", payload.ErrInvalid
		}
		if _, e = tx.ExecContext(ctx, s.insertIgnore()+s.payloadTable("payload_refs")+" (record_kind,record_id,field,payload_id) VALUES (?,?,?,?)", kind, stored.ID, field, id); e != nil {
			return "", e
		}
		var linked string
		if e = tx.QueryRowContext(ctx, "SELECT payload_id FROM "+s.payloadTable("payload_refs")+" WHERE record_kind=? AND record_id=? AND field=?", kind, stored.ID, field).Scan(&linked); e != nil {
			return "", e
		}
		if linked != id {
			return "", payload.ErrInvalid
		}
	}
	if turn, ok := v.(core.Turn); ok && turn.Input.PayloadRefsReset() {
		// 换代重建 Session：删除已不再引用的输入字段行，让旧密文引用随重新冻结被替换。
		activeContext := stored.Payloads.Refs["context"]
		activeBlocks := stored.Payloads.Refs["blocks"]
		if _, e = tx.ExecContext(ctx, "DELETE FROM "+s.payloadTable("payload_refs")+" WHERE record_kind=? AND record_id=? AND field IN ('context','blocks') AND (payload_id <> ? OR ? = '') AND (payload_id <> ? OR ? = '')",
			kind, turn.ID, activeContext, activeContext, activeBlocks, activeBlocks); e != nil {
			return "", e
		}
	}

	return string(data), nil
}
func (s *store) hydrate(ctx context.Context, v any) error {
	if record, ok := v.(interface{ IsOpaqueStored() bool }); ok && record.IsOpaqueStored() {
		return nil
	}
	if s.payloadCodec == nil {
		return nil
	}
	if e := s.validatePayloadOwnership(ctx, v); e != nil {
		return e
	}
	if e := s.validatePayloadLinks(ctx, s.db, v); e != nil {
		return e
	}
	ps := payload.NewSession(s.payloadCodec, s.payloadNamespace)
	defer ps.Close()
	return ps.Hydrate(ctx, v, func(ctx context.Context, id string) (payload.Record, error) {
		r, e := loadOne[payload.Record](ctx, s.db, "SELECT data FROM "+s.payloadTable("sensitive_payloads")+" WHERE id=?", id)
		if e != nil {
			return payload.Record{}, e
		}
		if r == nil {
			return payload.Record{}, payload.ErrInvalid
		}
		return *r, nil
	})
}
func (s *store) hydrateIngest(ctx context.Context, r *core.IngestResult) error {
	if r.Ignored {
		return nil
	}
	if e := s.hydrate(ctx, &r.Message); e != nil {
		return e
	}
	if r.Turn != nil {
		if e := s.hydrate(ctx, r.Turn); e != nil {
			return e
		}
	}
	if r.Stopped != nil {
		if e := s.hydrate(ctx, r.Stopped); e != nil {
			return e
		}
	}
	if r.Reply != nil {
		return s.hydrate(ctx, r.Reply)
	}
	return nil
}
func (s *store) hydrateTurnLease(ctx context.Context, r *core.TurnLease) error {
	if e := s.hydrate(ctx, &r.Turn); e != nil {
		return e
	}
	return s.hydrate(ctx, &r.Message)
}

func (s *store) restoreInput(current, original *core.Turn) error {
	if !reflect.DeepEqual(current.Payloads, original.Payloads) {
		return core.ErrConflict
	}
	current.Input = original.Input
	return nil
}

func (s *store) exactPayloadBinding(ctx context.Context, q queryer, id, version string) (core.Binding, error) {
	if id == "" || version == "" {
		return core.Binding{}, core.ErrConflict
	}
	v, e := loadOne[core.BindingVersion](ctx, q, "SELECT data FROM "+s.t.versions+" WHERE binding_id=? AND version=?", id, version)
	if e != nil {
		return core.Binding{}, e
	}
	if v == nil || v.BindingID != id || v.Content.ID != id {
		return core.Binding{}, core.ErrConflict
	}
	return v.Content, nil
}
func (s *store) messagePayloadBinding(ctx context.Context, q queryer, m core.InboundMessage) (core.Binding, error) {
	if m.BindingVersion != "" {
		return s.exactPayloadBinding(ctx, q, m.BindingID, m.BindingVersion)
	}
	turn, e := loadOne[core.Turn](ctx, q, "SELECT data FROM "+s.t.turns+" WHERE message_id=?", m.ID)
	if e != nil {
		return core.Binding{}, e
	}
	if turn != nil {
		if turn.BindingID != m.BindingID {
			return core.Binding{}, core.ErrConflict
		}
		return s.exactPayloadBinding(ctx, q, m.BindingID, turn.BindingVersion)
	}
	// 旧控制消息没有Turn或精确版本字段，只接受全部保留历史配置一致的归属证明。
	var binding core.Binding
	found := false
	after := ""
	for {
		versions, e := loadMany[core.BindingVersion](ctx, q, "SELECT data FROM "+s.t.versions+" WHERE binding_id=? AND version>? ORDER BY version LIMIT 100", m.BindingID, after)
		if e != nil {
			return binding, e
		}
		for _, v := range versions {
			if v.BindingID != m.BindingID || v.Content.ID != m.BindingID {
				return binding, core.ErrConflict
			}
			if found && (v.Content.Owner != binding.Owner || v.Content.Project != binding.Project) {
				return binding, core.ErrConflict
			}
			binding = v.Content
			found = true
			after = v.Version
		}
		if len(versions) < 100 {
			break
		}
	}
	if !found {
		return binding, core.ErrConflict
	}
	return binding, nil
}
func (s *store) payloadSourceBinding(ctx context.Context, q queryer, value any) (core.Binding, error) {
	switch v := value.(type) {
	case core.InboundMessage:
		return s.messagePayloadBinding(ctx, q, v)
	case core.Turn:
		if v.Snapshot != nil {
			return v.Snapshot.Binding, nil
		}
		return s.exactPayloadBinding(ctx, q, v.BindingID, v.BindingVersion)
	case core.SessionSnapshot:
		return v.Spec.Binding, nil
	case core.Interaction:
		turn, err := s.turn(ctx, q, v.TurnID)
		if err != nil {
			return core.Binding{}, err
		}
		if turn == nil || turn.BindingID != v.BindingID {
			return core.Binding{}, core.ErrConflict
		}
		return s.payloadSourceBinding(ctx, q, *turn)
	case core.Delivery:
		if v.Snapshot != nil {
			return v.Snapshot.Binding, nil
		}
		if v.Source.Kind == core.SourceTurn || v.Source.Kind == core.SourceProgress {
			turn, e := s.turn(ctx, q, v.Source.ID)
			if e != nil {
				return core.Binding{}, e
			}
			if turn == nil || turn.BindingID != v.BindingID {
				return core.Binding{}, core.ErrConflict
			}
			return s.payloadSourceBinding(ctx, q, *turn)
		}
		control, e := loadOne[core.ControlRequest](ctx, q, "SELECT data FROM "+s.t.controls+" WHERE id=?", v.Source.ID)
		if e != nil {
			return core.Binding{}, e
		}
		if control == nil {
			return core.Binding{}, core.ErrConflict
		}
		m, e := loadOne[core.InboundMessage](ctx, q, "SELECT data FROM "+s.t.messages+" WHERE id=?", control.MessageID)
		if e != nil {
			return core.Binding{}, e
		}
		if m == nil || m.BindingID != v.BindingID {
			return core.Binding{}, core.ErrConflict
		}
		return s.messagePayloadBinding(ctx, q, *m)
	}
	return core.Binding{}, core.ErrInvalid
}

func (s *store) validatePayloadOwnership(ctx context.Context, target any) error {
	var v any
	var state *core.StoredPayloads
	switch p := target.(type) {
	case *core.InboundMessage:
		if p == nil {
			return nil
		}
		v, state = *p, p.Payloads
	case *core.Turn:
		if p == nil {
			return nil
		}
		v, state = *p, p.Payloads
	case *core.SessionSnapshot:
		if p == nil {
			return nil
		}
		v, state = *p, p.Payloads
	case *core.Delivery:
		if p == nil {
			return nil
		}
		v, state = *p, p.Payloads
	case *core.Interaction:
		if p == nil {
			return nil
		}
		v, state = *p, p.Payloads
	default:
		return core.ErrInvalid
	}
	if state == nil {
		return payload.ErrPolicyMismatch
	}
	b, e := s.payloadSourceBinding(ctx, s.db, v)
	if e != nil {
		return e
	}
	owner, project := b.Owner, b.Project
	if owner == "" {
		owner = "binding:" + b.ID
	}
	if project == "" {
		project = "default"
	}
	if owner != state.Owner || project != state.Project {
		return payload.ErrInvalid
	}
	return nil
}

func (s *store) validatePayloadLinks(ctx context.Context, q queryer, target any) error {
	var state *core.StoredPayloads
	var kind, id string
	switch v := target.(type) {
	case *core.InboundMessage:
		if v == nil {
			return nil
		}
		state, kind, id = v.Payloads, "message", v.ID
	case *core.Turn:
		if v == nil {
			return nil
		}
		state, kind, id = v.Payloads, "turn", v.ID
	case *core.SessionSnapshot:
		if v == nil {
			return nil
		}
		state, kind, id = v.Payloads, "snapshot", v.ID
	case *core.Delivery:
		if v == nil {
			return nil
		}
		state, kind, id = v.Payloads, "delivery", v.ID
	case *core.Interaction:
		if v == nil {
			return nil
		}
		state, kind, id = v.Payloads, "interaction", v.ID
	default:
		return core.ErrInvalid
	}
	if state == nil {
		return payload.ErrPolicyMismatch
	}
	for field, ref := range state.Refs {
		var actual string
		if e := q.QueryRowContext(ctx, "SELECT payload_id FROM "+s.payloadTable("payload_refs")+" WHERE record_kind=? AND record_id=? AND field=?", kind, id, field).Scan(&actual); e != nil {
			return e
		}
		if actual != ref {
			return payload.ErrInvalid
		}
	}
	return nil
}

// restoreInteraction 只在锁外解密；事务中确认不可变载荷仍与读取快照一致。
func restoreInteraction(current, original *core.Interaction) error {
	if original == nil || !reflect.DeepEqual(current.Payloads, original.Payloads) {
		return core.ErrConflict
	}
	current.InputPreview, current.DenyReason = original.InputPreview, original.DenyReason
	return nil
}
