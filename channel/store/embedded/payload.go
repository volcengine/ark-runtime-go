// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"

	"go.etcd.io/bbolt"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/payload"
)

// storeTx 显式传递事务依赖；原始bbolt事务仅用于结构和策略检查。
type storeTx interface{ Bucket([]byte) *bbolt.Bucket }

var (
	bucketPayloads    = []byte("sensitive_payloads")
	bucketPayloadRefs = []byte("payload_refs")
	keyPayloadMode    = []byte("payload_mode")
)

type payloadTx struct {
	*bbolt.Tx
	session *payload.Session
	ctx     context.Context
	binding *core.Binding
	store   *store
}

func (s *store) update(ctx context.Context, fn func(storeTx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.payloadCodec == nil {
		return s.db.Update(func(tx *bbolt.Tx) error { return fn(tx) })
	}
	ps := payload.NewSession(s.payloadCodec, s.payloadNamespace)
	defer ps.Close()
	for range 3 {
		err := s.db.Update(func(tx *bbolt.Tx) error {
			if err := fn(&payloadTx{Tx: tx, session: ps, store: s, ctx: ctx}); err != nil {
				return err
			}
			if ps.Pending() {
				return payload.ErrPrepare
			}
			return ctx.Err()
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
func (s *store) view(ctx context.Context, fn func(storeTx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.db.View(func(tx *bbolt.Tx) error { return fn(tx) })
}
func (t *payloadTx) put(bucket, id []byte, value any) error {
	var target any
	var binding core.Binding
	kind := ""
	recordID := ""
	var turn *turnRecord
	var delivery *deliveryRecord
	switch v := value.(type) {
	case core.InboundMessage:
		target = v
		kind = "message"
		recordID = v.ID
		var err error
		binding, err = t.store.messagePayloadBinding(t, v)
		if err != nil {
			return err
		}
	case turnRecord:
		turn = &v
		target = v.Turn
		kind = "turn"
		recordID = v.ID
		var err error
		binding, err = t.store.turnPayloadBinding(t, v.Turn)
		if err != nil {
			return err
		}
	case core.Turn:
		target = v
		kind = "turn"
		recordID = v.ID
		var err error
		binding, err = t.store.turnPayloadBinding(t, v)
		if err != nil {
			return err
		}
	case core.SessionSnapshot:
		target = v
		kind = "snapshot"
		recordID = v.ID
		binding = v.Spec.Binding
	case core.Interaction:
		target, kind, recordID = v, "interaction", v.ID
		var err error
		binding, err = t.store.interactionPayloadBinding(t, v)
		if err != nil {
			return err
		}
	case deliveryRecord:
		delivery = &v
		target = v.Delivery
		kind = "delivery"
		recordID = v.ID
		var err error
		binding, err = t.store.deliveryPayloadBinding(t, v.Delivery)
		if err != nil {
			return err
		}
	case core.Delivery:
		target = v
		kind = "delivery"
		recordID = v.ID
		var err error
		binding, err = t.store.deliveryPayloadBinding(t, v)
		if err != nil {
			return err
		}
	default:
		data, err := core.EncodeStored(value)
		if err != nil {
			return err
		}
		return t.Bucket(bucket).Put(id, data)
	}
	data, refs, err := t.session.Protect(target, binding)
	if err != nil {
		return err
	}
	prepared := map[string]payload.Record{}
	for _, r := range t.session.Records() {
		prepared[r.ID] = r
	}
	for _, ref := range refs {
		if r, ok := prepared[ref]; ok {
			raw, err := core.EncodeStored(r)
			if err != nil {
				return err
			}
			if old := t.Bucket(bucketPayloads).Get([]byte(ref)); old == nil {
				if err = t.Bucket(bucketPayloads).Put([]byte(ref), raw); err != nil {
					return err
				}
			}
		} else if !t.session.Pending() && t.Bucket(bucketPayloads).Get([]byte(ref)) == nil {
			return payload.ErrInvalid
		}
	}
	if !t.session.Pending() {
		var checked any
		switch target.(type) {
		case core.InboundMessage:
			checked = &core.InboundMessage{}
		case core.Turn:
			checked = &core.Turn{}
		case core.SessionSnapshot:
			checked = &core.SessionSnapshot{}
		case core.Delivery:
			checked = &core.Delivery{}
		case core.Interaction:
			checked = &core.Interaction{}
		}
		if err = core.DecodeStored(data, checked); err != nil {
			return err
		}
		if err = t.session.Verify(t.ctx, checked, func(_ context.Context, ref string) (payload.Record, error) { return payloadRecord(t, ref) }); err != nil {
			return err
		}
	}
	var stored struct {
		Payloads *core.StoredPayloads `json:"_payloads"`
	}
	if err = core.DecodeStored(data, &stored); err != nil {
		return err
	}
	for field, ref := range stored.Payloads.Refs {
		k := key([]byte(kind), []byte(recordID), []byte(field))
		if prior := t.Bucket(bucketPayloadRefs).Get(k); prior != nil && !bytes.Equal(prior, []byte(ref)) {
			return core.ErrConflict
		}
		if err = t.Bucket(bucketPayloadRefs).Put(k, []byte(ref)); err != nil {
			return err
		}
	}
	if turn != nil {
		turn.Turn = core.Turn{}
		if err = core.DecodeStored(data, &turn.Turn); err != nil {
			return err
		}
		data, err = core.EncodeStored(turn)
	}
	if delivery != nil {
		delivery.Delivery = core.Delivery{}
		if err = core.DecodeStored(data, &delivery.Delivery); err != nil {
			return err
		}
		data, err = core.EncodeStored(delivery)
	}
	if err != nil {
		return err
	}
	return t.Bucket(bucket).Put(id, data)
}
func (s *store) turnPayloadBinding(tx storeTx, t core.Turn) (core.Binding, error) {
	if t.Snapshot != nil {
		return t.Snapshot.Binding, nil
	}
	v, err := getJSON[core.BindingVersion](tx, bucketVersions, key([]byte(t.BindingID), []byte(t.BindingVersion)))
	if err != nil {
		return core.Binding{}, err
	}
	if v == nil {
		return core.Binding{}, payload.ErrInvalid
	}
	return v.Content, nil
}
func (s *store) exactPayloadBinding(tx storeTx, bindingID, version string) (core.Binding, error) {
	if bindingID == "" || version == "" {
		return core.Binding{}, core.ErrConflict
	}
	v, err := getJSON[core.BindingVersion](tx, bucketVersions, key([]byte(bindingID), []byte(version)))
	if err != nil {
		return core.Binding{}, err
	}
	if v == nil || v.BindingID != bindingID || v.Content.ID != bindingID {
		return core.Binding{}, core.ErrConflict
	}
	return v.Content, nil
}
func (s *store) messagePayloadBinding(tx storeTx, m core.InboundMessage) (core.Binding, error) {
	if protected, ok := tx.(*payloadTx); ok && protected.binding != nil && protected.binding.ID == m.BindingID {
		return *protected.binding, nil
	}
	if m.BindingVersion != "" {
		return s.exactPayloadBinding(tx, m.BindingID, m.BindingVersion)
	}
	if m.Seq > 0 {
		t, err := getTurn(tx, fmt.Sprintf("turn:%020d", m.Seq))
		if err != nil {
			return core.Binding{}, err
		}
		if t != nil {
			if t.MessageID != m.ID || t.BindingID != m.BindingID {
				return core.Binding{}, core.ErrConflict
			}
			return s.exactPayloadBinding(tx, m.BindingID, t.BindingVersion)
		}
	}
	var b *core.Binding
	err := tx.Bucket(bucketVersions).ForEach(func(_, raw []byte) error {
		var v core.BindingVersion
		if err := core.DecodeStored(raw, &v); err != nil {
			return err
		}
		if v.BindingID == m.BindingID {
			if v.Content.ID != m.BindingID {
				return core.ErrConflict
			}
			if b != nil && (b.Owner != v.Content.Owner || b.Project != v.Content.Project) {
				return core.ErrConflict
			}
			copy := v.Content
			b = &copy
		}
		return nil
	})
	if err != nil {
		return core.Binding{}, err
	}
	if b == nil {
		return core.Binding{}, core.ErrConflict
	}
	return *b, nil
}
func (s *store) deliveryPayloadBinding(tx storeTx, d core.Delivery) (core.Binding, error) {
	if d.Snapshot != nil {
		return d.Snapshot.Binding, nil
	}
	if d.Source.Kind == core.SourceTurn || d.Source.Kind == core.SourceProgress {
		t, err := getTurn(tx, d.Source.ID)
		if err != nil {
			return core.Binding{}, err
		}
		if t == nil || t.BindingID != d.BindingID {
			return core.Binding{}, core.ErrConflict
		}
		return s.turnPayloadBinding(tx, t.Turn)
	}
	if d.Source.Kind != core.SourceControl {
		return core.Binding{}, core.ErrConflict
	}
	control, err := getJSON[core.ControlRequest](tx, bucketControls, []byte(d.Source.ID))
	if err != nil {
		return core.Binding{}, err
	}
	if control == nil {
		return core.Binding{}, core.ErrConflict
	}
	m, err := getJSON[core.InboundMessage](tx, bucketMessages, []byte(control.MessageID))
	if err != nil {
		return core.Binding{}, err
	}
	if m == nil || m.BindingID != d.BindingID {
		return core.Binding{}, core.ErrConflict
	}
	return s.messagePayloadBinding(tx, *m)
}
func payloadMessageID(bindingID, providerID string) string {
	b, _ := core.EncodeStored([]string{bindingID, providerID})
	sum := sha256.Sum256(b)
	return "msg:" + hex.EncodeToString(sum[:])
}
func (s *store) controlID(m core.InboundMessage) string {
	if s.payloadCodec != nil && len(m.ID) == 68 {
		return "ctl:" + m.ID
	}
	return fmt.Sprintf("ctl:%020d", m.Seq)
}
func (s *store) hydrate(ctx context.Context, v any) error {
	if s.payloadCodec == nil {
		return nil
	}
	if record, ok := v.(interface{ IsOpaqueStored() bool }); ok {
		value := reflect.ValueOf(v)
		if value.Kind() == reflect.Pointer && value.IsNil() || record.IsOpaqueStored() {
			return nil
		}
	}
	if err := s.verifyReadPayload(ctx, v); err != nil {
		return err
	}
	ps := payload.NewSession(s.payloadCodec, s.payloadNamespace)
	defer ps.Close()
	return ps.Hydrate(ctx, v, func(ctx context.Context, id string) (payload.Record, error) {
		var out payload.Record
		err := s.db.View(func(tx *bbolt.Tx) error {
			raw := tx.Bucket(bucketPayloads).Get([]byte(id))
			if raw == nil {
				return payload.ErrInvalid
			}
			return core.DecodeStored(bytes.Clone(raw), &out)
		})
		return out, err
	})
}
func (s *store) hydrateIngest(ctx context.Context, r *core.IngestResult) error {
	if r.Ignored {
		return nil
	}
	if err := s.hydrate(ctx, &r.Message); err != nil {
		return err
	}
	if r.Turn != nil {
		if err := s.hydrate(ctx, r.Turn); err != nil {
			return err
		}
	}
	if r.Stopped != nil {
		if err := s.hydrate(ctx, r.Stopped); err != nil {
			return err
		}
	}
	if r.Reply != nil {
		return s.hydrate(ctx, r.Reply)
	}
	return nil
}
func (s *store) hydrateTurnLease(ctx context.Context, l *core.TurnLease) error {
	if l == nil {
		return nil
	}
	if err := s.hydrate(ctx, &l.Turn); err != nil {
		return err
	}
	return s.hydrate(ctx, &l.Message)
}

func payloadRecord(tx storeTx, id string) (payload.Record, error) {
	var r payload.Record
	raw := tx.Bucket(bucketPayloads).Get([]byte(id))
	if raw == nil {
		return r, payload.ErrInvalid
	}
	err := core.DecodeStored(bytes.Clone(raw), &r)
	return r, err
}
func (s *store) captureIngestMetadata(tx storeTx, r *core.IngestResult) error {
	if r.Ignored {
		return nil
	}
	if s.payloadCodec == nil {
		return nil
	}
	m, err := getJSON[core.InboundMessage](tx, bucketMessages, []byte(r.Message.ID))
	if err != nil {
		return err
	}
	if m == nil {
		return payload.ErrInvalid
	}
	r.Message = *m
	for _, turn := range []*core.Turn{r.Turn, r.Stopped} {
		if turn == nil {
			continue
		}
		stored, err := getTurn(tx, turn.ID)
		if err != nil {
			return err
		}
		if stored == nil {
			return payload.ErrInvalid
		}
		*turn = stored.Turn
	}
	if r.Reply != nil {
		stored, err := getDelivery(tx, r.Reply.ID)
		if err != nil {
			return err
		}
		if stored == nil {
			return payload.ErrInvalid
		}
		*r.Reply = stored.Delivery
	}
	return nil
}
func (s *store) verifyReadPayload(ctx context.Context, value any) error {
	return s.view(ctx, func(tx storeTx) error {
		var state *core.StoredPayloads
		var b core.Binding
		var kind, id string
		var err error
		switch v := value.(type) {
		case *core.InboundMessage:
			if v == nil {
				return nil
			}
			state = v.Payloads
			kind, id = "message", v.ID
			if state == nil {
				return payload.ErrPolicyMismatch
			}
			b, err = s.messagePayloadBinding(tx, *v)
		case *core.Turn:
			if v == nil {
				return nil
			}
			state = v.Payloads
			kind, id = "turn", v.ID
			if state == nil {
				return payload.ErrPolicyMismatch
			}
			b, err = s.turnPayloadBinding(tx, *v)
		case *core.SessionSnapshot:
			if v == nil {
				return nil
			}
			state = v.Payloads
			kind, id = "snapshot", v.ID
			if state == nil {
				return payload.ErrPolicyMismatch
			}
			b = v.Spec.Binding
		case *core.Delivery:
			if v == nil {
				return nil
			}
			state = v.Payloads
			kind, id = "delivery", v.ID
			if state == nil {
				return payload.ErrPolicyMismatch
			}
			b, err = s.deliveryPayloadBinding(tx, *v)
		case *core.Interaction:
			if v == nil {
				return nil
			}
			state = v.Payloads
			kind, id = "interaction", v.ID
			if state == nil {
				return payload.ErrPolicyMismatch
			}
			b, err = s.interactionPayloadBinding(tx, *v)
		default:
			return payload.ErrInvalid
		}
		if err != nil {
			return err
		}
		owner, project := b.Owner, b.Project
		if owner == "" {
			owner = "binding:" + b.ID
		}
		if project == "" {
			project = "default"
		}
		if state.Owner != owner || state.Project != project {
			return payload.ErrInvalid
		}
		ps := payload.NewSession(s.payloadCodec, s.payloadNamespace)
		defer ps.Close()
		if err := ps.Verify(ctx, value, func(_ context.Context, id string) (payload.Record, error) { return payloadRecord(tx, id) }); err != nil {
			return err
		}
		for field, ref := range state.Refs {
			if !bytes.Equal(tx.Bucket(bucketPayloadRefs).Get(key([]byte(kind), []byte(id), []byte(field))), []byte(ref)) {
				return payload.ErrInvalid
			}
		}
		return nil
	})
}

func (s *store) interactionPayloadBinding(tx storeTx, interaction core.Interaction) (core.Binding, error) {
	turn, err := getTurn(tx, interaction.TurnID)
	if err != nil {
		return core.Binding{}, err
	}
	if turn == nil || turn.BindingID != interaction.BindingID {
		return core.Binding{}, core.ErrConflict
	}
	return s.turnPayloadBinding(tx, turn.Turn)
}

func restoreInteraction(current, original *core.Interaction) error {
	if original == nil || !reflect.DeepEqual(current.Payloads, original.Payloads) {
		return core.ErrConflict
	}
	current.InputPreview, current.DenyReason = original.InputPreview, original.DenyReason
	return nil
}
