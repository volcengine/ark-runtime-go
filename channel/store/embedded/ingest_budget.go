// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

import (
	"bytes"
	"encoding/binary"
	"time"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

var bucketRejected = []byte("rejected_messages")

func (s *store) channelAtCapacity(tx storeTx, b core.Binding) (bool, error) {
	if s.maxChannelOpenTurns == 0 {
		return false, nil
	}
	count := 0
	c := tx.Bucket(bucketOpenTurns).Cursor()
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		t, err := getTurn(tx, string(k))
		if err != nil {
			return false, err
		}
		if t == nil {
			return false, core.ErrConflict
		}
		v, err := getJSON[core.BindingVersion](tx, bucketVersions, key([]byte(t.BindingID), []byte(t.BindingVersion)))
		if err != nil {
			return false, err
		}
		if v == nil {
			return false, core.ErrConflict
		}
		if core.IngestChannelScope(v.Content) == core.IngestChannelScope(b) {
			count++
			if count >= s.maxChannelOpenTurns {
				return true, nil
			}
		}
	}
	return false, nil
}
func (s *store) rejectedResult(tx storeTx, message core.InboundMessage) (*core.IngestResult, error) {
	c := tx.Bucket(bucketRejected).Cursor()
	prefix := append([]byte(message.ID), 0)
	k, _ := c.Seek(prefix)
	if k == nil || !bytes.HasPrefix(k, prefix) {
		return nil, nil
	}
	r, err := getJSON[core.RejectedMessage](tx, bucketRejected, k)
	if err != nil {
		return nil, err
	}
	control, err := getJSON[core.ControlRequest](tx, bucketControls, []byte(r.ControlID))
	if err != nil {
		return nil, err
	}
	d, err := getDelivery(tx, r.DeliveryID)
	if err != nil {
		return nil, err
	}
	if d == nil || control == nil {
		return nil, core.ErrConflict
	}
	return &core.IngestResult{Message: message, Duplicate: true, Rejected: true, Control: control, Reply: &d.Delivery}, nil
}
func (s *store) reserveRejection(tx storeTx, message core.InboundMessage, controlID, deliveryID, eventID string, now time.Time) error {
	if s.maxRejectedMessages == 0 || countKeys(tx.Bucket(bucketRejected)) >= s.maxRejectedMessages {
		return core.ErrQueueFull
	}
	return putJSON(tx, bucketRejected, key([]byte(message.ID), []byte(eventID)), core.RejectedMessage{MessageID: message.ID, ControlID: controlID, DeliveryID: deliveryID, CreatedAt: now.UnixNano()})
}
func (s *store) persistRejected(tx storeTx, message core.InboundMessage, v core.BindingVersion, now time.Time, result *core.IngestResult) error {
	if s.maxRejectedMessages == 0 {
		return core.ErrQueueFull
	}
	var control *core.ControlRequest
	var reply *core.Delivery
	c := tx.Bucket(bucketControlsByTime).Cursor()
	cutoff := nanos(now.Add(-s.rejectNoticeInterval))
	for k, _ := c.Last(); k != nil && binary.BigEndian.Uint64(k[:8]) > cutoff; k, _ = c.Prev() {
		r, err := getJSON[core.ControlRequest](tx, bucketControls, k[8:])
		if err != nil {
			return err
		}
		if r == nil || r.Kind != core.RejectControlKind || r.ConversationID != message.ConversationID {
			continue
		}
		d := core.NewControlReply(*r, message, v.Content, "")
		stored, err := getDelivery(tx, d.ID)
		if err != nil {
			return err
		}
		if stored != nil {
			control = r
			reply = &stored.Delivery
			break
		}
	}
	if control == nil {
		r, d := core.NewRejectControl(message, v)
		r.CreatedAt = now
		if err := putJSON(tx, bucketControls, []byte(r.ID), r); err != nil {
			return err
		}
		if err := tx.Bucket(bucketControlsByTime).Put(timeKey(nanos(now), r.ID), nil); err != nil {
			return err
		}
		if err := s.insertDeliveries(tx, d.Source, []core.Delivery{d}); err != nil {
			return err
		}
		stored, err := getDelivery(tx, d.ID)
		if err != nil {
			return err
		}
		control = &r
		reply = &stored.Delivery
	}
	if err := s.reserveRejection(tx, message, control.ID, reply.ID, message.ProviderEventID, now); err != nil {
		return err
	}
	result.Rejected = true
	result.Control = control
	result.Reply = reply
	return nil
}
func (s *store) pruneRejectedSource(tx storeTx, id string, cutoff uint64, removed *core.PruneResult) (bool, error) {
	prefix := sourcePrefix(core.DeliverySource{Kind: core.SourceControl, ID: id})
	dc := tx.Bucket(bucketDeliveryBySrc).Cursor()
	for k, v := dc.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = dc.Next() {
		d, err := getDelivery(tx, string(v))
		if err != nil {
			return false, err
		}
		if d == nil || d.Status != core.DeliveryDone {
			return false, nil
		}
	}
	var keys [][]byte
	messages := map[string]bool{}
	c := tx.Bucket(bucketRejected).Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		var r core.RejectedMessage
		if err := core.DecodeStored(v, &r); err != nil {
			return false, err
		}
		if r.ControlID != id {
			continue
		}
		if uint64(r.CreatedAt) >= cutoff {
			return false, nil
		}
		keys = append(keys, append([]byte(nil), k...))
		messages[r.MessageID] = true
	}
	for messageID := range messages {
		if supported, err := s.pruneMessageSupported(tx, messageID); err != nil || !supported {
			return false, err
		}
	}
	for messageID := range messages {
		if _, err := deleteSource(tx, core.DeliverySource{Kind: core.SourceControl, ID: ""}, messageID); err != nil {
			return false, err
		}
	}
	for _, k := range keys {
		if err := tx.Bucket(bucketRejected).Delete(k); err != nil {
			return false, err
		}
	}
	if len(messages) > 1 {
		removed.Messages += len(messages) - 1
	}
	return true, nil
}
