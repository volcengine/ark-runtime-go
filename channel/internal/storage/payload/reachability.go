// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package payload

import (
	"encoding/json"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// References verifies required slots and their identities before a record can
// be pruned or used as a GC root. Unknown work keeps its raw envelope links.
func References(namespace, kind, id string, raw []byte) (map[string]string, error) {
	var record interface{ IsOpaqueStored() bool }
	switch kind {
	case "message":
		record = &core.InboundMessage{}
	case "turn":
		record = &core.Turn{}
	case "snapshot":
		record = &core.SessionSnapshot{}
	case "delivery":
		record = &core.Delivery{}
	case "interaction":
		record = &core.Interaction{}
	default:
		return nil, ErrInvalid
	}
	if err := core.DecodeStored(raw, record); err != nil {
		return nil, ErrInvalid
	}
	if record.IsOpaqueStored() {
		return opaqueReferences(raw)
	}
	state, _, storedID, fields, _, err := metadata(record)
	if err != nil || storedID != id || state.Refs == nil {
		return nil, ErrInvalid
	}
	for _, field := range fields {
		_, expected, err := referenceIdentity(namespace, state, kind, id, field)
		if err != nil || state.Refs[field] != expected {
			return nil, ErrInvalid
		}
	}
	return state.Refs, nil
}

func opaqueReferences(raw []byte) (map[string]string, error) {
	var envelope struct {
		Payloads *struct {
			Refs map[string]string `json:"refs"`
		} `json:"_payloads"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, ErrInvalid
	}
	if envelope.Payloads == nil || len(envelope.Payloads.Refs) == 0 {
		// No inspectable links for this protocol. Preserve all ciphertext.
		return nil, core.ErrUnsupportedWork
	}
	for field, ref := range envelope.Payloads.Refs {
		if field == "" || ref == "" {
			return nil, ErrInvalid
		}
	}
	return envelope.Payloads.Refs, nil
}
