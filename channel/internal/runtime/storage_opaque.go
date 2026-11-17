// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"bytes"
	"encoding/json"
	"reflect"
)

// DecodeOpaqueStored 为未知协议保留整条原文，只提供有限string身份字段的只读视图。
// kind来自调用方显式说明的格式或早期协议头；legacy仍严格解码，不能借此绕过校验。
func DecodeOpaqueStored(data []byte, out any, kind string) error {
	if kind == "" || kind == WorkKindLegacy {
		return ErrInvalid
	}
	value := reflect.ValueOf(out)
	if !value.IsValid() || value.Kind() != reflect.Pointer || value.IsNil() || !json.Valid(data) {
		return ErrInvalid
	}
	view := reflect.New(value.Elem().Type()).Elem()
	state := ensureOpaqueCarrier(view)
	if state == nil {
		return ErrInvalid
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(data, &fields)
	known := storedFields(view.Type())
	for _, name := range []string{"id", "binding_id", "conversation_id", "route", "event_id"} {
		index, ok := known[name]
		if !ok {
			continue
		}
		var text string
		if json.Unmarshal(fields[name], &text) != nil || len(text) > 1024 {
			continue
		}
		field := storedFieldValue(view, index)
		if field.IsValid() && field.CanSet() && field.Kind() == reflect.String {
			field.SetString(text)
		}
	}
	opaqueSafeBody(view)
	state.stored = &storedRecordState{raw: bytes.Clone(data), opaqueKind: kind}
	value.Elem().Set(view)
	return nil
}

func opaqueStoredKind(data []byte, t reflect.Type) (string, bool, error) {
	if !hasStoredCarrier(t) {
		return "", false, nil
	}
	kind, err := StoredWorkKind(data)
	if kind == WorkKindUserAuth && userAuthCarrier(t) {
		return kind, false, err
	}
	return kind, err == nil && kind != WorkKindLegacy && (kind != WorkKindReaction || !hasReactionDeliveryCarrier(t)), err
}

func hasStoredCarrier(t reflect.Type) bool {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return false
	}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.Type == storedRecordType || field.Anonymous && hasStoredCarrier(field.Type) {
			return true
		}
	}
	return false
}

func ensureOpaqueCarrier(value reflect.Value) *StoredRecord {
	if value.Kind() == reflect.Pointer {
		if value.IsNil() && value.CanSet() {
			value.Set(reflect.New(value.Type().Elem()))
		}
		if value.IsNil() {
			return nil
		}
		return ensureOpaqueCarrier(value.Elem())
	}
	if value.Kind() != reflect.Struct {
		return nil
	}
	for i := 0; i < value.NumField(); i++ {
		field := value.Type().Field(i)
		if field.Type == storedRecordType && value.Field(i).CanAddr() {
			return value.Field(i).Addr().Interface().(*StoredRecord)
		}
		if field.Anonymous && hasStoredCarrier(field.Type) {
			if state := ensureOpaqueCarrier(value.Field(i)); state != nil {
				return state
			}
		}
	}
	return nil
}

func readStoredState(value reflect.Value) *storedRecordState {
	value = storedIndirect(value)
	if !value.IsValid() || value.Kind() != reflect.Struct {
		return nil
	}
	for i := 0; i < value.NumField(); i++ {
		field := value.Type().Field(i)
		if field.Type == storedRecordType && value.Field(i).CanInterface() {
			return value.Field(i).Interface().(StoredRecord).stored
		}
		if field.Anonymous {
			if state := readStoredState(value.Field(i)); state != nil {
				return state
			}
		}
	}
	return nil
}

// opaqueSafeBody 避免公开metadata视图的普通JSON编码触发空union错误；不暴露未知业务载荷。
func opaqueSafeBody(value reflect.Value) {
	value = storedIndirect(value)
	if !value.IsValid() || value.Kind() != reflect.Struct {
		return
	}
	if value.Type() == reflect.TypeFor[Delivery]() {
		value.FieldByName("Body").Set(reflect.ValueOf(DeliveryBody{opaque: &opaqueStoredBody{raw: json.RawMessage("null")}}))
		return
	}
	if value.Type() == deliveryBodyType {
		body := value.Addr().Interface().(*DeliveryBody)
		body.opaque = &opaqueStoredBody{raw: json.RawMessage("null")}
		return
	}
	for i := 0; i < value.NumField(); i++ {
		if value.Type().Field(i).Anonymous {
			opaqueSafeBody(value.Field(i))
		}
	}
}

// opaqueDecodeTemplate 仅供容器的临时typed占位解码；随后captureStored恢复整记录opaque。
func opaqueDecodeTemplate(value reflect.Value) {
	value = storedIndirect(value)
	if !value.IsValid() || value.Kind() != reflect.Struct {
		return
	}
	if value.Type() == reflect.TypeFor[Delivery]() {
		value.FieldByName("Body").Set(reflect.ValueOf(TextBody("")))
		return
	}
	if value.Type() == deliveryBodyType {
		value.Set(reflect.ValueOf(TextBody("")))
		return
	}
	for i := 0; i < value.NumField(); i++ {
		if value.Type().Field(i).Anonymous {
			opaqueDecodeTemplate(value.Field(i))
		}
	}
}

func hasReactionDeliveryCarrier(t reflect.Type) bool {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == reflect.TypeFor[Delivery]() {
		return true
	}
	if t.Kind() != reflect.Struct {
		return false
	}
	for i := 0; i < t.NumField(); i++ {
		if t.Field(i).Anonymous && hasReactionDeliveryCarrier(t.Field(i).Type) {
			return true
		}
	}
	return false
}

func userAuthCarrier(t reflect.Type) bool {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == reflect.TypeFor[UserAuthRecord]() || t == reflect.TypeFor[Turn]() || t == reflect.TypeFor[RemoteOperation]() {
		return true
	}
	if t.Kind() == reflect.Struct {
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if f.Anonymous && userAuthCarrier(f.Type) {
				return true
			}
		}
	}
	return false
}
