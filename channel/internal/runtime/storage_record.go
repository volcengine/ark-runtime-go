// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"sort"
	"strings"
)

// StoredRecord 保存旧 reader 无法表达的存储字段；普通已知记录不附着状态。
// 此状态只由 DecodeStored/EncodeStored 使用，不进入领域 JSON、内容哈希或日志。
type StoredRecord struct {
	stored *storedRecordState
}

type storedRecordState struct {
	raw        json.RawMessage
	baseline   json.RawMessage
	opaqueKind string
}

// IsOpaqueStored 表示整条记录属于未知协议；其metadata视图不能用于业务执行或写入。
func (s StoredRecord) IsOpaqueStored() bool { return s.stored != nil && s.stored.opaqueKind != "" }

// StoredWorkKind 返回兼容读取的早期 JSON 头或显式 opaque 格式，不驱动 SQL 分派。
func (s StoredRecord) StoredWorkKind() string {
	if s.IsOpaqueStored() {
		return s.stored.opaqueKind
	}
	if s.stored != nil {
		if kind, err := StoredWorkKind(s.stored.raw); err == nil {
			return kind
		}
	}
	return WorkKindLegacy
}

type frozenStoredJSON struct {
	raw      json.RawMessage
	baseline json.RawMessage
	digest   string
}

func digestBytes(data []byte) string {
	var compact bytes.Buffer
	if json.Compact(&compact, data) != nil {
		return ""
	}
	h := sha256.Sum256(compact.Bytes())
	return hex.EncodeToString(h[:])
}

// ForgetStoredFields 丢弃指定字段下的原文扩展；payload 脱敏时必须同时清除敏感子树。
// 路径使用持久 JSON 字段名，以点分隔；复制后修改，不影响原对象或其他 goroutine。
func (s *StoredRecord) ForgetStoredFields(paths ...string) {
	if s.stored == nil || s.IsOpaqueStored() {
		return
	}
	raw := s.stored.raw
	for _, path := range paths {
		raw = forgetStoredPath(raw, strings.Split(path, "."))
	}
	s.stored = &storedRecordState{raw: raw, baseline: s.stored.baseline}
}

func forgetStoredPath(raw []byte, path []string) []byte {
	var object map[string]json.RawMessage
	if len(path) == 0 || json.Unmarshal(raw, &object) != nil || object == nil {
		return raw
	}
	if len(path) == 1 {
		delete(object, path[0])
	} else if child, ok := object[path[0]]; ok {
		object[path[0]] = forgetStoredPath(child, path[1:])
	}
	out, _ := json.Marshal(object)
	return out
}

var storedRecordType = reflect.TypeFor[StoredRecord]()
var deliveryBodyType = reflect.TypeFor[DeliveryBody]()

// DecodeStored 解码持久记录，并为无法由当前类型表达的信息附着私有原文。
// 未知 DeliveryBody 保持 opaque，不能据此认定执行器支持其业务语义。
func DecodeStored(data []byte, out any) error {
	v := reflect.ValueOf(out)
	if !v.IsValid() || v.Kind() != reflect.Pointer || v.IsNil() {
		return ErrInvalid
	}
	if kind, opaque, err := opaqueStoredKind(data, v.Elem().Type()); err != nil {
		return err
	} else if opaque {
		return DecodeOpaqueStored(data, out, kind)
	}
	prepared, err := prepareStoredDecode(data, v.Elem().Type())
	if err != nil {
		return err
	}
	if err = json.Unmarshal(prepared, out); err != nil {
		return err
	}
	return captureStored(data, v.Elem())
}

// EncodeStored 编码持久记录；本次类型拥有的字段以当前值为准，其余原文递归保留。
// map 删除和 omitempty 清零均为显式的当前视图，数组只按稳定身份继承扩展。
func EncodeStored(value any) ([]byte, error) {
	if state := readStoredState(reflect.ValueOf(value)); state != nil && state.opaqueKind != "" {
		return bytes.Clone(state.raw), nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if !hasStoredState(reflect.ValueOf(value)) {
		return raw, nil
	}
	return encodeStoredValue(raw, reflect.ValueOf(value))
}

// hasStoredState 只读检查对象图，普通页直接复用标准编码，避免逐字段解析JSON。
// 子项可能在独立解密时获得原文，不能只检查聚合根。
func hasStoredState(value reflect.Value) bool {
	if !value.IsValid() {
		return false
	}
	t := value.Type()
	if t == storedRecordType {
		return !value.Field(0).IsNil()
	}
	if t == reflect.TypeFor[*opaqueStoredBody]() || t == reflect.TypeFor[*frozenStoredJSON]() {
		return !value.IsNil()
	}
	switch value.Kind() {
	case reflect.Pointer, reflect.Interface:
		return !value.IsNil() && hasStoredState(value.Elem())
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			field := value.Field(i)
			if field.CanInterface() || field.Type() == reflect.TypeFor[*opaqueStoredBody]() || field.Type() == reflect.TypeFor[*frozenStoredJSON]() {
				if hasStoredState(field) {
					return true
				}
			}
		}
	case reflect.Array, reflect.Slice:
		if !mayHaveStoredState(t.Elem()) {
			return false
		}
		for i := 0; i < value.Len(); i++ {
			if hasStoredState(value.Index(i)) {
				return true
			}
		}
	case reflect.Map:
		if !mayHaveStoredState(t.Elem()) {
			return false
		}
		iter := value.MapRange()
		for iter.Next() {
			if hasStoredState(iter.Value()) {
				return true
			}
		}
	}
	return false
}

func mayHaveStoredState(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Struct, reflect.Pointer, reflect.Interface, reflect.Array, reflect.Slice, reflect.Map:
		return true
	}
	return false
}

func storedIndirect(v reflect.Value) reflect.Value {
	for v.IsValid() && (v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface) {
		if v.IsNil() {
			return reflect.Value{}
		}
		v = v.Elem()
	}
	return v
}

func storedFields(t reflect.Type) map[string][]int {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	fields := map[string][]int{}
	if t.Kind() != reflect.Struct {
		return fields
	}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		name := strings.Split(field.Tag.Get("json"), ",")[0]
		if field.PkgPath != "" || name == "-" || field.Type == storedRecordType {
			continue
		}
		if field.Anonymous && name == "" {
			for key, index := range storedFields(field.Type) {
				fields[key] = append([]int{i}, index...)
			}
			continue
		}
		if name == "" {
			name = field.Name
		}
		fields[name] = []int{i}
	}
	return fields
}

func storedFieldValue(v reflect.Value, index []int) reflect.Value {
	for _, i := range index {
		v = storedIndirect(v)
		if !v.IsValid() {
			return v
		}
		v = v.Field(i)
	}
	return v
}

func storedFieldType(t reflect.Type, index []int) reflect.Type {
	for _, i := range index {
		for t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
		t = t.Field(i).Type
	}
	return t
}

func recordState(v reflect.Value) *StoredRecord {
	v = storedIndirect(v)
	if !v.IsValid() || v.Kind() != reflect.Struct {
		return nil
	}
	for i := 0; i < v.NumField(); i++ {
		field := v.Type().Field(i)
		if field.Type == storedRecordType && v.Field(i).CanAddr() {
			return v.Field(i).Addr().Interface().(*StoredRecord)
		}
		if field.Anonymous {
			if state := recordState(v.Field(i)); state != nil {
				return state
			}
		}
	}
	return nil
}

func prepareStoredDecode(raw []byte, t reflect.Type) ([]byte, error) {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if kind, opaque, err := opaqueStoredKind(raw, t); err != nil {
		return nil, err
	} else if opaque {
		value := reflect.New(t)
		if err = DecodeOpaqueStored(raw, value.Interface(), kind); err != nil {
			return nil, err
		}
		opaqueDecodeTemplate(value.Elem())
		return json.Marshal(value.Interface())
	}
	if t == deliveryBodyType {
		var head struct {
			Type BodyType `json:"type"`
		}
		if err := json.Unmarshal(raw, &head); err != nil {
			return nil, err
		}
		if !supportedStoredBody(head.Type) {
			return []byte(`{"type":"text","text":""}`), nil
		}
		return raw, nil
	}
	if t.Kind() == reflect.Struct {
		var object map[string]json.RawMessage
		if err := json.Unmarshal(raw, &object); err != nil {
			// time.Time 等自定义标量解码器自己验证其 wire 类型。
			return raw, nil
		}
		for name, index := range storedFields(t) {
			if child, ok := object[name]; ok {
				next, err := prepareStoredDecode(child, storedFieldType(t, index))
				if err != nil {
					return nil, err
				}
				object[name] = next
			}
		}
		return json.Marshal(object)
	}
	if t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
		var items []json.RawMessage
		if json.Unmarshal(raw, &items) != nil {
			return raw, nil
		}
		for i, item := range items {
			next, err := prepareStoredDecode(item, t.Elem())
			if err != nil {
				return nil, err
			}
			items[i] = next
		}
		return json.Marshal(items)
	}
	if t.Kind() == reflect.Map && t.Key().Kind() == reflect.String {
		var object map[string]json.RawMessage
		if json.Unmarshal(raw, &object) != nil {
			return raw, nil
		}
		for key, child := range object {
			next, err := prepareStoredDecode(child, t.Elem())
			if err != nil {
				return nil, err
			}
			object[key] = next
		}
		return json.Marshal(object)
	}
	return raw, nil
}

func supportedStoredBody(kind BodyType) bool {
	return kind == BodyRichText || kind == BodySessionFile || kind == BodyText || kind == BodyStreamText || kind == BodyImageStream || kind == BodyArtifact || kind == BodyOutputFile || kind == BodyProgress
}

func captureStored(raw []byte, value reflect.Value) error {
	v := storedIndirect(value)
	if !v.IsValid() {
		return nil
	}
	if kind, opaque, err := opaqueStoredKind(raw, v.Type()); err != nil {
		return err
	} else if opaque {
		if !v.CanAddr() {
			return ErrInvalid
		}
		return DecodeOpaqueStored(raw, v.Addr().Interface(), kind)
	}
	if v.Type() == deliveryBodyType {
		var head struct {
			Type BodyType `json:"type"`
		}
		if err := json.Unmarshal(raw, &head); err != nil {
			return err
		}
		if !supportedStoredBody(head.Type) {
			*v.Addr().Interface().(*DeliveryBody) = DeliveryBody{Type: head.Type, opaque: &opaqueStoredBody{kind: head.Type, raw: bytes.Clone(raw)}}
		}
		if hasStoredUnknown(raw, v.Type()) {
			baseline, err := json.Marshal(v.Interface())
			if err != nil {
				return err
			}
			v.Addr().Interface().(*DeliveryBody).stored = &storedRecordState{raw: bytes.Clone(raw), baseline: baseline}
		}
		return nil
	}
	if v.Kind() == reflect.Map && v.Type().Key().Kind() == reflect.String {
		var object map[string]json.RawMessage
		_ = json.Unmarshal(raw, &object)
		for key, child := range object {
			mapKey := reflect.ValueOf(key).Convert(v.Type().Key())
			item := v.MapIndex(mapKey)
			if !item.IsValid() {
				continue
			}
			copyValue := reflect.New(item.Type()).Elem()
			copyValue.Set(item)
			if err := captureStored(child, copyValue); err != nil {
				return err
			}
			v.SetMapIndex(mapKey, copyValue)
		}
		return nil
	}
	if v.Kind() == reflect.Slice || v.Kind() == reflect.Array {
		var items []json.RawMessage
		_ = json.Unmarshal(raw, &items)
		for i := 0; i < len(items) && i < v.Len(); i++ {
			if err := captureStored(items[i], v.Index(i)); err != nil {
				return err
			}
		}
		return nil
	}
	if v.Kind() != reflect.Struct {
		return nil
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil || object == nil {
		return nil
	}
	for name, index := range storedFields(v.Type()) {
		if child, ok := object[name]; ok {
			if err := captureStored(child, storedFieldValue(v, index)); err != nil {
				return err
			}
		}
	}
	if v.CanAddr() {
		switch snapshot := v.Addr().Interface().(type) {
		case *BindingVersion:
			if len(object["content"]) != 0 && hasStoredUnknown(object["content"], reflect.TypeFor[Binding]()) {
				baseline, _ := json.Marshal(snapshot.Content)
				snapshot.frozenContent = &frozenStoredJSON{raw: bytes.Clone(object["content"]), baseline: baseline, digest: strings.TrimPrefix(snapshot.Version, "bv1:")}
			}
		case *SessionSnapshot:
			if snapshot.Payloads == nil && len(object["spec"]) != 0 {
				snapshot.captureFrozenSpec(object["spec"])
			}
		case *TurnSnapshot:
			if len(object["binding"]) != 0 && hasStoredUnknown(object["binding"], reflect.TypeFor[Binding]()) {
				baseline, _ := json.Marshal(snapshot.Binding)
				snapshot.frozenBinding = &frozenStoredJSON{raw: bytes.Clone(object["binding"]), baseline: baseline, digest: strings.TrimPrefix(snapshot.ID, "turn-snapshot:")}
			}
		}
	}
	if state := recordState(v); state != nil {
		state.stored = nil
		if hasStoredUnknown(raw, v.Type()) || v.Type() == reflect.TypeFor[SessionSnapshot]() && v.Interface().(SessionSnapshot).frozenSpec != nil {
			baseline, err := json.Marshal(v.Interface())
			if err != nil {
				return err
			}
			state.stored = &storedRecordState{raw: bytes.Clone(raw), baseline: baseline}
		}
	}
	return nil
}

func storedBodyWireType(raw []byte) reflect.Type {
	var head struct {
		Type BodyType `json:"type"`
	}
	_ = json.Unmarshal(raw, &head)
	switch head.Type {
	case BodyRichText:
		return reflect.TypeFor[wireRichTextBody]()
	case BodyText, BodyStreamText, BodyImageStream:
		return reflect.TypeFor[wireTextBody]()
	case BodyArtifact, BodySessionFile, BodyOutputFile:
		return reflect.TypeFor[wireArtifactBody]()
	case BodyProgress:
		return reflect.TypeFor[wireProgressBody]()
	}
	return nil
}

func hasStoredUnknown(raw []byte, t reflect.Type) bool {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == deliveryBodyType {
		t = storedBodyWireType(raw)
		if t == nil {
			return true
		}
	}
	switch t.Kind() {
	case reflect.Struct:
		var object map[string]json.RawMessage
		if json.Unmarshal(raw, &object) != nil {
			return false
		}
		fields := storedFields(t)
		for name, child := range object {
			index, ok := fields[name]
			if !ok || hasStoredUnknown(child, storedFieldType(t, index)) {
				return true
			}
		}
	case reflect.Slice, reflect.Array:
		var items []json.RawMessage
		_ = json.Unmarshal(raw, &items)
		for _, item := range items {
			if hasStoredUnknown(item, t.Elem()) {
				return true
			}
		}
	case reflect.Map:
		var object map[string]json.RawMessage
		_ = json.Unmarshal(raw, &object)
		for _, item := range object {
			if hasStoredUnknown(item, t.Elem()) {
				return true
			}
		}
	}
	return false
}

func encodeStoredValue(raw []byte, value reflect.Value) ([]byte, error) {
	v := storedIndirect(value)
	if !v.IsValid() {
		return raw, nil
	}
	if state := readStoredState(v); state != nil && state.opaqueKind != "" {
		return bytes.Clone(state.raw), nil
	}
	// 值参数的副本可取址，StoredRecord 不要求调用方只传指针。
	if !v.CanAddr() {
		copyValue := reflect.New(v.Type()).Elem()
		copyValue.Set(v)
		v = copyValue
	}
	if v.Kind() == reflect.Slice || v.Kind() == reflect.Array {
		var items []json.RawMessage
		if json.Unmarshal(raw, &items) != nil {
			return raw, nil
		}
		changed := false
		for i := 0; i < len(items) && i < v.Len(); i++ {
			next, err := encodeStoredValue(items[i], v.Index(i))
			if err != nil {
				return nil, err
			}
			if !bytes.Equal(next, items[i]) {
				items[i], changed = next, true
			}
		}
		if changed {
			return json.Marshal(items)
		}
		return raw, nil
	}
	if v.Kind() == reflect.Map && v.Type().Key().Kind() == reflect.String {
		var object map[string]json.RawMessage
		if json.Unmarshal(raw, &object) != nil {
			return raw, nil
		}
		changed := false
		for key, child := range object {
			next, err := encodeStoredValue(child, v.MapIndex(reflect.ValueOf(key).Convert(v.Type().Key())))
			if err != nil {
				return nil, err
			}
			if !bytes.Equal(next, child) {
				object[key], changed = next, true
			}
		}
		if changed {
			return json.Marshal(object)
		}
		return raw, nil
	}
	if v.Kind() == reflect.Struct && v.Type() != deliveryBodyType {
		var object map[string]json.RawMessage
		if json.Unmarshal(raw, &object) == nil && object != nil {
			changed := false
			for name, index := range storedFields(v.Type()) {
				if child, ok := object[name]; ok {
					next, err := encodeStoredValue(child, storedFieldValue(v, index))
					if err != nil {
						return nil, err
					}
					if !bytes.Equal(next, child) {
						object[name], changed = next, true
					}
				}
			}
			if changed {
				raw = marshalStoredObject(raw, object)
			}
		}
	}
	if state := recordState(v); state != nil && state.stored != nil {
		return mergeStoredJSON(state.stored.raw, state.stored.baseline, raw, v.Type()), nil
	}
	return raw, nil
}

func mergeStoredJSON(original, baseline, current []byte, t reflect.Type) []byte {
	if len(original) > 0 && bytes.Equal(current, baseline) {
		return original
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if bytes.Equal(current, []byte("null")) {
		return current
	}
	if t == deliveryBodyType {
		oldType, newType := storedBodyWireType(original), storedBodyWireType(current)
		if oldType == nil || newType == nil || oldType != newType {
			return current
		}
		t = newType
	}
	if t.Kind() == reflect.Struct || t.Kind() == reflect.Map {
		var old, before, after map[string]json.RawMessage
		if json.Unmarshal(original, &old) != nil || json.Unmarshal(baseline, &before) != nil || json.Unmarshal(current, &after) != nil || after == nil {
			return current
		}
		fields := storedFields(t)
		merged := map[string]json.RawMessage{}
		if t.Kind() == reflect.Struct {
			for key, value := range old {
				if _, known := fields[key]; !known {
					merged[key] = value
				}
			}
		}
		for key, value := range after {
			childType := t
			if t.Kind() == reflect.Map {
				childType = t.Elem()
			} else if index, ok := fields[key]; ok {
				childType = storedFieldType(t, index)
			}
			merged[key] = mergeStoredJSON(old[key], before[key], value, childType)
		}
		return marshalStoredObject(current, merged)
	}
	if t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
		var old, before, after []json.RawMessage
		if json.Unmarshal(original, &old) != nil || json.Unmarshal(baseline, &before) != nil || json.Unmarshal(current, &after) != nil {
			return current
		}
		byID := map[string]int{}
		for i, item := range before {
			if id := storedStableID(item); id != "" {
				if _, duplicate := byID[id]; duplicate {
					return current
				}
				byID[id] = i
			}
		}
		for i, item := range after {
			j, found := byID[storedStableID(item)]
			if found && j < len(old) {
				after[i] = mergeStoredJSON(old[j], before[j], item, t.Elem())
			} else if len(before) == len(after) && i < len(old) && bytes.Equal(item, before[i]) {
				after[i] = old[i]
			}
		}
		out, _ := json.Marshal(after)
		return out
	}
	return current
}

// marshalStoredObject 保持当前编码器的字段顺序；替换嵌套原文不能改变冻结摘要。
func marshalStoredObject(order []byte, fields map[string]json.RawMessage) []byte {
	keys := make([]string, 0, len(fields))
	seen := map[string]bool{}
	decoder := json.NewDecoder(bytes.NewReader(order))
	if token, err := decoder.Token(); err == nil && token == json.Delim('{') {
		for decoder.More() {
			token, err = decoder.Token()
			if err != nil {
				break
			}
			key, ok := token.(string)
			if !ok {
				break
			}
			var skip json.RawMessage
			if decoder.Decode(&skip) != nil {
				break
			}
			if _, exists := fields[key]; exists && !seen[key] {
				keys = append(keys, key)
				seen[key] = true
			}
		}
	}
	var added []string
	for key := range fields {
		if !seen[key] {
			added = append(added, key)
		}
	}
	sort.Strings(added)
	keys = append(keys, added...)
	var out bytes.Buffer
	out.WriteByte('{')
	for i, key := range keys {
		if i > 0 {
			out.WriteByte(',')
		}
		name, _ := json.Marshal(key)
		out.Write(name)
		out.WriteByte(':')
		if len(fields[key]) == 0 {
			out.WriteString("null")
		} else {
			out.Write(fields[key])
		}
	}
	out.WriteByte('}')
	return out.Bytes()
}

func storedStableID(raw []byte) string {
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil {
		return ""
	}
	for _, key := range []string{"id", "event_id", "key", "file_id", "resource_id"} {
		var value string
		if json.Unmarshal(object[key], &value) == nil && value != "" {
			return key + ":" + value
		}
	}
	return ""
}
