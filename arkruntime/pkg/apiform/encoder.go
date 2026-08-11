// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Package apiform encodes request structs into multipart/form bodies, driven by
// explicit `form:"<name>"` struct tags (injected at codegen time via the Go
// OpenAPI variant). This mirrors the legacy volcengine-go SDK's tag-driven
// encoder; optionality is detected via ogen's Opt* IsSet() convention (and nil
// pointers). Nested structs produce bracket notation, e.g. `tos[bucket]`.
package apiform

import (
	"fmt"
	"mime/multipart"
	"reflect"
	"strconv"
	"strings"
)

const formTag = "form"

// optionalSetter is satisfied by ogen's Opt* wrappers.
type optionalSetter interface{ IsSet() bool }

// Marshal writes the `form`-tagged fields of v to w. v must be a struct or a
// pointer to one. Fields without a `form` tag are skipped.
func Marshal(v any, w *multipart.Writer) error {
	rv := deref(reflect.ValueOf(v))
	if !rv.IsValid() {
		return nil
	}
	if rv.Kind() != reflect.Struct {
		return fmt.Errorf("apiform: Marshal expects a struct, got %s", rv.Kind())
	}
	return encodeStruct("", rv, w)
}

func encodeStruct(prefix string, rv reflect.Value, w *multipart.Writer) error {
	t := rv.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath != "" { // unexported
			continue
		}
		name := tagName(field)
		if name == "" || name == "-" {
			continue
		}
		key := name
		if prefix != "" {
			key = prefix + "[" + name + "]"
		}
		if err := encodeValue(key, rv.Field(i), w); err != nil {
			return err
		}
	}
	return nil
}

func encodeValue(key string, rv reflect.Value, w *multipart.Writer) error {
	// ogen Opt* wrapper: omit when unset, otherwise encode its Value.
	if rv.CanInterface() {
		if opt, ok := rv.Interface().(optionalSetter); ok {
			if !opt.IsSet() {
				return nil
			}
			if inner := rv.FieldByName("Value"); inner.IsValid() {
				return encodeValue(key, inner, w)
			}
		}
	}

	rv = deref(rv)
	if !rv.IsValid() {
		return nil
	}

	switch rv.Kind() {
	case reflect.Struct:
		return encodeStruct(key, rv, w)
	case reflect.Slice, reflect.Array:
		for i := 0; i < rv.Len(); i++ {
			if err := encodeValue(key+"[]", rv.Index(i), w); err != nil {
				return err
			}
		}
		return nil
	default:
		s, err := scalar(rv)
		if err != nil {
			return err
		}
		return w.WriteField(key, s)
	}
}

func scalar(rv reflect.Value) (string, error) {
	switch rv.Kind() {
	case reflect.String:
		return rv.String(), nil
	case reflect.Bool:
		if rv.Bool() {
			return "true", nil
		}
		return "false", nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(rv.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(rv.Uint(), 10), nil
	case reflect.Float32:
		return strconv.FormatFloat(rv.Float(), 'f', -1, 32), nil
	case reflect.Float64:
		return strconv.FormatFloat(rv.Float(), 'f', -1, 64), nil
	default:
		return "", fmt.Errorf("apiform: unsupported scalar kind %s", rv.Kind())
	}
}

func tagName(field reflect.StructField) string {
	tag := field.Tag.Get(formTag)
	if tag == "" {
		return ""
	}
	return strings.Split(tag, ",")[0]
}

func deref(rv reflect.Value) reflect.Value {
	for rv.Kind() == reflect.Ptr || rv.Kind() == reflect.Interface {
		if rv.IsNil() {
			return reflect.Value{}
		}
		rv = rv.Elem()
	}
	return rv
}
