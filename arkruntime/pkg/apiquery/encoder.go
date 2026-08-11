// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Package apiquery encodes request structs into URL query values, driven by
// explicit `query:"<name>"` struct tags (injected at codegen time via the Go
// OpenAPI variant). Optionality is detected via ogen's Opt* IsSet() convention
// (and nil pointers). It is the query counterpart to package apiform.
package apiquery

import (
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"strings"
)

const queryTag = "query"

type optionalSetter interface{ IsSet() bool }

// Marshal returns the `query`-tagged fields of v as url.Values. v must be a
// struct or pointer to one. Fields without a `query` tag are skipped.
func Marshal(v any) (url.Values, error) {
	out := url.Values{}
	rv := deref(reflect.ValueOf(v))
	if !rv.IsValid() {
		return out, nil
	}
	if rv.Kind() != reflect.Struct {
		return nil, fmt.Errorf("apiquery: Marshal expects a struct, got %s", rv.Kind())
	}
	if err := encodeStruct("", rv, out); err != nil {
		return nil, err
	}
	return out, nil
}

func encodeStruct(prefix string, rv reflect.Value, out url.Values) error {
	t := rv.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath != "" {
			continue
		}
		name, repeat := parseTag(field)
		if name == "" || name == "-" {
			continue
		}
		key := name
		if prefix != "" {
			key = prefix + "[" + name + "]"
		}
		if err := encodeValue(key, rv.Field(i), out, repeat); err != nil {
			return err
		}
	}
	return nil
}

// encodeValue serializes rv under key. repeat controls array encoding: when
// true, slices are emitted as repeated bare `key=v` pairs (e.g.
// `filter.task_ids=a&filter.task_ids=b`); when false (the default), slices are
// emitted with PHP-style brackets `key[]=v` (e.g. `include[]=a&include[]=b`).
func encodeValue(key string, rv reflect.Value, out url.Values, repeat bool) error {
	if rv.CanInterface() {
		if opt, ok := rv.Interface().(optionalSetter); ok {
			if !opt.IsSet() {
				return nil
			}
			if inner := rv.FieldByName("Value"); inner.IsValid() {
				return encodeValue(key, inner, out, repeat)
			}
		}
	}

	rv = deref(rv)
	if !rv.IsValid() {
		return nil
	}

	switch rv.Kind() {
	case reflect.Struct:
		return encodeStruct(key, rv, out)
	case reflect.Slice, reflect.Array:
		elemKey := key
		if !repeat {
			elemKey = key + "[]"
		}
		for i := 0; i < rv.Len(); i++ {
			if err := encodeValue(elemKey, rv.Index(i), out, repeat); err != nil {
				return err
			}
		}
		return nil
	default:
		s, err := scalar(rv)
		if err != nil {
			return err
		}
		out.Add(key, s)
		return nil
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
		return "", fmt.Errorf("apiquery: unsupported scalar kind %s", rv.Kind())
	}
}

// parseTag returns the query field name and whether the `,repeat` option is
// set. `,repeat` selects bare repeated array encoding (`k=a&k=b`) instead of
// the default bracketed form (`k[]=a&k[]=b`).
func parseTag(field reflect.StructField) (name string, repeat bool) {
	tag := field.Tag.Get(queryTag)
	if tag == "" {
		return "", false
	}
	parts := strings.Split(tag, ",")
	name = parts[0]
	for _, opt := range parts[1:] {
		if opt == "repeat" {
			repeat = true
		}
	}
	return name, repeat
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
