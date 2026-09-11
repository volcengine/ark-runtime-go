// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Package validate is a drop-in shim for the subset of
// github.com/ogen-go/ogen/validate that our ogen-generated model code
// references. Keeping this package internal lets us drop the ogen runtime
// dependency from go.mod, which in turn lowers the Go floor consumers of the
// SDK must satisfy.
//
// The Makefile's vendor-<api> step rewrites ogen's import path to point here
// during regeneration. If a future ogen release introduces a new symbol in
// its validate package that the codegen emits, the build will fail with an
// "undefined: validate.X" error and that symbol must be mirrored here.
//
// Error string formats match ogen's so any downstream code that inspects
// .Error() text continues to work.
package validate

import (
	"strings"

	"github.com/go-faster/errors"
)

// ErrFieldRequired reports that a field is required, but not found.
var ErrFieldRequired = errors.New("field required")

// ErrNilPointer reports that Validate was called on a nil pointer receiver.
var ErrNilPointer = errors.New("nil pointer")

// FieldError is a single failed validation on a struct field.
type FieldError struct {
	Name  string
	Error error
}

// Error is a collection of FieldErrors returned when one or more struct
// fields fail validation.
type Error struct {
	Fields []FieldError
}

// Error implements the error interface. The format matches ogen's:
//
//	invalid: field1 (reason), field2 (reason)
func (e *Error) Error() string {
	var b strings.Builder
	b.WriteString("invalid:")
	for i, f := range e.Fields {
		if i != 0 {
			b.WriteRune(',')
		}
		b.WriteRune(' ')
		b.WriteString(f.Name)
		b.WriteString(" (")
		b.WriteString(f.Error.Error())
		b.WriteString(")")
	}
	return b.String()
}
