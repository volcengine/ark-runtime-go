// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"errors"
	"math"
	"testing"
)

func TestErrorString(t *testing.T) {
	e := &Error{Fields: []FieldError{
		{Name: "messages", Error: ErrFieldRequired},
		{Name: "model", Error: ErrFieldRequired},
	}}
	got := e.Error()
	want := "invalid: messages (field required), model (field required)"
	if got != want {
		t.Fatalf("Error() mismatch with ogen's format\n got: %q\nwant: %q", got, want)
	}
}

func TestErrorStringEmpty(t *testing.T) {
	if got := (&Error{}).Error(); got != "invalid:" {
		t.Fatalf("empty Error() = %q, want %q", got, "invalid:")
	}
}

func TestFloatValidate(t *testing.T) {
	v := Float{}
	if err := v.Validate(1.5); err != nil {
		t.Fatalf("finite value should pass: %v", err)
	}
	if err := v.Validate(math.NaN()); err == nil {
		t.Fatal("NaN should fail")
	}
	if err := v.Validate(math.Inf(1)); err == nil {
		t.Fatal("+Inf should fail")
	}
	if err := v.Validate(math.Inf(-1)); err == nil {
		t.Fatal("-Inf should fail")
	}
}

func TestErrFieldRequiredSentinelMessage(t *testing.T) {
	if ErrFieldRequired.Error() != "field required" {
		t.Fatalf("ErrFieldRequired text drift; downstream regex may break: %q", ErrFieldRequired.Error())
	}
	if !errors.Is(ErrFieldRequired, ErrFieldRequired) {
		t.Fatal("errors.Is sanity check failed")
	}
}
