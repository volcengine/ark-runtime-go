// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"math"
	"math/big"
	"testing"

	"github.com/volcengine/ark-runtime-go/arkruntime/internal/ogenregex"
)

func TestConstraintValues(t *testing.T) {
	tests := []struct {
		name    string
		check   func() error
		invalid bool
	}{
		{"unicode length", func() error { return (String{MaxLength: 2, MaxLengthSet: true}).Validate("中文") }, false},
		{"max string", func() error { return (String{MaxLength: 2, MaxLengthSet: true}).Validate("中文名") }, true},
		{"min string", func() error { return (String{MinLength: 1, MinLengthSet: true}).Validate("") }, true},
		{"array lower boundary", func() error { return (Array{MinLength: 1, MinLengthSet: true}).ValidateLength(1) }, false},
		{"empty array", func() error { return (Array{MinLength: 1, MinLengthSet: true}).ValidateLength(0) }, true},
		{"long array", func() error { return (Array{MaxLength: 2, MaxLengthSet: true}).ValidateLength(3) }, true},
		{"negative integer", func() error { return (Int{Min: 0, MinSet: true}).Validate(-1) }, true},
		{"integer boundary", func() error { return (Int{Min: 0, MinSet: true}).Validate(0) }, false},
		{"exclusive integer", func() error { return (Int{Max: 5, MaxSet: true, MaxExclusive: true}).Validate(5) }, true},
		{"int multiple", func() error { return (Int{MultipleOf: 3, MultipleOfSet: true}).Validate(-6) }, false},
		{"int not multiple", func() error { return (Int{MultipleOf: 3, MultipleOfSet: true}).Validate(4) }, true},
		{"float lower", func() error { return (Float{Min: -2, MinSet: true, Max: 2, MaxSet: true}).Validate(-2) }, false},
		{"float upper", func() error { return (Float{Min: -2, MinSet: true, Max: 2, MaxSet: true}).Validate(2) }, false},
		{"float too high", func() error { return (Float{Max: 2, MaxSet: true}).Validate(2.1) }, true},
		{"float too low", func() error { return (Float{Min: -2, MinSet: true}).Validate(-2.1) }, true},
		{"exclusive float", func() error { return (Float{Min: 0, MinSet: true, MinExclusive: true}).Validate(0) }, true},
		{"float multiple", func() error { return (Float{MultipleOf: big.NewRat(1, 2), MultipleOfSet: true}).Validate(1.5) }, false},
		{"float not multiple", func() error { return (Float{MultipleOf: big.NewRat(1, 2), MultipleOfSet: true}).Validate(1.25) }, true},
		{"nan", func() error { return (Float{Min: 0, MinSet: true}).Validate(math.NaN()) }, true},
		{"infinity", func() error { return (Float{}).Validate(math.Inf(1)) }, true},
		{"pattern", func() error {
			return (String{Regex: ogenregex.MustCompile("^clear_thinking")}).Validate("clear_thinking_20250901")
		}, false},
		{"pattern mismatch", func() error { return (String{Regex: ogenregex.MustCompile("^clear_thinking")}).Validate("other") }, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.check(); (err != nil) != tc.invalid {
				t.Fatalf("error=%v, want invalid=%v", err, tc.invalid)
			}
		})
	}
}
