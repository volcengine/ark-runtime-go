// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package ogenregex

import "testing"

func TestECMAPatterns(t *testing.T) {
	for _, tc := range []struct {
		pattern, value string
		match          bool
	}{
		{"^(clear_thinking|clear_tool_uses)", "clear_thinking_20250901", true},
		{"^(clear_thinking|clear_tool_uses)", "unrelated", false},
		{"^(?!examples/)", "examples/main.go", false},
		{"^(?!examples/)", "src/main.go", true},
		{"^.$", "\u2028", false},
		{"^\\s$", "\u00a0", true},
		{"^.$", "中", true},
	} {
		t.Run(tc.pattern+tc.value, func(t *testing.T) {
			r, err := Compile(tc.pattern)
			if err != nil {
				t.Fatal(err)
			}
			if r.String() != tc.pattern {
				t.Fatal("pattern changed")
			}
			match, err := r.MatchString(tc.value)
			if err != nil || match != tc.match {
				t.Fatalf("string match=%v error=%v", match, err)
			}
			match, err = r.Match([]byte(tc.value))
			if err != nil || match != tc.match {
				t.Fatalf("byte match=%v error=%v", match, err)
			}
		})
	}
}

func TestInvalidPattern(t *testing.T) {
	if _, err := Compile("("); err == nil {
		t.Fatal("invalid regex accepted")
	}
	defer func() {
		if recover() == nil {
			t.Error("MustCompile did not panic")
		}
	}()
	MustCompile("(")
}
