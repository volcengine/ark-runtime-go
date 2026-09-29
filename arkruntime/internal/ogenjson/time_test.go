// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package json

import (
	"testing"
	"time"

	"github.com/go-faster/jx"
)

func TestDateTimeWireFormat(t *testing.T) {
	for _, tc := range []struct{ input, output string }{
		{`"2026-09-28T10:20:30Z"`, `"2026-09-28T10:20:30Z"`},
		{`"2026-09-28T10:20:30.123456789+08:00"`, `"2026-09-28T10:20:30+08:00"`},
		{`"2026-09-28T10:20:30-05:30"`, `"2026-09-28T10:20:30-05:30"`},
		{`"0001-01-01T00:00:00Z"`, `"0001-01-01T00:00:00Z"`},
	} {
		t.Run(tc.input, func(t *testing.T) {
			got, err := DecodeDateTime(jx.DecodeStr(tc.input))
			if err != nil {
				t.Fatal(err)
			}
			want, err := time.Parse(time.RFC3339, tc.input[1:len(tc.input)-1])
			if err != nil || !got.Equal(want) || got.Nanosecond() != want.Nanosecond() {
				t.Fatalf("decoded %v, want %v: %v", got, want, err)
			}
			e := jx.GetEncoder()
			defer jx.PutEncoder(e)
			EncodeDateTime(e, got)
			if e.String() != tc.output {
				t.Fatalf("encoded %s, want %s", e.String(), tc.output)
			}
		})
	}
}

func TestDateTimeRejectsInvalidValues(t *testing.T) {
	for _, input := range []string{`null`, `123`, `{}`, `[]`, `""`, `"2026-09-28"`, `"2026-02-30T10:20:30Z"`, `"2026-09-28T10:20:30"`, `"unterminated`} {
		t.Run(input, func(t *testing.T) {
			if _, err := DecodeDateTime(jx.DecodeStr(input)); err == nil {
				t.Fatal("expected decode error")
			}
		})
	}
}
