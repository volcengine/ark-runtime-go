// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Package json provides the ogen date-time codecs used by generated models
// without importing the ogen module (whose Go floor exceeds this SDK's).
package json

import (
	"time"

	"github.com/go-faster/jx"
)

// DecodeDateTime decodes an RFC3339 string, preserving fractional seconds and
// offsets accepted by time.Parse, matching ogen v1.20.3.
func DecodeDateTime(d *jx.Decoder) (time.Time, error) {
	s, err := d.Str()
	if err != nil {
		return time.Time{}, err
	}
	return time.Parse(time.RFC3339, s)
}

// EncodeDateTime uses RFC3339, intentionally omitting fractional seconds like
// ogen v1.20.3. Use of RFC3339Nano would change the generated wire format.
func EncodeDateTime(e *jx.Encoder, v time.Time) {
	var buf [64]byte
	e.ByteStr(v.AppendFormat(buf[:0], time.RFC3339))
}
