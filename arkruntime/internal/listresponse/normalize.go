// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Package listresponse adapts legacy null list responses before strict decoding.
package listresponse

import (
	"bytes"
	"encoding/json"
)

// Normalize changes only top-level data:null tokens, retaining duplicate fields
// and all other bytes so the generated decoder keeps its validation behavior.
func Normalize(data []byte, emptyPage string) []byte {
	if bytes.Equal(bytes.Trim(data, " \t\r\n"), []byte("null")) {
		return []byte(emptyPage)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	first, err := decoder.Token()
	if err != nil || first != json.Delim('{') {
		return data
	}
	var normalized []byte
	copied := 0
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return data
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return data
		}
		if key == "data" && bytes.Equal(value, []byte("null")) {
			end := int(decoder.InputOffset())
			start := end - len(value)
			normalized = append(normalized, data[copied:start]...)
			normalized = append(normalized, '[', ']')
			copied = end
		}
	}
	if _, err := decoder.Token(); err != nil || copied == 0 {
		return data
	}
	return append(normalized, data[copied:]...)
}
