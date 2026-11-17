// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package oauth

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"
)

// validJSON 在 map 解码前拒绝损坏的 UTF-8 和任意层级的重复对象成员。
// 保留未知字段，并使用 Number 避免无关扩展数值被 float64 范围限制。
func validJSON(raw []byte) bool {
	if !utf8.Valid(raw) {
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if !readJSONValue(decoder) {
		return false
	}
	_, err := decoder.Token()
	return errors.Is(err, io.EOF)
}

func readJSONValue(decoder *json.Decoder) bool {
	token, err := decoder.Token()
	if err != nil {
		return false
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return true
	}
	switch delimiter {
	case '{':
		members := make(map[string]struct{})
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return false
			}
			name, ok := key.(string)
			if !ok {
				return false
			}
			if _, duplicate := members[name]; duplicate {
				return false
			}
			members[name] = struct{}{}
			if !readJSONValue(decoder) {
				return false
			}
		}
		closing, err := decoder.Token()
		return err == nil && closing == json.Delim('}')
	case '[':
		for decoder.More() {
			if !readJSONValue(decoder) {
				return false
			}
		}
		closing, err := decoder.Token()
		return err == nil && closing == json.Delim(']')
	default:
		return false
	}
}
