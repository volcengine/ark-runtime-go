// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"encoding/json"
)

type Unmarshaler interface {
	Unmarshal(data []byte, v interface{}) error
}

type JSONUnmarshaler struct{}

func (jm *JSONUnmarshaler) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}
