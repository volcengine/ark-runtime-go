// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"encoding/json"
)

type Marshaller interface {
	Marshal(value interface{}) ([]byte, error)
}

type JSONMarshaller struct{}

func (jm *JSONMarshaller) Marshal(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}
