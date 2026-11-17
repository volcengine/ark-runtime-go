// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package runner

import (
	"encoding/json"
	"io"
	"net/http"
)

func quote(value string) string {
	data, _ := json.Marshal(value)
	return string(data)
}

func decodeJSON(resp *http.Response, value any) error {
	return json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(value)
}
