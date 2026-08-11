// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package arkruntime

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// WithExtraBody adds top-level fields to a JSON request body. Extra fields
// override fields with the same name in the typed request body. Multiple calls
// are merged in order, with values from later calls taking precedence.
//
// WithExtraBody is supported only for requests whose typed body serializes to a
// JSON object. It returns a request error for bodyless, multipart,
// pre-marshalled, or non-object requests.
func WithExtraBody(fields map[string]interface{}) requestOption {
	return func(args *requestOptions) {
		if args.extraBody == nil {
			args.extraBody = make(map[string]interface{}, len(fields))
		}
		for key, value := range fields {
			args.extraBody[key] = value
		}
	}
}

func mergeExtraBody(body interface{}, extra map[string]interface{}) (interface{}, error) {
	if len(extra) == 0 {
		return body, nil
	}
	if body == nil {
		return nil, errors.New("extra body requires a non-nil JSON object request body")
	}
	if isPreMarshalledBody(body) {
		return nil, fmt.Errorf("extra body is not supported for pre-marshalled request body %T", body)
	}

	bodyJSON, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal request body for extra body merge: %w", err)
	}

	fields := make(map[string]json.RawMessage)
	if err := json.Unmarshal(bodyJSON, &fields); err != nil {
		return nil, fmt.Errorf("extra body requires a JSON object request body: %w", err)
	}
	if fields == nil {
		return nil, errors.New("extra body requires a non-null JSON object request body")
	}

	for key, value := range extra {
		valueJSON, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("marshal extra body field %q: %w", key, err)
		}
		fields[key] = valueJSON
	}

	merged, err := json.Marshal(fields)
	if err != nil {
		return nil, fmt.Errorf("marshal merged request body: %w", err)
	}
	return merged, nil
}

func isPreMarshalledBody(body interface{}) bool {
	switch body.(type) {
	case io.Reader, []byte, json.RawMessage:
		return true
	default:
		return false
	}
}
