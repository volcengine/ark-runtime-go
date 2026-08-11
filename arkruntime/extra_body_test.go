// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package arkruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"strings"
	"testing"
)

func TestWithExtraBodyOverridesTypedFields(t *testing.T) {
	typedBody := struct {
		Model string `json:"model"`
		Seed  int64  `json:"seed"`
	}{
		Model: "typed-model",
		Seed:  math.MaxInt64,
	}

	client := NewClientWithApiKey("test-api-key")
	options := []requestOption{
		WithExtraBody(map[string]interface{}{
			"model":  "first-extra-model",
			"nested": map[string]interface{}{"enabled": true},
			"null":   nil,
		}),
		WithExtraBody(map[string]interface{}{
			"model": "last-extra-model",
		}),
		withBody(typedBody),
	}

	request, requestErr := client.newRequest(
		context.Background(),
		http.MethodPost,
		"https://example.com/api/v3/test",
		"",
		"",
		options...,
	)
	if requestErr != nil {
		t.Fatalf("newRequest() error = %v", requestErr)
	}
	defer request.Body.Close()

	bodyJSON, err := io.ReadAll(request.Body)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal(bodyJSON, &body); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	assertRawJSONEqual(t, body["model"], `"last-extra-model"`)
	assertRawJSONEqual(t, body["seed"], `9223372036854775807`)
	assertRawJSONEqual(t, body["nested"], `{"enabled":true}`)
	assertRawJSONEqual(t, body["null"], `null`)
}

func TestMergeExtraBodyRejectsUnsupportedBodies(t *testing.T) {
	tests := []struct {
		name    string
		body    interface{}
		wantErr string
	}{
		{name: "nil", body: nil, wantErr: "non-nil JSON object"},
		{name: "reader", body: bytes.NewBufferString(`{"prompt":"test"}`), wantErr: "pre-marshalled"},
		{name: "bytes", body: []byte(`{"prompt":"test"}`), wantErr: "pre-marshalled"},
		{name: "raw message", body: json.RawMessage(`{"prompt":"test"}`), wantErr: "pre-marshalled"},
		{name: "array", body: []string{"test"}, wantErr: "JSON object"},
		{name: "null", body: (*struct{})(nil), wantErr: "non-null JSON object"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := mergeExtraBody(test.body, map[string]interface{}{"extra": true})
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("mergeExtraBody() error = %v, want containing %q", err, test.wantErr)
			}
		})
	}
}

func TestMergeExtraBodyReportsMarshalErrors(t *testing.T) {
	t.Run("typed body", func(t *testing.T) {
		_, err := mergeExtraBody(
			struct {
				Invalid chan int `json:"invalid"`
			}{Invalid: make(chan int)},
			map[string]interface{}{"extra": true},
		)
		if err == nil || !strings.Contains(err.Error(), "marshal request body") {
			t.Fatalf("mergeExtraBody() error = %v", err)
		}
	})

	t.Run("extra field", func(t *testing.T) {
		_, err := mergeExtraBody(
			struct {
				Prompt string `json:"prompt"`
			}{Prompt: "test"},
			map[string]interface{}{"invalid": make(chan int)},
		)
		if err == nil || !strings.Contains(err.Error(), `marshal extra body field "invalid"`) {
			t.Fatalf("mergeExtraBody() error = %v", err)
		}
	})
}

func TestMergeExtraBodyEmptyIsNoOp(t *testing.T) {
	body := &struct {
		Prompt string `json:"prompt"`
	}{Prompt: "test"}

	merged, err := mergeExtraBody(body, nil)
	if err != nil {
		t.Fatalf("mergeExtraBody() error = %v", err)
	}
	if merged != body {
		t.Fatal("mergeExtraBody() should preserve the original body when no extra fields are set")
	}
}

func assertRawJSONEqual(t *testing.T, got json.RawMessage, want string) {
	t.Helper()
	if !bytes.Equal(got, []byte(want)) {
		t.Fatalf("raw JSON = %s, want %s", got, want)
	}
}
