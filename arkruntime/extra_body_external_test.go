// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package arkruntime_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/images"
)

func TestWithExtraBodyIsUsableByExternalCallers(t *testing.T) {
	requestBodies := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			return
		}
		requestBodies <- body
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"model":"response-model","created":1}`))
	}))
	defer server.Close()

	client := arkruntime.NewClientWithApiKey(
		"test-api-key",
		arkruntime.WithBaseUrl(server.URL),
		arkruntime.WithRetryTimes(0),
	)
	request := &images.CreateImageGenerationRequest{
		Model:  "typed-model",
		Prompt: "test prompt",
	}

	_, err := client.GenerateImages(
		context.Background(),
		request,
		arkruntime.WithExtraBody(map[string]interface{}{
			"model":           "extra-model",
			"preview_feature": true,
		}),
	)
	if err != nil {
		t.Fatalf("GenerateImages() error = %v", err)
	}

	var body map[string]json.RawMessage
	if err := json.Unmarshal(<-requestBodies, &body); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if got := string(body["model"]); got != `"extra-model"` {
		t.Fatalf("model = %s, want %q", got, "extra-model")
	}
	if got := string(body["preview_feature"]); got != "true" {
		t.Fatalf("preview_feature = %s, want true", got)
	}
}
