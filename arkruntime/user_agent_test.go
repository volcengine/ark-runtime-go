// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package arkruntime_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/images"
)

func TestRequestUserAgent(t *testing.T) {
	version, err := os.ReadFile("../VERSION")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name     string
		custom   bool
		value    string
		expected string
	}{
		{name: "repository version", expected: "ark-runtime-go/" + strings.TrimSpace(string(version))},
		{name: "custom override", custom: true, value: "my-app/2.0", expected: "my-app/2.0"},
		{name: "explicitly disabled", custom: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			headers := make(chan http.Header, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				headers <- r.Header.Clone()
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"model":"test-model","created":1,"data":[]}`))
			}))
			defer server.Close()
			client := arkruntime.NewClientWithApiKey("test-key",
				arkruntime.WithBaseUrl(server.URL), arkruntime.WithRetryTimes(0))
			request := &images.CreateImageGenerationRequest{Model: "test-model", Prompt: "test"}
			if test.custom {
				_, err = client.GenerateImages(context.Background(), request,
					arkruntime.WithCustomHeader("User-Agent", test.value))
			} else {
				_, err = client.GenerateImages(context.Background(), request)
			}
			if err != nil {
				t.Fatal(err)
			}
			got := <-headers
			if got.Get("User-Agent") != test.expected {
				t.Errorf("User-Agent = %q, want %q", got.Get("User-Agent"), test.expected)
			}
			if got.Get("X-Client-Request-Id") == "" {
				t.Error("missing client request ID")
			}
			if got.Get("Authorization") != "Bearer test-key" {
				t.Error("authentication header changed")
			}
		})
	}
}
