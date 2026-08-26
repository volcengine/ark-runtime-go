// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package arkruntime

import "testing"

func TestNewClientConfigBaseURL(t *testing.T) {
	tests := []struct {
		name    string
		options []ConfigOption
		want    string
	}{
		{
			name: "default",
			want: "https://ark.cn-beijing.volces.com/api/v3",
		},
		{
			name:    "override",
			options: []ConfigOption{WithBaseUrl("https://example.com/api/v3/")},
			want:    "https://example.com/api/v3",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := NewClientConfig("test-api-key", "", "", test.options...)
			if config.BaseURL != test.want {
				t.Fatalf("BaseURL = %q, want %q", config.BaseURL, test.want)
			}
		})
	}
}
