// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package oauth

import (
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOAuthRejectsHTTPServiceURLs(t *testing.T) {
	for _, config := range []Config{
		{AppID: "app", AppSecret: "secret", AccountsBaseURL: "http://accounts.example.test"},
		{AppID: "app", AppSecret: "secret", OpenBaseURL: "http://open.example.test"},
	} {
		_, err := New(config)
		require.Error(t, err)
	}
}

func TestBeginRejectsHTTPVerificationURL(t *testing.T) {
	for _, field := range []string{"verification_uri", "verification_uri_complete"} {
		t.Run(field, func(t *testing.T) {
			client, _ := fixture(t, func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, `{"device_code":"d","user_code":"u","`+field+`":"http://example.test/device?user_code=u","expires_in":600,"interval":5}`)
			})
			_, err := client.Begin(context.Background(), nil)
			require.Error(t, err)
		})
	}
}
