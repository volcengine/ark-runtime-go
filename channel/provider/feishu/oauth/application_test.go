// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package oauth

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestApplicationUserScopes(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/open-apis/auth/v3/tenant_access_token/internal" {
			require.Equal(t, "POST", r.Method)
			var body map[string]string
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Equal(t, map[string]string{"app_id": "cli_app", "app_secret": "secret"}, body)
			require.Equal(t, "application/json", r.Header.Get("Content-Type"))
			_, _ = fmt.Fprint(w, `{"code":0,"tenant_access_token":"tenant-secret"}`)
			return
		}
		require.Equal(t, "/open-apis/application/v6/applications/cli_app", r.URL.Path)
		require.Equal(t, "zh_cn", r.URL.Query().Get("lang"))
		require.Equal(t, "Bearer tenant-secret", r.Header.Get("Authorization"))
		_, _ = fmt.Fprint(w, `{"code":0,"data":{"app":{"app_id":"cli_app","scopes":[{"scope":"z","token_types":["user","tenant"]},{"scope":"a","token_types":["user"]},{"scope":"a","token_types":["user"]},{"scope":"tenant","token_types":["tenant"]}]}}}`)
	}))
	defer server.Close()
	c, err := New(Config{AppID: "cli_app", AppSecret: "secret", OpenBaseURL: server.URL, HTTPClient: server.Client()})
	require.NoError(t, err)
	scopes, err := c.ApplicationUserScopes(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"a", "z"}, scopes)
}
func TestBeginRejectsMoreThan200Scopes(t *testing.T) {
	c, err := New(Config{AppID: "cli_app", AppSecret: "secret"})
	require.NoError(t, err)
	_, err = c.Begin(context.Background(), make([]string, 201))
	require.Error(t, err)
}

func TestApplicationUserScopesRejectsUnprovenResponse(t *testing.T) {
	for name, body := range map[string]string{
		"missing_code":    `{"data":{"app":{"app_id":"cli_app","scopes":[]}}}`,
		"wrong_app":       `{"code":0,"data":{"app":{"app_id":"other","scopes":[]}}}`,
		"missing_scopes":  `{"code":0,"data":{"app":{"app_id":"cli_app"}}}`,
		"null_scopes":     `{"code":0,"data":{"app":{"app_id":"cli_app","scopes":null}}}`,
		"bad_type":        `{"code":0,"data":{"app":{"app_id":"cli_app","scopes":[{"scope":"a","token_types":["unknown"]}]}}}`,
		"missing_types":   `{"code":0,"data":{"app":{"app_id":"cli_app","scopes":[{"scope":"a"}]}}}`,
		"upstream_secret": `{"code":123,"msg":"secret-must-not-leak"}`,
		"oversized":       strings.Repeat("x", maxResponseBytes+1),
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					_, _ = fmt.Fprint(w, `{"code":0,"tenant_access_token":"tenant-secret"}`)
					return
				}
				_, _ = fmt.Fprint(w, body)
			}))
			defer server.Close()
			c, err := New(Config{AppID: "cli_app", AppSecret: "secret", OpenBaseURL: server.URL, HTTPClient: server.Client()})
			require.NoError(t, err)
			_, err = c.ApplicationUserScopes(context.Background())
			require.Error(t, err)
			require.NotContains(t, err.Error(), "secret")
		})
	}
}
func TestBeginScopeLimit(t *testing.T) {
	for _, count := range []int{200, 201, 401} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			calls := 0
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				_, _ = fmt.Fprint(w, `{"device_code":"d","user_code":"u","verification_uri":"https://example.com","expires_in":600,"interval":5}`)
			}))
			defer server.Close()
			c, err := New(Config{AppID: "cli_app", AppSecret: "secret", AccountsBaseURL: server.URL, HTTPClient: server.Client()})
			require.NoError(t, err)
			_, err = c.Begin(context.Background(), make([]string, count))
			if count == 200 {
				require.NoError(t, err)
				require.Equal(t, 1, calls)
			} else {
				require.Error(t, err)
				require.Zero(t, calls)
			}
		})
	}
}
