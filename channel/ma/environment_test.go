// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package ma_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/arkruntime"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/ma"
)

// TestEnvironmentConfigType 验证公开 Environment 的资源类型与运行配置类型分开读取。
func TestEnvironmentConfigType(t *testing.T) {
	for _, configType := range []string{"cloud", "self_hosted"} {
		t.Run(configType, func(t *testing.T) {
			configJSON := `{"type":"` + configType + `"}`
			var expectedEnv map[string]string
			if configType == "cloud" {
				configJSON = `{"type":"cloud","env":{"CUSTOMER":"original"}}`
				expectedEnv = map[string]string{"CUSTOMER": "original"}
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "/api/v3/environments/env-1", r.URL.Path)
				_, _ = io.WriteString(w, `{"id":"env-1","type":"environment","name":"fixture","config":`+configJSON+`,"created_at":"2026-10-19T00:00:00Z","updated_at":"2026-10-19T00:00:00Z"}`)
			}))
			defer server.Close()
			client, err := ma.NewClient(server.URL, "key", ma.ClientOptions{})
			require.NoError(t, err)
			env, err := client.GetEnvironment(context.Background(), "env-1")
			require.NoError(t, err)
			require.Equal(t, configType, env.Type)
			require.Equal(t, expectedEnv, env.Env)
		})
	}
}

// TestEnvironmentWithOverridesWire 使用 owning IDL 的 cloud 配置，保证只发送 env 覆写。
func TestEnvironmentWithOverridesWire(t *testing.T) {
	for _, auth := range []string{"static", "injected SDK"} {
		t.Run(auth, func(t *testing.T) {
			var captured map[string]any
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "Bearer key", r.Header.Get("Authorization"))
				switch r.URL.Path {
				case "/api/v3/environments/env-1":
					_, _ = io.WriteString(w, `{"id":"env-1","type":"environment","name":"fixture","config":{"type":"cloud","env":{"CUSTOMER_TOKEN":"c","LARKSUITE_CLI_STRICT_MODE":"user"},"image_template":"restricted-image","networking":{"type":"unrestricted"},"packages":{"type":"packages","pip":["example==1.0.0"]},"setup_script":"echo customer","tos":{"bucket":"customer-bucket","prefix":"outputs/"}},"created_at":"2026-10-19T00:00:00Z","updated_at":"2026-10-19T00:00:00Z"}`)
				case "/api/v3/sessions":
					require.NoError(t, json.NewDecoder(r.Body).Decode(&captured))
					_, _ = io.WriteString(w, `{"id":"s1","type":"session","agent":{"type":"agent","id":"a1","version":1},"status":"idle","environment_id":"env","created_at":"2026-10-13T00:00:00Z","updated_at":"2026-10-13T00:00:00Z"}`)
				default:
					t.Errorf("unexpected request %s", r.URL.Path)
				}
			}))
			defer server.Close()
			options := ma.ClientOptions{}
			key := "key"
			if auth == "injected SDK" {
				options.SDKClient = arkruntime.NewClientWithApiKey(key, arkruntime.WithBaseUrl(server.URL+"/api/v3"))
			}

			client, err := ma.NewClient(server.URL, key, options)
			require.NoError(t, err)
			base, err := client.GetEnvironment(context.Background(), "env-1")
			require.NoError(t, err)
			merged := map[string]string{"CUSTOMER_TOKEN": base.Env["CUSTOMER_TOKEN"], core.EnvLarkSuiteAppID: "app-1", core.EnvLarkSuiteStrictMode: "bot"}
			_, err = client.CreateSession(context.Background(), core.CreateSessionSpec{
				Binding: core.Binding{AgentID: "a1", AgentVersion: "v1", EnvironmentID: "env-1"}, CreationKey: "ck", LookupKey: "lk",
				EnvironmentOverride: &core.EnvironmentOverride{Type: base.Type, Env: merged},
			})
			require.NoError(t, err)
			require.NotContains(t, captured, "environment_id")
			require.NotContains(t, captured, "expected_configuration_version")
			require.Equal(t, map[string]any{
				"type": "environment_with_overrides", "id": "env-1", "config": map[string]any{
					"type": "cloud", "env": map[string]any{"CUSTOMER_TOKEN": "c", core.EnvLarkSuiteStrictMode: "bot", core.EnvLarkSuiteAppID: "app-1"},
				},
			}, captured["environment"], "完整形状断言同时禁止 environment.environment 与 image/networking/packages/setup/tos 字段")
		})
	}
}

// TestEnvironmentCreate412PreservesAPIError 验证 412 沿用普通公开 API 错误，不添加已撤销的版本冲突分类。
func TestEnvironmentCreate412PreservesAPIError(t *testing.T) {
	count := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		w.WriteHeader(http.StatusPreconditionFailed)
		_, _ = io.WriteString(w, `{"error":{"code":"PreconditionFailed.Other","request_id":"request-1"}}`)
	}))
	defer server.Close()
	client, err := ma.NewClient(server.URL, "key", ma.ClientOptions{})
	require.NoError(t, err)
	_, err = client.CreateSession(context.Background(), core.CreateSessionSpec{Binding: core.Binding{AgentID: "a1", AgentVersion: "v1", EnvironmentID: "env-1"}, CreationKey: "ck", LookupKey: "lk"})
	var apiErr *ma.APIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, http.StatusPreconditionFailed, apiErr.Status)
	require.Equal(t, "PreconditionFailed.Other", apiErr.Code)
	require.Equal(t, 1, count)
}

// TestEnvironmentInvalidFrozenOverrideNeverSends 拒绝旧快照中的未知配置类型，禁止猜测修改为 cloud。
func TestEnvironmentInvalidFrozenOverrideNeverSends(t *testing.T) {
	for _, tc := range []struct{ name, id, kind string }{
		{"resource_type", "env-1", "environment"}, {"old_fixture", "env-1", "ark_sandbox"}, {"missing_id", "", "cloud"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(500) }))
			defer server.Close()
			client, err := ma.NewClient(server.URL, "key", ma.ClientOptions{})
			require.NoError(t, err)
			_, err = client.CreateSession(context.Background(), core.CreateSessionSpec{CreationKey: "ck", LookupKey: "lk", Binding: core.Binding{AgentID: "a1", AgentVersion: "v1", EnvironmentID: tc.id}, EnvironmentOverride: &core.EnvironmentOverride{Type: tc.kind, Env: map[string]string{"CUSTOM": "frozen"}}})
			var apiErr *ma.APIError
			require.ErrorAs(t, err, &apiErr)
			require.Equal(t, 400, apiErr.Status)
			require.Equal(t, "InvalidEnvironmentConfig", apiErr.Code)
			require.Zero(t, calls)
		})
	}
}
