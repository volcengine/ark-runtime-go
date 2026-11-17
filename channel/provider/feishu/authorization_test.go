// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/volcengine/ark-runtime-go/arkruntime"

	"github.com/volcengine/ark-runtime-go/channel"
	"github.com/volcengine/ark-runtime-go/channel/ma"
	"github.com/volcengine/ark-runtime-go/channel/provider/feishu"
	"github.com/volcengine/ark-runtime-go/channel/store"
	"github.com/volcengine/ark-runtime-go/channel/store/embedded"
)

func TestUserAuthorizationChecksEnvironmentAndApplicationScopes(t *testing.T) {
	for _, tc := range []struct {
		name, environment string
		scopes            []string
		wantError         bool
		plaintext         bool
	}{{"cloud with available scope", "cloud", []string{"read"}, false, false}, {"self hosted cannot inject user identity", "self_hosted", []string{"read"}, true, false}, {"tenant scope is not a user scope", "cloud", []string{"tenant_only"}, true, false}, {"plaintext store cannot persist authorization prompts", "cloud", []string{"read"}, true, true}} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body string
				switch r.URL.Path {
				case "/api/v3/environments/env":
					body = `{"id":"env","type":"environment","name":"fixture","created_at":"2026-10-13T00:00:00Z","updated_at":"2026-10-13T00:00:00Z","config":{"type":"` + tc.environment + `","env":{}}}`
				case "/open-apis/auth/v3/tenant_access_token/internal":
					body = `{"code":0,"tenant_access_token":"fixture-token"}`
				case "/open-apis/application/v6/applications/app":
					body = `{"code":0,"data":{"app":{"app_id":"app","scopes":[{"scope":"read","token_types":["user"]},{"scope":"tenant_only","token_types":["tenant"]}]}}}`
				default:
					t.Errorf("unexpected request: %s", r.URL.Path)
					w.WriteHeader(404)
					return
				}
				if _, err := io.WriteString(w, body); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			codec, err := store.NewAESGCM("fixture", []byte(strings.Repeat("k", 32)))
			if err != nil {
				t.Fatal(err)
			}
			options := embedded.Options{PayloadCodec: codec, PayloadNamespace: "fixture", RequirePayloadEncryption: true}
			if tc.plaintext {
				options = embedded.Options{}
			}
			path := filepath.Join(t.TempDir(), "channel.db")
			if err := embedded.Install(path, options); err != nil {
				t.Fatal(err)
			}
			st, err := embedded.Open(path, options)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := st.Close(); err != nil {
					t.Error(err)
				}
			}()
			if err := store.ExpandUserAuth(context.Background(), st); err != nil {
				t.Fatal(err)
			}
			agent, err := ma.New(arkruntime.NewClientWithApiKey("fixture", arkruntime.WithBaseUrl(server.URL+"/api/v3"), arkruntime.WithHTTPClient(server.Client())), ma.Options{})
			if err != nil {
				t.Fatal(err)
			}
			b := channel.Binding{ID: "local", AppID: "app", AgentID: "agent", AgentVersion: "v1", EnvironmentID: "env", Identity: channel.IdentityPolicy{P2P: "user"}, UserAuthScopes: tc.scopes}
			p, err := feishu.NewWithContext(context.Background(), feishu.Config{Binding: b, AppSecretRef: "env:APP", SecretResolver: resolver{}, HTTPClient: server.Client(), OpenBaseURL: server.URL, UserAuthorization: &feishu.UserAuthorization{Namespace: "customer-deployment", Store: st, MA: agent, Codec: codec}})
			if (err != nil) != tc.wantError {
				t.Fatalf("unexpected initialization result: %v", err)
			}
			if !tc.wantError {
				if _, err := channel.NewRuntime(st, []channel.Connection{{Binding: b, MA: agent, Provider: p}}, channel.Options{}); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
