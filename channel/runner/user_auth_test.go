// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package runner

import (
	"context"
	"database/sql"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.etcd.io/bbolt"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/provider/feishu/oauth"
	"github.com/volcengine/ark-runtime-go/channel/secret"

	"github.com/stretchr/testify/require"
)

func userConfigJSON() string {
	return strings.Replace(strings.Replace(validJSON(), `"database_path"`, `"user_auth":{"owner":"configured-owner","project":"configured-project","encryption_key_ref":"env:AUTH_KEY"},"database_path"`, 1), `"identity":"none"`, `"identity_v2":{"p2p":"user"},"user_auth":{"channel_id":"channel-a","epoch":1,"scopes":["read"]}`, 1)
}
func TestUserAuthConfigRequiresExplicitScopeIdentityAndEncryption(t *testing.T) {
	cfg, err := Load(writeConfig(t, userConfigJSON()))
	require.NoError(t, err)
	require.Equal(t, core.IdentityUser, cfg.Connections[0].resolvedIdentity.P2P)
	for _, bad := range []string{strings.Replace(userConfigJSON(), `"epoch":1`, `"epoch":0`, 1), strings.Replace(userConfigJSON(), `env:AUTH_KEY`, `raw-secret`, 1), strings.Replace(userConfigJSON(), `"owner":"configured-owner"`, `"owner":""`, 1)} {
		_, err = Load(writeConfig(t, bad))
		require.Error(t, err)
	}
}
func TestUserAuthScopesOptionalAndNamespaceRejected(t *testing.T) {
	_, err := Load(writeConfig(t, strings.Replace(userConfigJSON(), `"scopes":["read"]`, `"scopes":[]`, 1)))
	require.NoError(t, err)
	_, err = Load(writeConfig(t, strings.Replace(userConfigJSON(), `"owner":"configured-owner"`, `"namespace":"legacy","owner":"configured-owner"`, 1)))
	require.Error(t, err)
}
func TestUserAuthEncryptedStorageInstallAndReadonlyReopen(t *testing.T) {
	for _, driver := range []string{"embedded", "sqlite"} {
		t.Run(driver, func(t *testing.T) {
			cfg, err := Load(writeConfig(t, userConfigJSON()))
			require.NoError(t, err)
			cfg.Store.Driver = driver
			resolver := staticResolver{"env:AUTH_KEY": base64.StdEncoding.EncodeToString([]byte(strings.Repeat("k", 32)))}
			require.NoError(t, InstallStorageWith(context.Background(), cfg, resolver))
			s, err := openStore(context.Background(), cfg, resolver, modeRun)
			require.NoError(t, err)
			b := core.Binding{ID: "primary", Owner: "configured-owner", Project: "configured-project"}
			m := core.Message{BindingID: b.ID, ProviderEventID: "event", MessageID: "message", TenantKey: "tenant", ChatID: "chat", ChatType: "p2p", Text: "original-input-secret", ReceivedAt: time.Now()}
			_, err = s.Ingest(context.Background(), core.NewBindingVersion(b), m)
			require.NoError(t, err)
			require.NoError(t, s.Close())
			raw, err := os.ReadFile(cfg.DatabasePath)
			require.NoError(t, err)
			require.NotContains(t, string(raw), "original-input-secret")
			status, err := StatusWith(context.Background(), cfg, resolver)
			require.NoError(t, err)
			require.Len(t, status.Turns, 1)
			_, err = StatusWith(context.Background(), cfg, staticResolver{"env:AUTH_KEY": "bad-key"})
			require.Error(t, err)
			require.NotContains(t, err.Error(), "bad-key")
			checks := Doctor(context.Background(), cfg, DoctorOptions{Resolver: resolver})
			found := false
			for _, c := range checks {
				if c.Name == "user_auth_encryption" {
					found = true
					require.True(t, c.OK, c.Detail)
				}
			}
			require.True(t, found)
		})
	}
}

func TestUserAuthMissingStorageRequiresExplicitExpand(t *testing.T) {
	for _, driver := range []string{"embedded", "sqlite"} {
		t.Run(driver, func(t *testing.T) {
			cfg, err := Load(writeConfig(t, userConfigJSON()))
			require.NoError(t, err)
			cfg.Store.Driver = driver
			resolver := staticResolver{"env:AUTH_KEY": base64.StdEncoding.EncodeToString([]byte(strings.Repeat("k", 32)))}
			require.NoError(t, InstallStorageWith(context.Background(), cfg, resolver))
			if driver == "embedded" {
				db, e := bbolt.Open(cfg.DatabasePath, 0600, nil)
				require.NoError(t, e)
				require.NoError(t, db.Update(func(tx *bbolt.Tx) error { return tx.DeleteBucket([]byte("user_auth")) }))
				require.NoError(t, db.Close())
			} else {
				db, e := sql.Open("sqlite", cfg.DatabasePath)
				require.NoError(t, e)
				_, e = db.Exec("DROP TABLE channel_user_auth")
				require.NoError(t, e)
				require.NoError(t, db.Close())
			}
			before, err := os.ReadFile(cfg.DatabasePath)
			require.NoError(t, err)
			_, err = StatusWith(context.Background(), cfg, resolver)
			require.ErrorContains(t, err, "expand-user-auth")
			after, err := os.ReadFile(cfg.DatabasePath)
			require.NoError(t, err)
			require.Equal(t, before, after)
			require.NoError(t, ExpandUserAuthStorageWith(context.Background(), cfg, resolver))
			_, err = StatusWith(context.Background(), cfg, resolver)
			require.NoError(t, err)
		})
	}
}

func TestUserAuthDocumentedExampleValidatesOffline(t *testing.T) {
	cfg, err := Load("../configs/user-device.example.json")
	require.NoError(t, err)
	require.Equal(t, core.IdentityUser, cfg.Connections[0].resolvedIdentity.P2P)
}

func TestUserAuthRunnerWiresPrincipalAndDoctorNeverAuthorizes(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		require.Equal(t, http.MethodGet, r.Method)
		switch r.URL.Path {
		case "/api/v3/environments/env_1":
			_, _ = io.WriteString(w, `{"id":"env_1","type":"environment","name":"fixture","created_at":"2026-10-13T00:00:00Z","updated_at":"2026-10-13T00:00:00Z","config":{"type":"cloud"}}`)
		case "/api/v3/agents/agent_1":
			_, _ = io.WriteString(w, `{}`)
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			w.WriteHeader(500)
		}
	}))
	defer server.Close()
	oauthServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/open-apis/auth/v3/tenant_access_token/internal" {
			_, _ = io.WriteString(w, `{"code":0,"tenant_access_token":"synthetic-token"}`)
			return
		}
		_, _ = io.WriteString(w, `{"code":0,"data":{"app":{"app_id":"cli_app","scopes":[{"scope":"read","token_types":["user"]}]}}}`)
	}))
	defer oauthServer.Close()
	cfg, err := Load(writeConfig(t, userConfigJSON()))
	require.NoError(t, err)
	cfg.Connections[0].MA.BaseURL = server.URL
	resolver := staticResolver{"env:AUTH_KEY": base64.StdEncoding.EncodeToString([]byte(strings.Repeat("k", 32))), "env:APP_SECRET": "synthetic-app-secret", "env:MA_KEY": "synthetic-api-key"}
	require.NoError(t, InstallStorageWith(context.Background(), cfg, resolver))
	started := make(chan struct{})
	binding := make(chan core.Binding, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runWith(ctx, cfg, dependencies{Resolver: resolver, NewOAuth: func(c oauth.Config) (*oauth.Client, error) {
			c.OpenBaseURL = oauthServer.URL
			c.HTTPClient = oauthServer.Client()
			return oauth.New(c)
		}, NewProvider: func(b core.Binding, _ FeishuConfig, _ secret.Resolver) (core.Provider, error) {
			binding <- b
			return blockingProvider{started}, nil
		}, LogOutput: &syncBuffer{}})
	}()
	select {
	case <-started:
	case e := <-done:
		t.Fatalf("runner stopped: %v", e)
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("runner did not start")
	}
	cancel()
	require.NoError(t, <-done)
	b := <-binding
	require.Equal(t, "configured-owner", b.Owner)
	require.Equal(t, "configured-project", b.Project)
	require.Equal(t, "channel-a", b.ChannelID)
	require.EqualValues(t, 1, b.BindingEpoch)
	require.Equal(t, []string{"read"}, b.UserAuthScopes)
	checks := Doctor(context.Background(), cfg, DoctorOptions{Resolver: resolver, Online: true, HTTPClient: server.Client()})
	for _, check := range checks {
		require.True(t, check.OK, check.Detail)
	}
	require.EqualValues(t, 3, requests.Load())
}

func TestSelectApplicationUserScopes(t *testing.T) {
	scopes, err := selectApplicationUserScopes([]string{"a", "b"}, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, scopes)
	scopes, err = selectApplicationUserScopes([]string{"a", "b"}, []string{"b", "a", "b"})
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, scopes)
	_, err = selectApplicationUserScopes([]string{"a"}, []string{"tenant"})
	require.Error(t, err)
}
