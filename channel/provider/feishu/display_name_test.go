// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestLookupBotNameWithoutReceiver(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/open-apis/auth/v3/tenant_access_token/internal":
			_, _ = w.Write([]byte(`{"code":0,"tenant_access_token":"test-token","expire":7200}`))
		case "/open-apis/bot/v3/info":
			require.Equal(t, "Bearer test-token", r.Header.Get("Authorization"))
			_, _ = w.Write([]byte(`{"code":0,"bot":{"open_id":"bot-id","app_name":"销售助手"}}`))
		default:
			t.Errorf("unexpected request: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	p, err := NewProvider(ProviderConfig{Binding: core.Binding{ID: "binding", AppID: t.Name()}, AppSecretRef: "env:APP", SecretResolver: fixedSecret("test-secret"), OpenBaseURL: server.URL, HTTPClient: server.Client()})
	require.NoError(t, err)
	lookup, ok := any(p).(interface {
		LookupBotName(context.Context) (string, error)
	})
	require.True(t, ok, "provider must expose the bot application name")
	name, err := lookup.LookupBotName(context.Background())
	require.NoError(t, err)
	require.Equal(t, "销售助手", name)
	select {
	case <-p.Ready():
		t.Fatal("name lookup must not start a receiver")
	default:
	}
}

func TestDisplayNameLookupPreservesSafeAPICode(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusForbidden} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/open-apis/auth/v3/tenant_access_token/internal" {
					_, _ = w.Write([]byte(`{"code":0,"tenant_access_token":"test-token","expire":7200}`))
					return
				}
				w.WriteHeader(status)
				_, _ = w.Write([]byte(`{"code":99991672,"msg":"private upstream diagnostics"}`))
			}))
			defer server.Close()
			p, err := NewProvider(ProviderConfig{Binding: core.Binding{ID: "binding", AppID: t.Name()}, AppSecretRef: "env:APP", SecretResolver: fixedSecret("test-secret"), OpenBaseURL: server.URL, HTTPClient: server.Client()})
			require.NoError(t, err)
			_, err = p.LookupUserName(context.Background(), "user")
			require.Error(t, err)
			var coded core.APIErrorCode
			require.ErrorAs(t, err, &coded)
			require.Equal(t, status, coded.HTTPStatus())
			require.Equal(t, "99991672", coded.ErrorCode())
			require.NotContains(t, err.Error(), "private")
		})
	}
}

func TestDisplayNameLookupWithoutReceiverAndPermissionFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/open-apis/auth/v3/tenant_access_token/internal":
			_, _ = w.Write([]byte(`{"code":0,"tenant_access_token":"test-token","expire":7200}`))
		case "/open-apis/im/v1/chats/chat":
			require.Equal(t, "Bearer test-token", r.Header.Get("Authorization"))
			_, _ = w.Write([]byte(`{"code":0,"data":{"name":"测试群"}}`))
		case "/open-apis/contact/v3/users/user":
			require.Equal(t, "open_id", r.URL.Query().Get("user_id_type"))
			_, _ = w.Write([]byte(`{"code":0,"data":{"user":{"name":"测试用户"}}}`))
		default:
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"code":99991672,"msg":"private upstream diagnostics"}`))
		}
	}))
	defer server.Close()
	p, err := NewProvider(ProviderConfig{Binding: core.Binding{ID: "binding", AppID: "display-name-test-app"}, AppSecretRef: "env:APP", SecretResolver: fixedSecret("test-secret"), OpenBaseURL: server.URL, HTTPClient: server.Client()})
	require.NoError(t, err)
	chat, err := p.LookupChatName(context.Background(), "chat")
	require.NoError(t, err)
	require.Equal(t, "测试群", chat)
	name, err := p.LookupUserName(context.Background(), "user")
	require.NoError(t, err)
	require.Equal(t, "测试用户", name)
	_, err = p.LookupUserName(context.Background(), "denied")
	require.Error(t, err)
	require.NotContains(t, err.Error(), "private")
	_, err = p.LookupUserName(context.Background(), "")
	require.Error(t, err)
	select {
	case <-p.Ready():
		t.Fatal("name lookup must not start a receiver")
	default:
	}
}
