// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestMessageContextCachesSuccessAndPermissionFailureByTenant(t *testing.T) {
	var chats, users atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/open-apis/auth/v3/tenant_access_token/internal":
			_, _ = w.Write([]byte(`{"code":0,"tenant_access_token":"token","expire":7200}`))
		case "/open-apis/im/v1/chats/oc_chat":
			chats.Add(1)
			_, _ = w.Write([]byte(`{"code":0,"data":{"name":"普通群","chat_mode":"group"}}`))
		case "/open-apis/contact/v3/users/ou_user":
			users.Add(1)
			_, _ = w.Write([]byte(`{"code":99991672,"msg":"permission denied"}`))
		default:
			t.Errorf("unexpected request: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	p, err := NewProvider(ProviderConfig{Binding: core.Binding{ID: "binding", AppID: t.Name()}, AppSecretRef: "env:APP", SecretResolver: fixedSecret("secret"), OpenBaseURL: server.URL, HTTPClient: server.Client()})
	require.NoError(t, err)
	resolver, ok := any(p).(core.ChannelContextProvider)
	require.True(t, ok)
	require.Equal(t, "feishu", resolver.ChannelProvider())
	m := core.Message{TenantKey: "tenant-a", ChatType: "group", ChatID: "oc_chat", ThreadID: "omt_thread", SenderID: "ou_user"}
	for range 2 {
		details, err := resolver.ResolveMessageContext(context.Background(), m)
		require.Error(t, err)
		require.Equal(t, core.MessageContext{ChatName: "普通群", ChatMode: "group"}, details, "缺 contact 权限不应丢弃成功的群信息")
	}
	require.Equal(t, int32(1), chats.Load())
	require.Equal(t, int32(1), users.Load())
	m.TenantKey = "tenant-b"
	_, _ = resolver.ResolveMessageContext(context.Background(), m)
	require.Equal(t, int32(2), chats.Load())
	require.Equal(t, int32(2), users.Load())
	p.messageContextCache.mu.Lock()
	now := time.Now()
	for key, entry := range p.messageContextCache.entries {
		if key.tenant != "tenant-a" {
			continue
		}
		if key.kind == "user" {
			require.WithinDuration(t, now.Add(time.Minute), entry.expires, time.Second)
		} else {
			require.WithinDuration(t, now.Add(10*time.Minute), entry.expires, time.Second)
		}
		entry.expires = now.Add(-time.Second)
		p.messageContextCache.entries[key] = entry
	}
	p.messageContextCache.mu.Unlock()
	m.TenantKey = "tenant-a"
	_, _ = resolver.ResolveMessageContext(context.Background(), m)
	require.Equal(t, int32(3), chats.Load(), "过期后必须重查")
	require.Equal(t, int32(3), users.Load())
}

func TestMessageContextCacheIsBoundedUnderConcurrency(t *testing.T) {
	p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/open-apis/auth/v3/tenant_access_token/internal" {
			tokenResponse(w)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":0,"data":{"user":{"name":"用户"}}}`))
	})
	var workers sync.WaitGroup
	for worker := range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range 80 {
				details, err := p.ResolveMessageContext(t.Context(), core.Message{TenantKey: "tenant", ChatType: "p2p", SenderID: "user-" + strconv.Itoa(worker*80+index)})
				if err != nil || details.SenderName != "用户" {
					t.Errorf("上下文查询失败：%v", err)
				}
			}
		}()
	}
	workers.Wait()
	p.messageContextCache.mu.Lock()
	defer p.messageContextCache.mu.Unlock()
	require.LessOrEqual(t, len(p.messageContextCache.entries), 256)
}

func TestMessageContextBoundsSlowLookup(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/open-apis/auth/v3/tenant_access_token/internal" {
			_, _ = w.Write([]byte(`{"code":0,"tenant_access_token":"token","expire":7200}`))
			return
		}
		<-r.Context().Done()
	}))
	defer server.Close()
	p, err := NewProvider(ProviderConfig{Binding: core.Binding{ID: "binding", AppID: t.Name()}, AppSecretRef: "env:APP", SecretResolver: fixedSecret("secret"), OpenBaseURL: server.URL, HTTPClient: server.Client()})
	require.NoError(t, err)
	resolver, ok := any(p).(core.ChannelContextProvider)
	require.True(t, ok)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	start := time.Now()
	details, err := resolver.ResolveMessageContext(ctx, core.Message{ChatType: "p2p", TenantKey: "t", SenderID: "user"})
	require.Error(t, err)
	require.Less(t, time.Since(start), 250*time.Millisecond)
	require.Empty(t, details.SenderName)
	select {
	case <-p.Ready():
		t.Fatal("上下文查询不得启动接收器")
	default:
	}
}
