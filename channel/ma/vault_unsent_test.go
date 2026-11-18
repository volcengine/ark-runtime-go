// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package ma

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	memory "github.com/volcengine/ark-runtime-go/channel/internal/testutil/memorystore"
)

// TestCancelledVaultCreateDoesNotLeaveReservation 验证 POST 前失败不能将整个 Bot 主体永久变成 unknown。
func TestCancelledVaultCreateDoesNotLeaveReservation(t *testing.T) {
	var posts atomic.Int32
	var vault map[string]any
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			items := []any{}
			if vault != nil {
				items = append(items, vault)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": items})
		case http.MethodPost:
			posts.Add(1)
			if err := json.NewDecoder(r.Body).Decode(&vault); err != nil {
				w.WriteHeader(400)
				return
			}
			vault["id"] = "vault-recovered"
			vault["type"] = "vault"
			vault["created_at"] = "2026-10-13T00:00:00Z"
			vault["updated_at"] = "2026-10-13T00:00:00Z"
			_ = json.NewEncoder(w).Encode(vault)
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := NewClient(server.URL, "key", ClientOptions{})
	cancel()

	require.NoError(t, err)
	store, err := memory.New(memory.Options{})
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	coordinator := core.NewBotVaultCoordinator(store, client, "owner")
	spec := core.BotVaultCreate{Name: "bot-vault", DisplayName: "bot-vault"}
	_, err = client.CreateVault(ctx, spec)
	require.ErrorIs(t, err, core.ErrVaultNotCreated)
	require.ErrorIs(t, err, context.Canceled)
	_, err = coordinator.EnsureVault(ctx, "principal", spec, time.Minute)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, posts.Load())
	pending, err := store.GetBotVaultReservation(context.Background(), "principal", spec.Name)
	require.NoError(t, err)
	require.Nil(t, pending)
	ref, err := coordinator.EnsureVault(context.Background(), "principal", spec, time.Minute)
	require.NoError(t, err)
	require.Equal(t, "vault-recovered", ref.ID)
	require.EqualValues(t, 1, posts.Load())
}
