// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package ma

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	memory "github.com/volcengine/ark-runtime-go/channel/internal/testutil/memorystore"
)

func TestUnknownVaultCreateRecoversByPrincipalWithoutScanning(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "new marker", true: "legacy principal"}[legacy], func(t *testing.T) {
			posts, gets := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					posts++
					w.WriteHeader(http.StatusBadGateway)
					return
				}
				gets++
				assert.Equal(t, "/api/v3/vaults", r.URL.Path)
				var filter map[string]string
				assert.NoError(t, json.Unmarshal([]byte(r.URL.Query().Get("metadata")), &filter))
				assert.Equal(t, map[string]string{"ark_channel_principal": "principal"}, filter)
				assert.Equal(t, "2", r.URL.Query().Get("limit"))
				metadata := map[string]string{"ark_channel_principal": "principal"}
				if !legacy {
					metadata["ark_channel_vault_key"] = "creation-key"
				}
				displayName := "customer-renamed"
				if legacy {
					displayName = "creation-key"
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{map[string]any{
					"id": "vlt-recovered", "type": "vault", "created_at": "2026-10-13T00:00:00Z", "updated_at": "2026-10-13T00:00:00Z", "display_name": displayName, "metadata": metadata,
				}}})
			}))
			defer server.Close()
			client, err := NewClient(server.URL, "key", ClientOptions{})
			require.NoError(t, err)
			store, err := memory.New(memory.Options{})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			coordinator := core.NewBotVaultCoordinator(store, client, "owner")
			ref, err := coordinator.EnsureVault(context.Background(), "principal", core.BotVaultCreate{Name: "creation-key", DisplayName: "Channel User"}, time.Minute)
			require.NoError(t, err)
			require.Equal(t, "vlt-recovered", ref.ID)
			require.Equal(t, 1, posts, "unknown creation is reconciled without a transport replay")
			require.Equal(t, 1, gets)
		})
	}
}

func TestVaultRecoveryRejectsUnprovenCandidates(t *testing.T) {
	for _, tc := range []struct {
		name, body string
	}{
		{"other principal", `{"data":[{"id":"vlt-other","metadata":{"ark_channel_principal":"foreign","ark_channel_vault_key":"creation-key"},"type":"vault","display_name":"fixture","created_at":"2026-10-13T00:00:00Z","updated_at":"2026-10-13T00:00:00Z"}]}`},
		{"other creation", `{"data":[{"id":"vlt-other","metadata":{"ark_channel_principal":"principal","ark_channel_vault_key":"other-key"},"type":"vault","display_name":"fixture","created_at":"2026-10-13T00:00:00Z","updated_at":"2026-10-13T00:00:00Z"}]}`},
		{"unrelated legacy vault", `{"data":[{"id":"vlt-old","display_name":"old-generation","metadata":{"ark_channel_principal":"principal"},"type":"vault","created_at":"2026-10-13T00:00:00Z","updated_at":"2026-10-13T00:00:00Z"}]}`},
		{"duplicate", `{"data":[{"id":"vlt-1","type":"vault","display_name":"fixture","created_at":"2026-10-13T00:00:00Z","updated_at":"2026-10-13T00:00:00Z"},{"id":"vlt-2","type":"vault","display_name":"fixture","created_at":"2026-10-13T00:00:00Z","updated_at":"2026-10-13T00:00:00Z"}]}`},
		{"more results", `{"data":[],"next_page":"next"}`},
		{"missing id", `{"data":[{"metadata":{"ark_channel_principal":"principal","ark_channel_vault_key":"creation-key"}}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls++
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			client, err := NewClient(server.URL, "key", ClientOptions{})
			require.NoError(t, err)
			_, err = client.FindVaultByPrincipal(context.Background(), "principal", "creation-key")
			require.Error(t, err)
			require.Equal(t, 1, calls)
		})
	}
}
