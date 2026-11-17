// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package runner

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/store/embedded"
)

// seedFailedDelivery 写入一个已结算轮次、一个已绑定 Session 的会话和一条 failed 投递。
func seedFailedDelivery(t *testing.T, config Config) (core.RouteKey, string) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, Install(ctx, config))
	store, err := openTestEmbedded(config.DatabasePath, embedded.Options{})
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	binding := core.Binding{ID: "primary", AppID: "cli_app", AgentID: "agent_1", AgentVersion: "v1", EnvironmentID: "env_1"}
	message := core.Message{BindingID: "primary", ProviderEventID: "evt", MessageID: "msg", TenantKey: "t", ChatID: "c", ChatType: "p2p", Text: "hi", ReceivedAt: time.Now()}
	ingested, err := store.Ingest(ctx, core.NewBindingVersion(binding), message)
	require.NoError(t, err)
	leased, err := store.LeaseTurn(ctx, core.ClaimOptions{Owner: "seed", TTL: time.Minute, MessageDeadline: time.Hour})
	require.NoError(t, err)
	_, err = store.BindSession(ctx, leased.Turn.Lease, core.SessionBinding{State: core.SessionActive, Generation: 1, SessionID: "sesn-1", CreationKey: "k", LookupKey: "l", BindingVersion: ingested.Turn.BindingVersion})
	require.NoError(t, err)
	delivery := core.NewDelivery(core.DeliverySource{Kind: core.SourceTurn, ID: ingested.Turn.ID}, 0, "answer", "primary", core.ReplyTargetFor(message, binding), core.TextBody("a"))
	require.NoError(t, store.SettleTurn(ctx, leased.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, []core.Delivery{delivery}))
	claimed, err := store.LeaseDelivery(ctx, "seed", time.Minute)
	require.NoError(t, err)
	require.NoError(t, store.CompleteDelivery(ctx, claimed.Lease, core.DeliveryResult{Status: core.DeliveryFailed, ErrorKind: "delivery_unconfirmed"}))
	return ingested.Turn.ConversationID, delivery.ID
}

func TestAdminCommandsOperateOnStoppedDatabase(t *testing.T) {
	config := testConfig(t)
	_, err := ResetConversation(context.Background(), config, "route:v1:x")
	require.ErrorContains(t, err, "does not exist")
	conversation, deliveryID := seedFailedDelivery(t, config)
	_, err = ResetConversation(context.Background(), config, "not-a-route")
	require.Error(t, err)
	reset, err := ResetConversation(context.Background(), config, string(conversation))
	require.NoError(t, err)
	require.Equal(t, uint64(2), reset.Generation)
	require.Equal(t, core.SessionReserved, reset.State)
	require.Empty(t, reset.SessionID)
	_, err = ResetConversation(context.Background(), config, "route:v1:unknown")
	require.ErrorContains(t, err, "no session binding")
	delivery, err := RetryDelivery(context.Background(), config, deliveryID)
	require.NoError(t, err)
	require.Equal(t, core.DeliveryPending, delivery.State)
	_, err = RetryDelivery(context.Background(), config, deliveryID)
	require.ErrorContains(t, err, "only failed")
	_, err = RetryDelivery(context.Background(), config, "missing")
	require.ErrorContains(t, err, "not found")
	removed, err := Prune(context.Background(), config)
	require.NoError(t, err)
	require.Equal(t, core.PruneResult{}, removed, "fresh records stay within retention")
	// Runner 持有写锁时运维命令拒绝执行。
	store, err := openTestEmbedded(config.DatabasePath, embedded.Options{})
	require.NoError(t, err)
	_, err = RetryDelivery(context.Background(), config, deliveryID)
	require.ErrorContains(t, err, "stop runner")
	require.NoError(t, store.Close())
}

type staticResolver map[string]string

func (r staticResolver) Resolve(ref string) (string, error) {
	if value, ok := r[ref]; ok {
		return value, nil
	}
	return "", errNotFound
}

var errNotFound = &resolveError{}

type resolveError struct{}

func (*resolveError) Error() string { return "missing" }

func TestDoctorReportsSecretsDatabaseAndOnlineProbes(t *testing.T) {
	var sawAuth, sawFeishu bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v3/agents/agent_1":
			sawAuth = r.Header.Get("Authorization") == "Bearer ma-key"
			_, _ = w.Write([]byte(`{"id":"agent_1"}`))
		case "/open-apis/auth/v3/tenant_access_token/internal":
			sawFeishu = true
			_, _ = w.Write([]byte(`{"code":0,"tenant_access_token":"t-xxx"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	data := strings.Replace(validJSON(), `https://example.com/api/v3`, server.URL+`/api/v3`, 1)
	data = strings.Replace(data, `"app_secret_ref":"env:APP_SECRET"`, `"app_secret_ref":"env:APP_SECRET","open_base_url":"`+server.URL+`"`, 1)
	config, err := Load(writeConfig(t, data))
	require.NoError(t, err)
	checks := Doctor(context.Background(), config, DoctorOptions{Resolver: staticResolver{}})
	byName := map[string]Check{}
	for _, check := range checks {
		byName[check.Name] = check
	}
	require.False(t, byName["database"].OK, "a missing database requires explicit installation")
	require.Contains(t, byName["database"].Detail, "install")
	require.NoFileExists(t, config.DatabasePath)
	require.False(t, byName["connection primary: ma_api_key"].OK)
	require.False(t, byName["connection primary: feishu_app_secret"].OK)
	seedFailedDelivery(t, config)
	checks = Doctor(context.Background(), config, DoctorOptions{Resolver: staticResolver{"env:APP_SECRET": "app-secret", "env:MA_KEY": "ma-key"}, Online: true})
	byName = map[string]Check{}
	for _, check := range checks {
		byName[check.Name] = check
		require.NotContains(t, check.Detail, "app-secret")
		require.NotContains(t, check.Detail, "ma-key")
	}
	require.False(t, byName["failed_deliveries"].OK, "failed deliveries need attention")
	require.Equal(t, "1", byName["failed_deliveries"].Detail)
	require.True(t, byName["connection primary: ma_agent"].OK)
	require.True(t, byName["connection primary: feishu_token"].OK)
	require.True(t, sawAuth)
	require.True(t, sawFeishu)
}

// TestDoctorChecksBotIdentityEnvironment 验证 doctor --online 会 GET 绑定环境并拒绝
// self_hosted 与 bot 身份的组合；cloud 环境通过。
func TestDoctorChecksBotIdentityEnvironment(t *testing.T) {
	for _, tc := range []struct {
		name    string
		envType string
		ok      bool
	}{
		{"cloud", "cloud", true},
		{"old_fixture_type", "ark_sandbox", false},
		{"unknown", "future_environment", false},
		{"missing_config_type", "", false},
		{"self_hosted", "self_hosted", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/api/v3/environments/env_1":
					_, _ = w.Write([]byte(`{"id":"env_1","type":"environment","config":{"type":"` + tc.envType + `"}}`))
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			data := strings.Replace(validJSON(), `https://example.com/api/v3`, server.URL+`/api/v3`, 1)
			// 绑定 bot 身份并挂载 bot vault，使离线校验通过。
			data = strings.Replace(data, `"identity":"none"`, `"identity":"bot","vault_ids":["vlt_1"],"bot_credential":{"vault_id":"vlt_1"}`, 1)
			config, err := Load(writeConfig(t, data))
			require.NoError(t, err)
			checks := Doctor(context.Background(), config, DoctorOptions{Resolver: staticResolver{"env:MA_KEY": "ma-key"}, Online: true})
			var found *Check
			for i := range checks {
				if checks[i].Name == "connection primary: identity_environment" {
					found = &checks[i]
				}
			}
			require.NotNil(t, found, "identity_environment check must run for bot connections")
			require.Equal(t, tc.ok, found.OK, found.Detail)
		})
	}
}

// TestDoctorBotIdentityCheckIsOfflineToo 验证即使不 --online，bot 连接也会出现
// identity_environment 静态检查项（self_hosted 的在线确认仅 online 时进行）。
func TestDoctorBotIdentityCheckIsOfflineToo(t *testing.T) {
	data := strings.Replace(validJSON(), `"identity":"none"`, `"identity":"bot","vault_ids":["vlt_1"],"bot_credential":{"vault_id":"vlt_1"}`, 1)
	config, err := Load(writeConfig(t, data))
	require.NoError(t, err)
	checks := Doctor(context.Background(), config, DoctorOptions{Resolver: staticResolver{}})
	var found bool
	for _, c := range checks {
		if c.Name == "connection primary: identity_environment" {
			found = true
			require.True(t, c.OK)
		}
	}
	require.True(t, found)
}
