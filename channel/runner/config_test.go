// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package runner

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func validJSON() string {
	return `{"database_path":"state.db","connections":[{"id":"primary","feishu":{"app_id":"cli_app","app_secret_ref":"env:APP_SECRET"},"ma":{"base_url":"https://example.com/api/v3","api_key_ref":"env:MA_KEY"},"agent_id":"agent_1","agent_version":"v1","environment_id":"env_1","identity":"none","response_mode":"non_stream"}]}`
}

func writeConfig(t *testing.T, data string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "runner.json")
	require.NoError(t, os.WriteFile(path, []byte(data), 0o600))
	return path
}

func TestLoadValidatesOfflineAndAnchorsDatabase(t *testing.T) {
	path := writeConfig(t, validJSON())
	config, err := Load(path)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(filepath.Dir(path), "state.db"), config.DatabasePath)
	require.Equal(t, 4, config.Runtime.Workers)
	require.Equal(t, 5*time.Minute, config.Runtime.TurnTimeout)
	require.Equal(t, "mention", config.Connections[0].GroupTrigger)
	require.Equal(t, "thread", config.Connections[0].GroupReplyMode)
	require.NoFileExists(t, config.DatabasePath)
}

func TestLoadRejectsInvalidConfig(t *testing.T) {
	cases := map[string]string{
		"unknown":              strings.Replace(validJSON(), `"identity":"none"`, `"api_key":"leak","identity":"none"`, 1),
		"trailing":             validJSON() + `{}`,
		"array":                `[]`,
		"duplicate app":        strings.Replace(validJSON(), `]}`, `,{"id":"other","feishu":{"app_id":"cli_app","app_secret_ref":"env:OTHER"},"ma":{"base_url":"https://example.com","api_key_ref":"env:MA_KEY"},"agent_id":"agent_1","agent_version":"1","environment_id":"env_1"}]}`, 1),
		"duplicate id":         strings.Replace(validJSON(), `]}`, `,{"id":"primary","feishu":{"app_id":"cli_other","app_secret_ref":"env:OTHER"},"ma":{"base_url":"https://example.com","api_key_ref":"env:MA_KEY"},"agent_id":"agent_1","agent_version":"1","environment_id":"env_1"}]}`, 1),
		"unsupported identity": strings.Replace(validJSON(), `"identity":"none"`, `"identity":"user"`, 1),
		"bot without vault":    strings.Replace(validJSON(), `"identity":"none"`, `"identity":"bot"`, 1),
		"bot vault not listed": strings.Replace(validJSON(), `"identity":"none"`, `"identity":"bot","bot_credential":{"vault_id":"vlt_1"}`, 1),
		"unsupported stream":   strings.Replace(validJSON(), `"response_mode":"non_stream"`, `"response_mode":"stream"`, 1),
		"unsupported oauth":    strings.Replace(validJSON(), `"identity":"none"`, `"user_oauth":true,"identity":"none"`, 1),
		"invalid secret":       strings.Replace(validJSON(), `env:APP_SECRET`, `plaintext`, 1),
		"url query":            strings.Replace(validJSON(), `https://example.com/api/v3`, `https://example.com/api/v3?key=x`, 1),
		"duration":             strings.Replace(validJSON(), `"database_path"`, `"runtime":{"turn_timeout":"31m"},"database_path"`, 1),
		"connection timeout":   strings.Replace(validJSON(), `"identity":"none"`, `"max_turn_duration":"7m","identity":"none"`, 1),
		"unknown nested":       strings.Replace(validJSON(), `"app_id":"cli_app"`, `"app_id":"cli_app","raw_secret":"leak"`, 1),
		"trailing junk":        validJSON() + ` garbage`,
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeConfig(t, data))
			require.Error(t, err)
			require.NotContains(t, err.Error(), "leak")
		})
	}
}

func TestLoadRejectsOversizedFile(t *testing.T) {
	_, err := Load(writeConfig(t, validJSON()+strings.Repeat(" ", maxConfigBytes)))
	require.Error(t, err)
}

func TestLoadRejectsInvalidGroupIdentityWithConnectionAndField(t *testing.T) {
	for _, tc := range []struct {
		name     string
		identity string
	}{
		{"group user", `"identity_v2":{"group":"user"}`},
		{"group user with p2p bot", `"identity_v2":{"group":"user","p2p":"bot"}`},
		{"group user with p2p user", `"identity_v2":{"group":"user","p2p":"user"}`},
		{"invalid group with legacy bot", `"identity":"bot","identity_v2":{"group":"weird"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := strings.Replace(validJSON(), `"identity":"none"`, tc.identity, 1)
			path := writeConfig(t, data)
			_, err := Load(path)
			require.EqualError(t, err, `connection "primary" identity_v2.group must be empty or bot`)
			require.NoFileExists(t, filepath.Join(filepath.Dir(path), "state.db"))
		})
	}
}

func TestBotIdentityConfigDefaults(t *testing.T) {
	config, err := Load(writeConfig(t, strings.Replace(validJSON(), `"identity":"none"`, `"identity":"bot","vault_ids":["vlt_1"],"bot_credential":{"vault_id":"vlt_1"}`, 1)))
	require.NoError(t, err)
	require.Equal(t, "bot", config.Connections[0].Identity)
	require.Equal(t, "FEISHU_TENANT_ACCESS_TOKEN", config.Connections[0].BotCredential.SecretName)
	require.Equal(t, []string{"open.feishu.cn"}, config.Connections[0].BotCredential.AllowedHosts)
}

func TestLoadIdleTimeoutAndLegacyAlias(t *testing.T) {
	for _, tc := range []struct {
		name, runtime string
		want          time.Duration
		invalid       bool
	}{
		{name: "default", runtime: `{}`, want: time.Hour},
		{name: "idle", runtime: `{"idle_timeout":"60m"}`, want: time.Hour},
		{name: "legacy", runtime: `{"message_deadline":"40m"}`, want: 40 * time.Minute},
		{name: "conflict", runtime: `{"idle_timeout":"60m","message_deadline":"40m"}`, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config, err := Load(writeConfig(t, strings.Replace(validJSON(), `"database_path"`, `"runtime":`+tc.runtime+`,"database_path"`, 1)))
			if tc.invalid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, config.Runtime.IdleTimeout)
			require.Equal(t, tc.want, config.Runtime.MessageDeadline)
		})
	}
}
