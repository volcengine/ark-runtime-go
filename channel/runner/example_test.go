// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package runner

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// exampleConfig 与 README 中的完整配置样例逐字一致，保证文档样例始终能通过离线校验。
const exampleConfig = `{
  "database_path": "state/channel.db",
  "runtime": {
    "workers": 4,
    "poll_interval": "500ms",
    "turn_timeout": "5m",
    "lease_ttl": "6m",
    "delivery_timeout": "30s",
    "delivery_max_attempts": 5,
    "message_deadline": "10m30s",
    "unknown_write_grace": "30s",
    "retention": "168h",
    "prune_interval": "1h",
    "attachments": {"max_count": 5, "max_bytes": 99999999, "max_total_bytes": 500000000},
    "context": {"max_messages": 20, "max_chars": 8000, "max_age": "24h"},
    "artifacts": {"max_count": 5, "max_bytes": 31457280}
  },
  "store": {
    "driver": "embedded",
    "max_open_turns": 1000,
    "max_message_bytes": 32768
  },
  "observability": {
    "admin_addr": "127.0.0.1:9090",
    "log_level": "info",
    "log_format": "json"
  },
  "connections": [
    {
      "id": "primary",
      "feishu": {
        "app_id": "cli_replace_with_feishu_app_id",
        "app_secret_ref": "env:FEISHU_APP_SECRET",
        "receive_mode": "websocket"
      },
      "ma": {
        "base_url": "https://ma.example.com/api/v3",
        "api_key_ref": "env:MA_API_KEY"
      },
      "agent_id": "replace_with_agent_id",
      "agent_version": "v1",
      "environment_id": "replace_with_environment_id",
      "vault_ids": ["replace_with_bot_vault_id"],
      "memory_ids": [],
      "group_trigger": "mention",
      "group_reply_mode": "thread",
      "max_turn_duration": "5m",
      "identity": "bot",
      "bot_credential": {
        "vault_id": "replace_with_bot_vault_id",
        "secret_name": "FEISHU_TENANT_ACCESS_TOKEN",
        "allowed_hosts": ["open.feishu.cn"]
      },
      "response_mode": "non_stream",
      "show_progress": true,
      "approvals": true,
      "recent_context": true,
      "deliver_artifacts": true,
      "hook_fail_open": false
    }
  ]
}`

func TestDocumentedExampleConfigValidates(t *testing.T) {
	config, err := Load(writeConfig(t, exampleConfig))
	require.NoError(t, err)
	require.Equal(t, 10*time.Minute+30*time.Second, config.Runtime.MessageDeadline)
	require.Equal(t, 30*time.Second, config.Runtime.UnknownWriteGrace)
	require.Equal(t, 5*time.Minute, config.Connections[0].MaxTurnDuration)
	require.Equal(t, "bot", config.Connections[0].Identity)
	require.True(t, config.Connections[0].DeliverArtifacts)
	require.Equal(t, 24*time.Hour, config.Runtime.Context.MaxAge)
}
