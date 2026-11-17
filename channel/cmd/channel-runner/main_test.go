// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

const fixtureConfig = `{"database_path":"state.db","connections":[{"id":"primary","feishu":{"app_id":"cli_app","app_secret_ref":"env:APP_SECRET"},"ma":{"base_url":"https://example.com","api_key_ref":"env:MA_KEY"},"agent_id":"agent_1","agent_version":"v1","environment_id":"env_1"}]}`

func TestInstallCommandCreatesStorageExplicitly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	require.NoError(t, os.WriteFile(path, []byte(fixtureConfig), 0o600))
	var stdout, stderr bytes.Buffer
	require.Zero(t, execute([]string{"install", "--config", path}, &stdout, &stderr), stderr.String())
	require.FileExists(t, filepath.Join(filepath.Dir(path), "state.db"))
	require.Contains(t, stdout.String(), "storage installed")
	require.Empty(t, stderr.String())
}

func TestExecuteUsageAndOfflineValidate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	require.NoError(t, os.WriteFile(path, []byte(fixtureConfig), 0o600))
	for _, args := range [][]string{nil, {"wat"}, {"validate"}, {"validate", "--config", path, "extra"}, {"validate", "--config", path, "--unknown"}} {
		var stdout, stderr bytes.Buffer
		require.NotZero(t, execute(args, &stdout, &stderr))
		require.Contains(t, stderr.String(), "usage")
	}
	var stdout, stderr bytes.Buffer
	require.Zero(t, execute([]string{"validate", "--config", path}, &stdout, &stderr))
	require.Contains(t, stdout.String(), "offline")
	require.Empty(t, stderr.String())
	require.NoFileExists(t, filepath.Join(filepath.Dir(path), "state.db"))
	stdout.Reset()
	stderr.Reset()
	require.NotZero(t, execute([]string{"status", "--config", path}, &stdout, &stderr))
	require.Contains(t, stderr.String(), "database does not exist")
}

func TestSubcommandHelpExitsSuccessfully(t *testing.T) {
	for _, command := range []string{"validate", "install", "run", "status", "doctor", "reset", "retry-delivery", "prune"} {
		t.Run(command, func(t *testing.T) {
			for _, help := range []string{"--help", "-h"} {
				var stdout, stderr bytes.Buffer
				require.Zero(t, execute([]string{command, help}, &stdout, &stderr))
				require.Contains(t, stdout.String(), "usage")
				require.Empty(t, stderr.String())
			}
		})
	}
	var stdout, stderr bytes.Buffer
	require.NotZero(t, execute([]string{"validate", "--unknown"}, &stdout, &stderr))
	require.Contains(t, stderr.String(), "usage")
}

func TestAdminCommandsRequireArguments(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	require.NoError(t, os.WriteFile(path, []byte(fixtureConfig), 0o600))
	for _, args := range [][]string{{"reset", "--config", path}, {"retry-delivery", "--config", path}} {
		var stdout, stderr bytes.Buffer
		require.Equal(t, 2, execute(args, &stdout, &stderr))
		require.Contains(t, stderr.String(), "usage")
	}
	var stdout, stderr bytes.Buffer
	require.Equal(t, 1, execute([]string{"retry-delivery", "--config", path, "--id", "d1"}, &stdout, &stderr))
	require.Contains(t, stderr.String(), "database does not exist")
	stdout.Reset()
	stderr.Reset()
	require.Equal(t, 1, execute([]string{"doctor", "--config", path}, &stdout, &stderr), "unresolvable secrets fail doctor")
	require.Contains(t, stdout.String(), "FAIL connection primary: ma_api_key")
	require.Contains(t, stdout.String(), "ok   config")
}
