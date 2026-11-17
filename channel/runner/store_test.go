// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package runner

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/secret"
)

func TestStoreDriverValidation(t *testing.T) {
	cases := map[string]string{
		"unknown driver":             `{"driver":"redis"}`,
		"mysql without dsn":          `{"driver":"mysql"}`,
		"mysql plaintext dsn":        `{"driver":"mysql","dsn_ref":"user:pass@tcp(db)/x"}`,
		"dsn on sqlite":              `{"driver":"sqlite","dsn_ref":"env:DSN"}`,
		"prefix on embedded":         `{"table_prefix":"a_"}`,
		"invalid prefix":             `{"driver":"sqlite","table_prefix":"a-b"}`,
		"oversized mysql identifier": `{"driver":"mysql","dsn_ref":"env:DSN","table_prefix":"` + strings.Repeat("x", 32) + `"}`,
	}
	for name, store := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeConfig(t, strings.Replace(validJSON(), `"database_path"`, `"store":`+store+`,"database_path"`, 1)))
			require.Error(t, err)
			require.NotContains(t, err.Error(), "pass")
		})
	}
	config, err := Load(writeConfig(t, strings.Replace(validJSON(), `"database_path":"state.db",`, `"store":{"driver":"mysql","dsn_ref":"env:CHANNEL_DSN","table_prefix":"prod_"},`, 1)))
	require.NoError(t, err, "mysql does not need database_path")
	require.Equal(t, "mysql", config.Store.Driver)
}

func TestRunOnSQLiteDriverAndAdminCommands(t *testing.T) {
	config, err := Load(writeConfig(t, strings.Replace(validJSON(), `"database_path"`, `"store":{"driver":"sqlite","table_prefix":"ch_"},"database_path"`, 1)))
	require.NoError(t, err)
	require.NoError(t, Install(t.Context(), config))
	t.Setenv("MA_KEY", "fixture-key")
	started := make(chan struct{})
	message := core.Message{BindingID: "primary", ProviderEventID: "evt", MessageID: "msg", TenantKey: "t", ChatID: "c", ChatType: "p2p", Text: "hi", ReceivedAt: time.Now()}
	deps := dependencies{
		Resolver: secret.EnvFileResolver{},
		NewMA:    func(_ MAConfig, _ string) (core.MAClient, error) { return fakeMA{}, nil },
		NewProvider: func(_ core.Binding, _ FeishuConfig, _ secret.Resolver) (core.Provider, error) {
			return acceptingProvider{message: message, started: started}, nil
		},
		LogOutput: &syncBuffer{},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runWith(ctx, config, deps) }()
	<-started
	// sqlite 支持多进程共享：Runner 运行期间只读状态查询也可用。
	require.Eventually(t, func() bool {
		status, err := Status(context.Background(), config)
		return err == nil && len(status.Turns) == 1
	}, 3*time.Second, 20*time.Millisecond)
	cancel()
	require.NoError(t, <-done)
	removed, err := Prune(context.Background(), config)
	require.NoError(t, err)
	require.Equal(t, core.PruneResult{}, removed)
	checks := Doctor(context.Background(), config, DoctorOptions{Resolver: staticResolver{"env:APP_SECRET": "a", "env:MA_KEY": "b"}})
	for _, check := range checks {
		if check.Name == "database" {
			require.True(t, check.OK, check.Detail)
		}
	}
}

func TestMySQLDriverFailsClosedWithoutSecretOrServer(t *testing.T) {
	config, err := Load(writeConfig(t, strings.Replace(validJSON(), `"database_path":"state.db",`, `"store":{"driver":"mysql","dsn_ref":"env:CHANNEL_DSN_UNSET"},`, 1)))
	require.NoError(t, err)
	_, err = Status(context.Background(), config)
	require.ErrorContains(t, err, "dsn unavailable")
	t.Setenv("CHANNEL_DSN_UNSET", "user:topsecret@tcp(127.0.0.1:1)/channel?timeout=200ms")
	_, err = Status(context.Background(), config)
	require.ErrorContains(t, err, "database unavailable")
	require.NotContains(t, err.Error(), "topsecret")
}

func TestIncompleteEmbeddedStoreIsRejectedWithoutChanges(t *testing.T) {
	config := testConfig(t)
	db, err := bbolt.Open(config.DatabasePath, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucket([]byte("meta"))
		return err
	}))
	require.NoError(t, db.Close())
	before, err := os.ReadFile(config.DatabasePath)
	require.NoError(t, err)
	_, err = Status(context.Background(), config)
	require.ErrorContains(t, err, "schema unsupported")
	after, err := os.ReadFile(config.DatabasePath)
	require.NoError(t, err)
	require.Equal(t, before, after)
}
