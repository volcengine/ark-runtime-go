// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package runner

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/secret"
)

type webhookProvider struct {
	blockingProvider
}

func (webhookProvider) WebhookHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"challenge":"ok"}`)
	})
}

func freeAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	require.NoError(t, listener.Close())
	return addr
}

func TestRunMountsWebhookPerConnection(t *testing.T) {
	addr := freeAddr(t)
	data := strings.Replace(validJSON(), `"app_secret_ref":"env:APP_SECRET"`, `"app_secret_ref":"env:APP_SECRET","receive_mode":"webhook","verification_token_ref":"env:VERIFY","encrypt_key_ref":"env:ENCRYPT"`, 1)
	data = strings.Replace(data, `"database_path"`, `"webhook_addr":"`+addr+`","database_path"`, 1)
	config, err := Load(writeConfig(t, data))
	require.NoError(t, err)
	require.NoError(t, Install(t.Context(), config))
	t.Setenv("MA_KEY", "k")
	started := make(chan struct{})
	deps := dependencies{
		Resolver:  secret.EnvFileResolver{},
		NewMA:     func(_ MAConfig, _ string) (core.MAClient, error) { return fakeMA{}, nil },
		LogOutput: &syncBuffer{},
		NewProvider: func(_ core.Binding, cfg FeishuConfig, _ secret.Resolver) (core.Provider, error) {
			require.Equal(t, "webhook", cfg.ReceiveMode)
			require.Equal(t, "env:VERIFY", cfg.VerificationTokenRef)
			return webhookProvider{blockingProvider: blockingProvider{started}}, nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runWith(ctx, config, deps) }()
	<-started
	var body string
	require.Eventually(t, func() bool {
		resp, err := http.Post("http://"+addr+"/feishu/primary/events", "application/json", bytes.NewReader([]byte(`{}`)))
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		raw, _ := io.ReadAll(resp.Body)
		body = string(raw)
		return resp.StatusCode == http.StatusOK
	}, 3*time.Second, 20*time.Millisecond)
	require.JSONEq(t, `{"challenge":"ok"}`, body)
	resp, err := http.Post("http://"+addr+"/feishu/other/events", "application/json", nil)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode, "only configured connections are mounted")
	cancel()
	require.NoError(t, <-done)
}

func TestWebhookConfigValidation(t *testing.T) {
	webhook := `"app_secret_ref":"env:APP_SECRET","receive_mode":"webhook","verification_token_ref":"env:VERIFY","encrypt_key_ref":"env:ENCRYPT"`
	cases := map[string]string{
		"missing encrypt key": strings.Replace(strings.Replace(validJSON(), `"app_secret_ref":"env:APP_SECRET"`, `"app_secret_ref":"env:APP_SECRET","receive_mode":"webhook","verification_token_ref":"env:VERIFY"`, 1), `"database_path"`, `"webhook_addr":"127.0.0.1:8080","database_path"`, 1),
		"missing addr":        strings.Replace(validJSON(), `"app_secret_ref":"env:APP_SECRET"`, webhook, 1),
		"missing token":       strings.Replace(strings.Replace(validJSON(), `"app_secret_ref":"env:APP_SECRET"`, `"app_secret_ref":"env:APP_SECRET","receive_mode":"webhook","encrypt_key_ref":"env:ENCRYPT"`, 1), `"database_path"`, `"webhook_addr":"127.0.0.1:8080","database_path"`, 1),
		"plaintext token":     strings.Replace(strings.Replace(validJSON(), `"app_secret_ref":"env:APP_SECRET"`, `"app_secret_ref":"env:APP_SECRET","receive_mode":"webhook","verification_token_ref":"raw","encrypt_key_ref":"env:ENCRYPT"`, 1), `"database_path"`, `"webhook_addr":"127.0.0.1:8080","database_path"`, 1),
		"addr without hook":   strings.Replace(validJSON(), `"database_path"`, `"webhook_addr":"127.0.0.1:8080","database_path"`, 1),
		"unknown mode":        strings.Replace(validJSON(), `"app_secret_ref":"env:APP_SECRET"`, `"app_secret_ref":"env:APP_SECRET","receive_mode":"poll"`, 1),
		"token on websocket":  strings.Replace(validJSON(), `"app_secret_ref":"env:APP_SECRET"`, `"app_secret_ref":"env:APP_SECRET","verification_token_ref":"env:VERIFY","encrypt_key_ref":"env:ENCRYPT"`, 1),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeConfig(t, data))
			require.Error(t, err)
		})
	}
}
