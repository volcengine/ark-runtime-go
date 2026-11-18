// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package runner

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func adminContextOperations() map[string]func(context.Context, Config) error {
	return map[string]func(context.Context, Config) error{
		"reset": func(ctx context.Context, cfg Config) error {
			_, err := ResetConversation(ctx, cfg, "route:v1:cancel")
			return err
		},
		"retry": func(ctx context.Context, cfg Config) error { _, err := RetryDelivery(ctx, cfg, "cancel"); return err },
		"prune": func(ctx context.Context, cfg Config) error { _, err := Prune(ctx, cfg); return err },
	}
}

func TestAdminCancellationInterruptsDatabaseInitialization(t *testing.T) {
	for name, operation := range adminContextOperations() {
		t.Run(name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer func() { require.NoError(t, listener.Close()) }()
			accepted := make(chan net.Conn, 1)
			go func() {
				conn, err := listener.Accept()
				if err == nil {
					accepted <- conn
				}
			}()
			cfg := testConfig(t)
			cfg.Store.Driver, cfg.Store.DSNRef = "mysql", "env:ADMIN_CONTEXT_DSN"
			t.Setenv("ADMIN_CONTEXT_DSN", "user:secret@tcp("+listener.Addr().String()+")/channel?timeout=5s&readTimeout=5s")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- operation(ctx, cfg) }()
			var conn net.Conn
			select {
			case conn = <-accepted:
			case <-time.After(2 * time.Second):
				t.Fatal("admin did not reach database handshake")
			}
			defer func() { _ = conn.Close() }()
			cancel()
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				_ = conn.Close()
				<-done
				t.Fatal("caller cancellation did not interrupt database initialization")
			}
		})
	}
}

func TestAdminCancelledMaintenancePreservesCancellation(t *testing.T) {
	for name, operation := range adminContextOperations() {
		t.Run(name, func(t *testing.T) {
			cfg := testConfig(t)
			seedFailedDelivery(t, cfg)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			require.ErrorIs(t, operation(ctx, cfg), context.Canceled)
			status, err := Status(context.Background(), cfg)
			require.NoError(t, err, "cancelled maintenance must release the embedded writer")
			require.Len(t, status.Deliveries, 1)
			require.Equal(t, "failed", string(status.Deliveries[0].State))
		})
	}
}

type countingAdminResolver struct{ calls int }

func (r *countingAdminResolver) Resolve(string) (string, error) { r.calls++; return "unused", nil }

func TestDoctorCancelledContextDoesNotResolveSecretsOrProbe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	resolver := &countingAdminResolver{}
	checks := Doctor(ctx, testConfig(t), DoctorOptions{Resolver: resolver, Online: true})
	require.Zero(t, resolver.calls)
	require.NotEmpty(t, checks)
	require.False(t, checks[len(checks)-1].OK)
	require.Equal(t, "context", checks[len(checks)-1].Name)
}
