// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package runner

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/secret"
	"github.com/volcengine/ark-runtime-go/channel/store/embedded"
)

func TestRunAdminListenFailureStopsRuntime(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { require.NoError(t, listener.Close()) }()
	config := testConfig(t)
	config.Observability.AdminAddr = listener.Addr().String()
	require.NoError(t, Install(t.Context(), config))
	t.Setenv("MA_KEY", "fixture-key")
	started := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- runWith(ctx, config, dependencies{
			Resolver: secret.EnvFileResolver{}, LogOutput: io.Discard,
			NewMA: func(MAConfig, string) (core.MAClient, error) { return fakeMA{}, nil },
			NewProvider: func(core.Binding, FeishuConfig, secret.Resolver) (core.Provider, error) {
				return blockingProvider{started: started}, nil
			},
		})
	}()
	select {
	case err := <-done:
		require.ErrorContains(t, err, "admin")
	case <-time.After(time.Second):
		cancel()
		<-done
		t.Fatal("admin listen failure did not stop the runner")
	}
	store, err := openTestEmbedded(config.DatabasePath, embedded.Options{})
	require.NoError(t, err, "runner must join components before releasing the store")
	require.NoError(t, store.Close())
}

func TestRunComponentsCancelsAndJoinsAfterUnexpectedExit(t *testing.T) {
	for _, failure := range []error{nil, errors.New("private downstream token")} {
		t.Run(map[bool]string{true: "unexpected_success", false: "error"}[failure == nil], func(t *testing.T) {
			started, stopped := make(chan struct{}), make(chan struct{})
			err := runComponents(context.Background(), []runnerComponent{
				{name: "runtime", run: func(ctx context.Context) error {
					close(started)
					<-ctx.Done()
					close(stopped)
					return ctx.Err()
				}},
				{name: "webhook", run: func(context.Context) error {
					<-started
					return failure
				}},
			})
			require.ErrorContains(t, err, "webhook")
			require.NotContains(t, err.Error(), "private downstream token")
			select {
			case <-stopped:
			default:
				t.Fatal("supervisor returned before runtime stopped")
			}
		})
	}
}
