// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func startWebhookRun(p *Provider, ctx context.Context, messages chan<- core.Message) <-chan error {
	done := make(chan error, 1)
	go func() {
		defer func() {
			if value := recover(); value != nil {
				done <- fmt.Errorf("receiver panic: %v", value)
			}
		}()
		done <- p.Start(ctx, func(_ context.Context, message core.Message) error { messages <- message; return nil })
	}()
	return done
}

func TestWebhookProviderCanRestartAndRejectsConcurrentStart(t *testing.T) {
	p := newFixtureProvider(t, func(w http.ResponseWriter, _ *http.Request) { tokenResponse(w) })
	p.receiveMode, p.identityReady = ReceiveWebhook, true
	p.verificationToken, p.encryptKey = "verify-token", "encrypt-key-fixture"
	ready := p.Ready()
	for range 2 {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		messages := make(chan core.Message, 1)
		done := startWebhookRun(p, ctx, messages)
		require.Eventually(t, func() bool { return p.webhook.Load() != nil }, time.Second, time.Millisecond)
		secondCtx, cancelSecond := context.WithCancel(context.Background())
		defer cancelSecond()
		secondDone := startWebhookRun(p, secondCtx, make(chan core.Message, 1))
		select {
		case err := <-secondDone:
			require.Error(t, err)
			require.NotContains(t, err.Error(), "panic")
		case <-time.After(time.Second):
			t.Error("concurrent Start did not reject promptly")
		}
		cancelSecond()
		require.Equal(t, ready, p.Ready())
		require.Equal(t, http.StatusOK, signedPost(t, p, messageEvent("verify-token")).Code)
		select {
		case <-messages:
		case <-time.After(time.Second):
			t.Error("active receiver lost its accept handler")
		}
		cancel()
		require.NoError(t, <-done)
		require.Equal(t, http.StatusServiceUnavailable, post(t, p.WebhookHandler(), nil, nil).Code)
	}
}

func TestProviderFailedInitializationCanRetry(t *testing.T) {
	allowIdentity := false
	p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/open-apis/auth/v3/tenant_access_token/internal" {
			tokenResponse(w)
			return
		}
		if allowIdentity {
			_, _ = fmt.Fprint(w, `{"code":0,"bot":{"open_id":"ou-bot"}}`)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	p.receiveMode = ReceiveWebhook
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.Error(t, <-startWebhookRun(p, ctx, make(chan core.Message, 1)))
	require.Nil(t, p.webhook.Load())
	select {
	case <-p.Ready():
		t.Fatal("failed initialization must not signal readiness")
	default:
	}
	allowIdentity = true
	done := startWebhookRun(p, ctx, make(chan core.Message, 1))
	select {
	case <-p.Ready():
	case <-time.After(time.Second):
		t.Error("initialization could not retry")
	}
	cancel()
	require.NoError(t, <-done)
	require.Nil(t, p.webhook.Load())
}
