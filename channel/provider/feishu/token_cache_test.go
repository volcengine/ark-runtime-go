// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestBotTokenCacheSharedWithAttachment(t *testing.T) {
	var calls atomic.Int32
	p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal") {
			calls.Add(1)
			tokenResponse(w)
			return
		}
		_, _ = io.WriteString(w, "attachment")
	})
	_, firstExpiry, err := p.BotToken(context.Background())
	require.NoError(t, err)
	_, secondExpiry, err := p.BotToken(context.Background())
	require.NoError(t, err)
	require.Equal(t, firstExpiry, secondExpiry)
	attachment, err := p.FetchAttachment(context.Background(), core.Message{MessageID: "message"}, core.Attachment{Kind: "file", Key: "file"})
	require.NoError(t, err)
	require.NoError(t, attachment.Body.Close())
	require.EqualValues(t, 1, calls.Load())
}

func TestBotTokenConcurrentRefreshWaitCanCancel(t *testing.T) {
	var calls atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	var enterOnce sync.Once
	p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		enterOnce.Do(func() { close(entered) })
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		tokenResponse(w)
	})
	leader := make(chan error, 1)
	go func() { _, _, err := p.BotToken(context.Background()); leader <- err }()
	<-entered
	ctx, cancel := context.WithCancel(context.Background())
	waiter := make(chan error, 1)
	go func() { _, _, err := p.BotToken(ctx); waiter <- err }()
	cancel()
	select {
	case err := <-waiter:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Error("canceled waiter is blocked by another refresh")
	}
	results := make(chan error, 12)
	for range 12 {
		go func() { _, _, err := p.BotToken(context.Background()); results <- err }()
	}
	close(release)
	require.NoError(t, <-leader)
	for range 12 {
		require.NoError(t, <-results)
	}
	require.EqualValues(t, 1, calls.Load())
}

func TestBotTokenRefreshesEarlyWithoutChangingActualExpiry(t *testing.T) {
	var calls atomic.Int32
	p := newFixtureProvider(t, func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		tokenResponse(w)
	})
	_, expiry, err := p.BotToken(context.Background())
	require.NoError(t, err)
	p.tenantTokens.mu.Lock()
	refreshAt := p.tenantTokens.refreshAt
	p.tenantTokens.refreshAt = time.Now().Add(-time.Second)
	p.tenantTokens.mu.Unlock()
	require.WithinDuration(t, expiry.Add(-5*time.Minute), refreshAt, time.Second)
	require.WithinDuration(t, time.Now().Add(2*time.Hour), expiry, time.Second)
	_, _, err = p.BotToken(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 2, calls.Load(), "refresh the token while its real expiry is still in the future")
}

func TestBotTokenCanceledRefreshDoesNotBlockNextCaller(t *testing.T) {
	var calls atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			close(entered)
			select {
			case <-r.Context().Done():
			case <-release:
			}
			return
		}
		tokenResponse(w)
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, _, err := p.BotToken(ctx); done <- err }()
	<-entered
	cancel()
	require.Error(t, <-done)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	token, _, err := p.BotToken(ctx)
	require.NoError(t, err)
	require.Equal(t, "fixture-token", token)
	require.EqualValues(t, 2, calls.Load())
}

func TestBotTokenRejectsInvalidExpiryWithoutCaching(t *testing.T) {
	for _, expiry := range []string{"0", "-1", "9223372036854775807"} {
		t.Run(expiry, func(t *testing.T) {
			var calls atomic.Int32
			p := newFixtureProvider(t, func(w http.ResponseWriter, _ *http.Request) {
				if calls.Add(1) == 1 {
					_, _ = io.WriteString(w, `{"code":0,"tenant_access_token":"invalid","expire":`+expiry+`}`)
					return
				}
				tokenResponse(w)
			})
			_, _, err := p.BotToken(context.Background())
			require.Error(t, err)
			token, _, err := p.BotToken(context.Background())
			require.NoError(t, err)
			require.Equal(t, "fixture-token", token)
			require.EqualValues(t, 2, calls.Load())
		})
	}
}
