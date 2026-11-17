// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

type fixedSecret string

func (f fixedSecret) Resolve(string) (string, error) { return string(f), nil }

func newFixtureProvider(t *testing.T, handler http.HandlerFunc) *Provider {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	p, err := NewProvider(ProviderConfig{
		Binding:      core.Binding{ID: "binding-1", AppID: "app-1"},
		AppSecretRef: "env:FIXTURE_SECRET", SecretResolver: fixedSecret("fixture-secret"),
		OpenBaseURL: server.URL,
	})
	require.NoError(t, err)
	return p
}

func tokenResponse(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, `{"code":0,"msg":"ok","tenant_access_token":"fixture-token","expire":7200}`)
}

func TestDeliveryTargetAndUUID(t *testing.T) {
	for _, tc := range []struct {
		mode   string
		path   string
		thread bool
	}{
		{"chat", "/open-apis/im/v1/messages", false},
		{"reply", "/open-apis/im/v1/messages/om-source/reply", false},
		{"thread", "/open-apis/im/v1/messages/om-source/reply", true},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			calls := 0
			p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal") {
					tokenResponse(w)
					return
				}
				calls++
				require.Equal(t, tc.path, r.URL.Path)
				require.Equal(t, http.MethodPost, r.Method)
				require.Equal(t, "Bearer fixture-token", r.Header.Get("Authorization"))
				var body map[string]any
				require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
				require.Equal(t, "uuid-stable", body["uuid"])
				require.Equal(t, "text", body["msg_type"])
				require.JSONEq(t, `{"text":"hello"}`, body["content"].(string))
				if tc.mode == "chat" {
					require.Equal(t, "chat_id", r.URL.Query().Get("receive_id_type"))
					require.Equal(t, "oc-target", body["receive_id"])
				} else if tc.thread {
					require.Equal(t, true, body["reply_in_thread"])
				} else {
					_, present := body["reply_in_thread"]
					require.False(t, present)
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"code":0,"data":{"message_id":"om-created"}}`)
			})
			id, err := p.Deliver(context.Background(), core.Delivery{
				UUID: "uuid-stable", Body: core.TextBody("hello"), ReplyTarget: core.ReplyTarget{Mode: tc.mode, ChatID: "oc-target", MessageID: "om-source"},
			})
			require.NoError(t, err)
			require.Equal(t, "om-created", id)
			require.Equal(t, 1, calls)
		})
	}
}

func TestDeliveryRejectAndUnknown(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		body      string
		uncertain bool
		retryable bool
	}{
		{"nonzero code", 200, `{"code":230001,"msg":"wire-secret"}`, false, false},
		{"plain rate limit", 429, "wire-secret", false, true},
		{"empty rate limit", 429, "", false, true},
		{"plain server failure", 503, "wire-secret", true, false},
		{"missing data", 200, `{"code":0,"data":null}`, true, false},
		{"server error", 500, `{"code":500,"msg":"wire-secret"}`, true, false},
		{"redirect", 307, `{"code":0,"data":{"message_id":"misleading"}}`, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal") {
					tokenResponse(w)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			})
			id, err := p.Deliver(context.Background(), core.Delivery{UUID: "uuid-stable", Body: core.TextBody("hello"), ReplyTarget: core.ReplyTarget{Mode: "chat", ChatID: "oc"}})
			require.Empty(t, id)
			require.Error(t, err)
			var failure core.DeliveryFailure
			require.True(t, errors.As(err, &failure))
			require.Equal(t, tc.uncertain, failure.Uncertain())
			require.Equal(t, tc.retryable, failure.Retryable())
			require.NotContains(t, err.Error(), "wire-secret")
		})
	}
}

type replyThrottleTransport struct {
	io.Reader
	closed bool
}

func (r *replyThrottleTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	response := &http.Response{Request: req, Header: make(http.Header)}
	if strings.HasSuffix(req.URL.Path, "/tenant_access_token/internal") {
		response.StatusCode = http.StatusOK
		response.Header.Set("Content-Type", "application/json")
		response.Body = io.NopCloser(strings.NewReader(`{"code":0,"tenant_access_token":"token","expire":7200}`))
	} else {
		response.StatusCode = http.StatusTooManyRequests
		response.Header.Set("Content-Type", "text/plain")
		response.Body = r
	}
	return response, nil
}

func (r *replyThrottleTransport) Close() error {
	r.closed = true
	return nil
}

func TestReplyClosesThrottledResponse(t *testing.T) {
	transport := &replyThrottleTransport{Reader: strings.NewReader("wire-secret")}
	p, err := NewProvider(ProviderConfig{
		Binding:      core.Binding{ID: "binding", AppID: "throttled-reply"},
		AppSecretRef: "env:FIXTURE", SecretResolver: fixedSecret("fixture"),
		HTTPClient: &http.Client{Transport: transport},
	})
	require.NoError(t, err)
	_, err = p.Deliver(t.Context(), core.Delivery{
		UUID: "uuid", Body: core.TextBody("reply"),
		ReplyTarget: core.ReplyTarget{Mode: "reply", MessageID: "om-source"},
	})
	var rejected core.DeliveryFailure
	require.ErrorAs(t, err, &rejected)
	require.True(t, rejected.Retryable())
	require.False(t, rejected.Uncertain())
	require.True(t, transport.closed)
	require.NotContains(t, err.Error(), "wire-secret")
}

func TestBotIdentityChecksStatusCodeAndOpenID(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		ok     bool
	}{
		{"success", 200, `{"code":0,"bot":{"open_id":"ou-bot"}}`, true},
		{"missing id", 200, `{"code":0,"bot":{}}`, false},
		{"code", 200, `{"code":1,"bot":{"open_id":"ou-bot"}}`, false},
		{"status", 502, `{"code":0,"bot":{"open_id":"ou-bot"}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal") {
					tokenResponse(w)
					return
				}
				require.Equal(t, "/open-apis/bot/v3/info", r.URL.Path)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			})
			id, err := p.getBotOpenID(context.Background())
			if tc.ok {
				require.NoError(t, err)
				require.Equal(t, "ou-bot", id)
			} else {
				require.Error(t, err)
				require.Empty(t, id)
			}
		})
	}
}

func TestInjectedHTTPClientIsClonedAndRedirectIsNotFollowed(t *testing.T) {
	redirects := 0
	caller := &http.Client{Timeout: 0}
	caller.CheckRedirect = func(*http.Request, []*http.Request) error { return nil }
	bounded := boundedClient(caller)
	require.Zero(t, caller.Timeout)
	require.NotNil(t, caller.CheckRedirect)
	require.Equal(t, requestTimeout, bounded.Timeout)
	require.NotSame(t, caller, bounded)
	// 客户端构造时同样应使用受限副本。
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal") {
			tokenResponse(w)
			return
		}
		redirects++
		if r.URL.Path == "/redirected" {
			t.Error("redirect followed")
			return
		}
		w.Header().Set("Location", "/redirected")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTemporaryRedirect)
		_, _ = io.WriteString(w, `{"code":0,"data":{"message_id":"misleading"}}`)
	}))
	t.Cleanup(server.Close)
	p, err := NewProvider(ProviderConfig{Binding: core.Binding{ID: "b", AppID: "app"}, AppSecretRef: "env:FIXTURE", SecretResolver: fixedSecret("s"), HTTPClient: caller, OpenBaseURL: server.URL})
	require.NoError(t, err)
	_, err = p.Deliver(context.Background(), core.Delivery{UUID: "u", Body: core.TextBody("hi"), ReplyTarget: core.ReplyTarget{Mode: "chat", ChatID: "oc"}})
	require.Error(t, err)
	var deliveryFailure core.DeliveryFailure
	require.ErrorAs(t, err, &deliveryFailure)
	require.True(t, deliveryFailure.Uncertain())
	require.Equal(t, 1, redirects)
}

func TestInitializeBotWithoutStartingReceiver(t *testing.T) {
	var calls atomic.Int32
	p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal") {
			tokenResponse(w)
			return
		}
		calls.Add(1)
		_, _ = io.WriteString(w, `{"code":0,"bot":{"open_id":"ou-bot"}}`)
	})
	for range 2 {
		id, err := p.InitializeBot(context.Background())
		require.NoError(t, err)
		require.Equal(t, "ou-bot", id)
	}
	require.Equal(t, int32(1), calls.Load())
}
