// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package oauth

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func fixture(t *testing.T, handler http.HandlerFunc) (*Client, *httptest.Server) {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	client, err := New(Config{AppID: "app", AppSecret: "secret-material", AccountsBaseURL: server.URL, HTTPClient: server.Client(), OpenBaseURL: server.URL, Clock: func() time.Time { return time.Unix(100, 0) }})
	require.NoError(t, err)
	return client, server
}

func TestBeginBasicFormAndFallback(t *testing.T) {
	client, _ := fixture(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "POST", r.Method)
		require.Equal(t, "/oauth/v1/device_authorization", r.URL.Path)
		id, secret, ok := r.BasicAuth()
		require.True(t, ok)
		require.Equal(t, "app", id)
		require.Equal(t, "secret-material", secret)
		require.NoError(t, r.ParseForm())
		require.Equal(t, "app", r.Form.Get("client_id"))
		require.Equal(t, "a offline_access", r.Form.Get("scope"))
		require.Empty(t, r.Form.Get("client_secret"))
		_, _ = io.WriteString(w, `{"device_code":"device-secret","user_code":"user-secret","verification_uri":"https://accounts.feishu.cn/device?secret=value","expires_in":600,"interval":5}`)
	})
	result, err := client.Begin(context.Background(), []string{"a", "offline_access"})
	require.NoError(t, err)
	require.Equal(t, "device-secret", result.DeviceCode)
	require.Equal(t, "user-secret", result.UserCode)
	require.Equal(t, time.Unix(700, 0), result.ExpiresAt)
	require.Equal(t, 5*time.Second, result.Interval)
	for _, value := range []any{result, &result, client, *client} {
		for _, format := range []string{"%v", "%+v", "%#v"} {
			output := fmt.Sprintf(format, value)
			for _, secret := range []string{"device-secret", "user-secret", "secret-material", "https://", "value"} {
				require.NotContains(t, output, secret)
			}
		}
	}
}

func TestPollSingleExchangeStatusesAndScopes(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     PollStatus
		scopeKnown bool
		refresh    string
	}{
		{"pending", `{"code":20094,"error":"authorization_pending"}`, PollPending, false, ""},
		{"slow", `{"code":20094,"error":"slow_down"}`, PollSlowDown, false, ""},
		{"denied", `{"error":"access_denied"}`, PollDenied, false, ""},
		{"expired", `{"error":"expired_token"}`, PollExpired, false, ""},
		{"invalid_grant", `{"error":"invalid_grant"}`, PollExpired, false, ""},
		{"success", `{"code":0,"access_token":"access-secret","refresh_token":"refresh-secret","expires_in":3600,"scope":"actual offline_access"}`, PollSucceeded, true, "refresh-secret"},
		{"no_scope_refresh", `{"access_token":"access-secret","expires_in":3600}`, PollSucceeded, false, ""},
		{"empty_scope", `{"access_token":"access-secret","expires_in":3600,"scope":""}`, PollSucceeded, true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			client, _ := fixture(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				require.Equal(t, "POST", r.Method)
				require.Equal(t, "/open-apis/authen/v2/oauth/token", r.URL.Path)
				require.Equal(t, "application/x-www-form-urlencoded", r.Header.Get("Content-Type"))
				require.NoError(t, r.ParseForm())
				require.Equal(t, "urn:ietf:params:oauth:grant-type:device_code", r.Form.Get("grant_type"))
				require.Equal(t, "device-secret", r.Form.Get("device_code"))
				require.Equal(t, "app", r.Form.Get("client_id"))
				require.Equal(t, "secret-material", r.Form.Get("client_secret"))
				_, _ = io.WriteString(w, tc.body)
			})
			result, err := client.Poll(context.Background(), "device-secret")
			require.NoError(t, err)
			require.Equal(t, tc.status, result.Status)
			require.EqualValues(t, 1, calls.Load())
			if tc.status == PollSucceeded {
				require.NotNil(t, result.Token)
				require.Equal(t, time.Unix(3700, 0), result.Token.ExpiresAt)
				require.Equal(t, tc.scopeKnown, result.Token.ScopeKnown)
				require.Equal(t, tc.refresh, result.Token.RefreshToken)
				if tc.name == "success" {
					require.Equal(t, []string{"actual", "offline_access"}, result.Token.GrantedScopes)
				}
				for _, format := range []string{"%v", "%+v", "%#v"} {
					require.NotContains(t, fmt.Sprintf(format, *result.Token), "secret")
					require.NotContains(t, fmt.Sprintf(format, result), "secret")
				}
			} else {
				require.Nil(t, result.Token)
			}
		})
	}
}

func TestRefreshAndIdentity(t *testing.T) {
	client, _ := fixture(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/open-apis/authen/v2/oauth/token":
			require.Equal(t, "POST", r.Method)
			require.NoError(t, r.ParseForm())
			require.Equal(t, "refresh_token", r.Form.Get("grant_type"))
			require.Equal(t, "refresh-secret", r.Form.Get("refresh_token"))
			require.Equal(t, "secret-material", r.Form.Get("client_secret"))
			_, _ = io.WriteString(w, `{"access_token":"new-secret","expires_in":60,"scope":"real"}`)
		case "/open-apis/authen/v1/user_info":
			require.Equal(t, "GET", r.Method)
			require.Equal(t, "Bearer access-secret", r.Header.Get("Authorization"))
			_, _ = io.WriteString(w, `{"code":0,"data":{"open_id":"user","tenant_key":"tenant","app_id":"untrusted"}}`)
		default:
			t.Errorf("unexpected path")
		}
	})
	token, err := client.Refresh(context.Background(), "refresh-secret")
	require.NoError(t, err)
	require.Empty(t, token.RefreshToken)
	require.Equal(t, []string{"real"}, token.GrantedScopes)
	identity, err := client.GetUserIdentity(context.Background(), "access-secret")
	require.NoError(t, err)
	require.Equal(t, Identity{AppID: "app", OpenID: "user", TenantKey: "tenant"}, identity)
}

func TestResponseBoundariesAndSafeErrors(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		kind       ErrorKind
	}{
		{"invalid_json", `access-secret`, 200, ErrorInvalidResponse}, {"array", `[]`, 200, ErrorInvalidResponse}, {"null", `null`, 200, ErrorInvalidResponse}, {"trailing", `{} {}`, 200, ErrorInvalidResponse},
		{"missing_token", `{"expires_in":60}`, 200, ErrorInvalidResponse}, {"bad_expiry", `{"access_token":"access-secret","expires_in":0}`, 200, ErrorInvalidResponse}, {"bad_scope", `{"access_token":"access-secret","expires_in":1,"scope":[]}`, 200, ErrorInvalidResponse},
		{"bad_code", `{"code":"0","access_token":"access-secret","expires_in":1}`, 200, ErrorInvalidResponse},
		{"big", strings.Repeat("x", (1<<20)+1), 200, ErrorInvalidResponse}, {"upstream", `{"msg":"access-secret https://private","error":"secret-material"}`, 503, ErrorUpstream},
		{"rejected", `{"code":20026,"error_description":"refresh-secret"}`, 400, ErrorReauthRequired},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, server := fixture(t, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			})
			_, err := client.Poll(context.Background(), "device-secret")
			var oe *Error
			require.ErrorAs(t, err, &oe)
			require.Equal(t, tc.kind, oe.Kind)
			if tc.kind == ErrorUpstream || tc.kind == ErrorInvalidResponse {
				require.Equal(t, OutcomeUnknown, oe.Outcome)
			}
			for _, format := range []string{"%v", "%+v", "%#v"} {
				s := fmt.Sprintf(format, err)
				for _, v := range []string{"access-secret", "refresh-secret", "secret-material", "device-secret", server.URL, "https://private"} {
					require.NotContains(t, s, v)
				}
			}
		})
	}
}

func TestBeginAndIdentityIncomplete(t *testing.T) {
	for _, body := range []string{`{}`, `{"device_code":"d","user_code":"u","verification_uri":"javascript:bad","expires_in":60,"interval":5}`, `{"device_code":"d","verification_uri":"https://example.com","expires_in":60,"interval":5}`, `{"device_code":"d","user_code":"u","verification_uri":"https://example.com","expires_in":-1,"interval":5}`} {
		client, _ := fixture(t, func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, body) })
		_, err := client.Begin(context.Background(), nil)
		require.Error(t, err)
	}
	for _, body := range []string{`{"code":0,"data":{"open_id":"user"}}`, `{"code":0,"data":{"tenant_key":"tenant"}}`, `{"code":0,"data":null}`} {
		client, _ := fixture(t, func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, body) })
		_, err := client.GetUserIdentity(context.Background(), "access-secret")
		require.Error(t, err)
	}
}

func TestNoRetryForDroppedResponseTimeoutCancellationOrRedirect(t *testing.T) {
	for _, mode := range []string{"drop", "timeout", "cancel", "redirect"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			client, server := fixture(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				switch mode {
				case "drop":
					conn, _, err := w.(http.Hijacker).Hijack()
					require.NoError(t, err)
					_ = conn.Close()
				case "timeout":
					select {
					case <-r.Context().Done():
					case <-time.After(150 * time.Millisecond):
					}
				case "redirect":
					http.Redirect(w, r, "/token-secret", http.StatusTemporaryRedirect)
				default:
					_, _ = io.WriteString(w, `{}`)
				}
			})
			ctx := context.Background()
			if mode == "timeout" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 20*time.Millisecond)
				defer cancel()
			}
			if mode == "cancel" {
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cancelled
			}
			_, err := client.Poll(ctx, "device-secret")
			var oe *Error
			require.ErrorAs(t, err, &oe)
			require.Equal(t, OutcomeUnknown, oe.Outcome)
			if mode == "cancel" {
				require.ErrorIs(t, err, context.Canceled)
				require.Zero(t, calls.Load())
			} else {
				require.EqualValues(t, 1, calls.Load())
			}
			require.NotContains(t, err.Error(), server.URL)
		})
	}
}

func TestConfigValidationAndDefaultTimeout(t *testing.T) {
	for _, base := range []string{"relative", "ftp://example.com", "https://user:secret@example.com", "https://example.com?secret=x", "https://example.com#secret", "https://"} {
		_, err := New(Config{AppID: "app", AppSecret: "secret-material", OpenBaseURL: base})
		require.Error(t, err)
		require.NotContains(t, err.Error(), base)
		require.NotContains(t, err.Error(), "secret-material")
	}
	_, err := New(Config{})
	require.Error(t, err)
	client, err := New(Config{AppID: "app", AppSecret: "secret-material"})
	require.NoError(t, err)
	require.Equal(t, 30*time.Second, client.httpClient.Timeout)
}

func TestBeginRejectsMalformedCompleteURLInsteadOfFallingBack(t *testing.T) {
	client, _ := fixture(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"device_code":"d","user_code":"u","verification_uri_complete":123,"verification_uri":"https://example.com/device","expires_in":60,"interval":5}`)
	})
	_, err := client.Begin(context.Background(), nil)
	require.Error(t, err)
}

func TestPollRejectsMalformedNumericCodeAlongsidePending(t *testing.T) {
	client, _ := fixture(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"code":"20094","error":"authorization_pending"}`)
	})
	_, err := client.Poll(context.Background(), "d")
	var failure *Error
	require.ErrorAs(t, err, &failure)
	require.Equal(t, ErrorInvalidResponse, failure.Kind)
}

func TestBodyReadCancellationKeepsSafeContextCause(t *testing.T) {
	for _, mode := range []string{"cancel", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			flushed := make(chan struct{})
			release := make(chan struct{})
			client, _ := fixture(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"access_token":"partial-secret`)
				w.(http.Flusher).Flush()
				close(flushed)
				select {
				case <-release:
				case <-r.Context().Done():
				}
			})
			reading := make(chan struct{})
			transport := client.httpClient.Transport
			client.httpClient.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
				response, err := transport.RoundTrip(request)
				if err == nil {
					response.Body = &observedReadCloser{ReadCloser: response.Body, started: reading}
				}
				return response, err
			})
			defer close(release)
			var ctx context.Context
			var cancel context.CancelFunc
			expected := context.Canceled
			if mode == "deadline" {
				ctx, cancel = context.WithTimeout(context.Background(), time.Second)
				expected = context.DeadlineExceeded
			} else {
				ctx, cancel = context.WithCancel(context.Background())
			}
			defer cancel()
			result := make(chan error, 1)
			go func() { _, err := client.Poll(ctx, "device-secret"); result <- err }()
			select {
			case <-flushed:
			case <-time.After(2 * time.Second):
				t.Fatal("response body did not flush")
			}
			select {
			case <-reading:
			case <-time.After(2 * time.Second):
				t.Fatal("response body was not read")
			}
			if mode == "cancel" {
				cancel()
			}
			var err error
			select {
			case err = <-result:
			case <-time.After(2 * time.Second):
				t.Fatal("request did not cancel")
			}
			var failure *Error
			require.ErrorAs(t, err, &failure)
			require.Equal(t, ErrorCancelled, failure.Kind)
			require.Equal(t, OutcomeUnknown, failure.Outcome)
			require.ErrorIs(t, err, expected)
			require.EqualValues(t, 1, calls.Load())
			require.NotContains(t, fmt.Sprintf("%#v", err), "partial-secret")
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

type observedReadCloser struct {
	io.ReadCloser
	started chan struct{}
	once    sync.Once
}

func (r *observedReadCloser) Read(buffer []byte) (int, error) {
	r.once.Do(func() { close(r.started) })
	return r.ReadCloser.Read(buffer)
}

func TestAmbiguousOrInvalidJSONNeverProvesOAuthResult(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		identity   bool
	}{
		{"duplicate_code", `{"code":20026,"code":0,"access_token":"synthetic","expires_in":60}`, false},
		{"duplicate_error", `{"error":"expired_token","error":"authorization_pending"}`, false},
		{"escaped_duplicate_key", `{"code":20026,"\u0063ode":0,"access_token":"synthetic","expires_in":60}`, false},
		{"nested_duplicate_identity", `{"code":0,"data":{"open_id":"first","open_id":"second","tenant_key":"tenant"}}`, true},
		{"nested_duplicate_extension", `{"access_token":"synthetic","expires_in":60,"extension":[{"id":1,"id":2}]}`, false},
		{"invalid_utf8_identity", "{\"code\":0,\"data\":{\"open_id\":\"user\xff\",\"tenant_key\":\"tenant\"}}", true},
		{"invalid_utf8_extension", "{\"access_token\":\"synthetic\",\"expires_in\":60,\"extension\":\"bad\xff\"}", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			client, _ := fixture(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); _, _ = io.WriteString(w, tc.body) })
			var err error
			if tc.identity {
				var identity Identity
				identity, err = client.GetUserIdentity(context.Background(), "access-secret")
				require.Equal(t, Identity{}, identity)
			} else {
				var result PollResult
				result, err = client.Poll(context.Background(), "device-secret")
				require.Nil(t, result.Token)
				require.Empty(t, result.Status)
			}
			var failure *Error
			require.ErrorAs(t, err, &failure)
			require.Equal(t, ErrorInvalidResponse, failure.Kind)
			require.Equal(t, OutcomeUnknown, failure.Outcome)
			require.EqualValues(t, 1, calls.Load())
			require.NotContains(t, fmt.Sprintf("%#v", err), "synthetic")
		})
	}
}

func TestUnknownJSONExtensionsRemainCompatible(t *testing.T) {
	client, _ := fixture(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"access_token":"synthetic","expires_in":60,"extension":{"list":[null,true,"text",{"id":1}],"huge":1e9999},"other":{"id":2}}`)
	})
	result, err := client.Poll(context.Background(), "device-secret")
	require.NoError(t, err)
	require.Equal(t, PollSucceeded, result.Status)
}
