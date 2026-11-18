// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package ma_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/arkruntime"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/ma"
)

type failedWriteTransport struct{ calls int }

func (f *failedWriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	f.calls++
	return &http.Response{
		StatusCode: http.StatusInternalServerError,
		Header:     http.Header{"Content-Type": {"application/json"}, "Retry-After-Ms": {"1"}},
		Body:       io.NopCloser(strings.NewReader(`{"error":{"code":"Unavailable","message":"fixture"}}`)),
		Request:    req,
	}, nil
}

func TestMAWriteRetryOwnership(t *testing.T) {
	operations := map[string]func(*ma.Client) error{
		"message": func(c *ma.Client) error {
			_, err := c.SendMessage(context.Background(), "session-1", "event-1", core.UserInput{Text: "hello"})
			return err
		},
		"vault": func(c *ma.Client) error {
			_, err := c.CreateVault(context.Background(), core.BotVaultCreate{Name: "creation-1", DisplayName: "fixture"})
			return err
		},
	}
	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			for _, injected := range []bool{false, true} {
				t.Run(map[bool]string{false: "owned_sdk", true: "caller_sdk"}[injected], func(t *testing.T) {
					transport := &failedWriteTransport{}
					httpClient := &http.Client{Transport: transport}
					options := ma.ClientOptions{HTTPClient: httpClient}
					wantCalls := 1
					if injected {
						options.SDKClient = arkruntime.NewClientWithApiKey("fixture", arkruntime.WithHTTPClient(httpClient), arkruntime.WithRetryTimes(1))
						wantCalls = 2
					}
					client, err := ma.NewClient("https://fixture.invalid", "fixture", options)
					require.NoError(t, err)
					require.Error(t, operation(client))
					require.Equal(t, wantCalls, transport.calls)
				})
			}
		})
	}
}
