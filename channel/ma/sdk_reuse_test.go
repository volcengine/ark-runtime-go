// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package ma_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/arkruntime"

	"github.com/volcengine/ark-runtime-go/channel/ma"
)

func TestChannelHonorsSDKRedirectBehavior(t *testing.T) {
	var calls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_, _ = io.WriteString(w, `{"id":"sesn-1","type":"session","status":"idle","environment_id":"env","agent":{},"created_at":"","updated_at":""}`)
	}))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, http.StatusFound) }))
	defer server.Close()
	sdk := arkruntime.NewClientWithApiKey("fixture", arkruntime.WithBaseUrl(server.URL+"/api/v3"), arkruntime.WithRetryTimes(0))
	client, err := ma.NewClient(server.URL, "", ma.ClientOptions{SDKClient: sdk})
	require.NoError(t, err)
	result, err := client.GetSession(context.Background(), "sesn-1")
	require.NoError(t, err)
	require.Equal(t, "sesn-1", result.ID)
	require.EqualValues(t, 1, calls.Load())
	require.Nil(t, sdk.HTTPClient().CheckRedirect)
}

func TestChannelHonorsSDKRetryBehavior(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `{"error":{"code":"Unavailable","message":"fixture"}}`)
			return
		}
		_, _ = io.WriteString(w, `{"id":"sesn-1","type":"session","status":"idle","environment_id":"env","agent":{},"created_at":"","updated_at":""}`)
	}))
	defer server.Close()
	sdk := arkruntime.NewClientWithApiKey("fixture", arkruntime.WithBaseUrl(server.URL+"/api/v3"), arkruntime.WithRetryTimes(1))
	client, err := ma.NewClient(server.URL, "", ma.ClientOptions{SDKClient: sdk})
	require.NoError(t, err)
	_, err = client.GetSession(context.Background(), "sesn-1")
	require.NoError(t, err)
	require.EqualValues(t, 2, calls.Load())
}
