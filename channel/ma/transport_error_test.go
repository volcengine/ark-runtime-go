// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package ma_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/ma"
)

type transportErrorFunc func(*http.Request) (*http.Response, error)

func (f transportErrorFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestTransportFailureKeepsCallerCancellationDistinct(t *testing.T) {
	for _, tc := range []struct {
		name    string
		ctx     func() (context.Context, context.CancelFunc)
		trip    transportErrorFunc
		timeout time.Duration
		want    error
	}{
		{
			name: "ordinary network failure",
			ctx:  func() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) },
			trip: func(*http.Request) (*http.Response, error) {
				return nil, errors.New("transport leaked fixture-key and private body")
			},
		},
		{
			name: "caller canceled",
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, cancel
			},
			trip: func(req *http.Request) (*http.Response, error) {
				<-req.Context().Done()
				return nil, req.Context().Err()
			},
			want: context.Canceled,
		},
		{
			name: "request deadline",
			ctx:  func() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) },
			trip: func(req *http.Request) (*http.Response, error) {
				<-req.Context().Done()
				return nil, req.Context().Err()
			},
			timeout: 10 * time.Millisecond,
			want:    context.DeadlineExceeded,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := tc.ctx()
			defer cancel()
			client, err := ma.NewClient("https://fixture.invalid", "fixture-key", ma.ClientOptions{HTTPClient: &http.Client{Transport: tc.trip}, RequestTimeout: tc.timeout})
			require.NoError(t, err)
			_, err = client.GetSession(ctx, "sesn-demo")
			if tc.want != nil {
				require.ErrorIs(t, err, tc.want)
				return
			}
			var api *ma.APIError
			require.ErrorAs(t, err, &api)
			require.Equal(t, http.StatusInternalServerError, api.Status, "the SDK synthesizes 500 for transport failures")
			require.NoError(t, ctx.Err())
			require.False(t, errors.Is(err, context.Canceled))
			require.False(t, errors.Is(err, context.DeadlineExceeded))
			require.False(t, strings.Contains(err.Error(), "fixture-key"))
			require.False(t, strings.Contains(err.Error(), "private body"))
		})
	}
}
