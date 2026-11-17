// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package ma_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/ma"
)

func TestStaticKeyUnauthorizedNotRetried(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer static-key") {
			t.Error("static key missing")
		}
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()
	client, err := ma.NewClient(server.URL, "static-key", ma.ClientOptions{})
	require.NoError(t, err)
	_, err = client.GetSession(context.Background(), "s1")
	var apiErr *ma.APIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, 401, apiErr.Status)
	require.Equal(t, int32(1), calls.Load())
}

func TestRequestBudgetRemainsDetectableWhileReadingResponse(t *testing.T) {
	for _, kind := range []string{"json", "sse", "upload"} {
		t.Run(kind, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.WriteHeader(http.StatusOK)
				w.(http.Flusher).Flush()
				<-r.Context().Done()
			}))
			defer server.Close()
			client, err := ma.NewClient(server.URL, "static", ma.ClientOptions{RequestTimeout: 50 * time.Millisecond, StreamTimeout: 50 * time.Millisecond, UploadTimeout: 50 * time.Millisecond})
			require.NoError(t, err)
			switch kind {
			case "json":
				_, err = client.GetSession(context.Background(), "s1")
			case "upload":
				_, err = client.UploadFile(context.Background(), core.FileUpload{Reader: strings.NewReader("x"), FileName: "x"})
			case "sse":
				var stream core.EventStream
				stream, err = client.StreamEvents(context.Background(), "s1")
				require.NoError(t, err)
				defer func() { _ = stream.Close() }()
				_, err = stream.Recv(context.Background())
			}
			require.ErrorIs(t, err, context.DeadlineExceeded)
		})
	}
}

func TestResponseBudgetUsesSDKRequestWithCustomTransport(t *testing.T) {
	for _, responseRequest := range []string{"nil", "unrelated"} {
		for _, kind := range []string{"json", "sse", "upload"} {
			t.Run(responseRequest+"/"+kind, func(t *testing.T) {
				client, err := ma.NewClient("http://example.invalid", "static", ma.ClientOptions{RequestTimeout: 30 * time.Millisecond, StreamTimeout: 30 * time.Millisecond, UploadTimeout: 30 * time.Millisecond, HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					response := &http.Response{StatusCode: 200, Header: make(http.Header), Body: &deadlineResponseBody{ctx: r.Context()}}
					if responseRequest == "unrelated" {
						response.Request = &http.Request{}
					}
					return response, nil
				})}})
				require.NoError(t, err)
				switch kind {
				case "json":
					_, err = client.GetSession(context.Background(), "s1")
				case "upload":
					_, err = client.UploadFile(context.Background(), core.FileUpload{Reader: strings.NewReader("x"), FileName: "x"})
				case "sse":
					var stream core.EventStream
					stream, err = client.StreamEvents(context.Background(), "s1")
					require.NoError(t, err)
					defer func() { _ = stream.Close() }()
					_, err = stream.Recv(context.Background())
				}
				require.ErrorIs(t, err, context.DeadlineExceeded)
			})
		}
	}
}

type deadlineResponseBody struct{ ctx context.Context }

func (b *deadlineResponseBody) Read([]byte) (int, error) {
	<-b.ctx.Done()
	return 0, errors.New("response read interrupted")
}
func (b *deadlineResponseBody) Close() error { return nil }

func TestRejectedHTTPStatusSurvivesErrorBodyCancellation(t *testing.T) {
	for _, status := range []int{400, 403} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			started := make(chan struct{})
			client, err := ma.NewClient("http://example.invalid", "static", ma.ClientOptions{HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: status, Header: http.Header{"X-Request-Id": {"known-canceled"}}, Body: &signaledDeadlineBody{deadlineResponseBody: deadlineResponseBody{ctx: r.Context()}, started: started}}, nil
			})}})
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, e := client.GetSession(ctx, "s1"); done <- e }()
			<-started
			cancel()
			err = <-done
			require.ErrorIs(t, err, context.Canceled)
			var apiErr *ma.APIError
			require.ErrorAs(t, err, &apiErr)
			require.Equal(t, status, apiErr.Status)
			require.Equal(t, "known-canceled", apiErr.RequestID)
		})
	}
}

type signaledDeadlineBody struct {
	deadlineResponseBody
	started chan struct{}
}

func (b *signaledDeadlineBody) Read(p []byte) (int, error) {
	close(b.started)
	return b.deadlineResponseBody.Read(p)
}
