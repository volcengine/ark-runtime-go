// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package ma_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/arkruntime"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/ma"
)

func TestSessionUsesSDKResponseValidation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Deliberately omit the public Session schema's type and timestamps.
		_, _ = io.WriteString(w, `{"id":"sesn-incomplete","status":"idle","environment_id":"env","agent":{"id":"agent","version":1}}`)
	}))
	defer server.Close()
	for _, injected := range []bool{false, true} {
		options := ma.ClientOptions{}
		if injected {
			options.SDKClient = arkruntime.NewClientWithApiKey("fixture", arkruntime.WithBaseUrl(server.URL+"/api/v3"))
		}
		client, err := ma.NewClient(server.URL, "fixture", options)
		require.NoError(t, err)
		_, err = client.GetSession(context.Background(), "sesn-incomplete")
		require.Error(t, err, "SDK validation must also apply to an injected client")
	}
}

func TestInjectedSDKFileRequestsStayOnSDKDestination(t *testing.T) {
	var wrongCalls atomic.Int32
	wrong := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		wrongCalls.Add(1)
		_, _ = io.WriteString(w, `{"id":"file-1","status":"active","download_url":"https://example.invalid/object","data":[],"has_more":false}`)
	}))
	defer wrong.Close()
	var paths []string
	correct := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		require.Equal(t, "Bearer sdk-fixture", r.Header.Get("Authorization"))
		if r.URL.Path == "/api/v3/files/file-1" {
			_, _ = io.WriteString(w, `{"id":"file-1","status":"active","download_url":"https://example.invalid/object"}`)
			return
		}
		_, _ = io.WriteString(w, `{"data":[{"id":"file-1","scope":{"type":"session","id":"sesn-1"},"status":"active","description":"/mnt/session/outputs/result.txt"}],"has_more":false}`)
	}))
	defer correct.Close()
	sdk := arkruntime.NewClientWithApiKey("sdk-fixture", arkruntime.WithBaseUrl(correct.URL+"/api/v3"))
	client, err := ma.NewClient(wrong.URL, "", ma.ClientOptions{SDKClient: sdk})
	require.NoError(t, err)
	_, err = client.ResolveFileURL(context.Background(), "file-1")
	require.NoError(t, err)
	files, err := client.ListSessionOutputFiles(context.Background(), "sesn-1")
	require.NoError(t, err)
	require.Zero(t, wrongCalls.Load(), "never combine SDK credentials with a different configured host")
	require.Len(t, files, 1)
	require.Equal(t, []string{"/api/v3/files/file-1", "/api/v3/files"}, paths)
}

func TestListEventsPreservesRejectedStatusAfterBodyDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(403)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	client, err := ma.NewClient(server.URL, "fixture", ma.ClientOptions{RequestTimeout: 30 * time.Millisecond})
	require.NoError(t, err)
	_, err = client.ListEvents(context.Background(), "sesn-1", core.EventQuery{})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	var api *ma.APIError
	require.ErrorAs(t, err, &api)
	require.Equal(t, 403, api.Status)
}

func TestNonReplayableUploadRetainsFinalUnauthorizedCode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(401)
		_, _ = io.WriteString(w, `{"error":{"code":"RejectedUpload","message":"fixture"}}`)
	}))
	defer server.Close()
	client, err := ma.NewClient(server.URL, "fixture", ma.ClientOptions{})
	require.NoError(t, err)
	_, err = client.UploadFile(context.Background(), core.FileUpload{Reader: bytes.NewBufferString("payload"), FileName: "fixture.txt"})
	var api *ma.APIError
	require.ErrorAs(t, err, &api)
	require.Equal(t, "RejectedUpload", api.Code)
}

func TestDotResourceIDsAreRejectedBeforeTransport(t *testing.T) {
	operations := map[string]func(*ma.Client) error{
		"environment": func(c *ma.Client) error { _, err := c.GetEnvironment(context.Background(), ".."); return err },
		"file":        func(c *ma.Client) error { _, err := c.ResolveFileURL(context.Background(), ".."); return err },
		"credential": func(c *ma.Client) error {
			_, err := c.UpsertEnvCredential(context.Background(), core.BotCredentialSpec{VaultID: "..", SecretName: "FIXTURE", AllowedHosts: []string{"example.com"}}, "synthetic")
			return err
		},
	}
	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			var requests atomic.Int32
			client, err := ma.NewClient("https://example.invalid", "fixture", ma.ClientOptions{HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				requests.Add(1)
				return &http.Response{StatusCode: 403, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
			})}})
			require.NoError(t, err)
			require.Error(t, operation(client))
			require.Zero(t, requests.Load())
		})
	}
}

func TestSDKResourceCreationPreservesMountPath(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		require.Equal(t, "fixture.txt", body["mount_path"])
		_, _ = io.WriteString(w, `{"type":"file","file_id":"registered-file-1","mount_path":"/mnt/session/uploads/fixture.txt"}`)
	}))
	defer server.Close()
	client, err := ma.NewClient(server.URL, "fixture", ma.ClientOptions{})
	require.NoError(t, err)
	_, err = client.CreateSessionResource(context.Background(), "sesn-1", "file-1", "fixture.txt")
	require.NoError(t, err)
}

func TestRejectedStatusSurvivesNilTransportHeaders(t *testing.T) {
	client, err := ma.NewClient("https://example.invalid", "fixture", ma.ClientOptions{HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 403, Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
	})}})
	require.NoError(t, err)
	_, err = client.GetSession(context.Background(), "sesn-1")
	var api *ma.APIError
	require.ErrorAs(t, err, &api)
	require.Equal(t, 403, api.Status)
}

func TestRequestErrorStatusDoesNotDependOnResponseHeaders(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  int
		failure string
	}{
		{"bad_request", 400, ""},
		{"unauthorized", 401, ""},
		{"server_error", 500, ""},
		{"network", 500, "eof"},
		{"timeout", 500, "timeout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			client, err := ma.NewClient("https://example.invalid", "fixture", ma.ClientOptions{RequestTimeout: 10 * time.Millisecond, HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				calls.Add(1)
				switch tc.failure {
				case "eof":
					return nil, errors.Join(io.EOF, errors.New("private-fixture"))
				case "timeout":
					<-r.Context().Done()
					return nil, r.Context().Err()
				default:
					return &http.Response{StatusCode: tc.status, Body: io.NopCloser(strings.NewReader("private-fixture")), Request: r}, nil
				}
			})}})
			require.NoError(t, err)
			_, err = client.SendMessage(context.Background(), "sesn-1", "input-1", core.UserInput{Text: "hello"})
			var api *ma.APIError
			require.ErrorAs(t, err, &api)
			require.Equal(t, tc.status, api.Status)
			require.NotContains(t, err.Error(), "private-fixture")
			require.EqualValues(t, 1, calls.Load(), "the adapter must not replay the write")
			if tc.status >= 500 {
				var unsent core.RequestNotSent
				require.False(t, errors.As(err, &unsent), "an ambiguous write must not be marked unsent")
			}
			if tc.failure == "timeout" {
				require.ErrorIs(t, err, context.DeadlineExceeded)
			}
		})
	}
}
