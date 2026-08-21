// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package arkruntime

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/volcengine/ark-runtime-go/arkruntime/model"
)

func TestHandleErrorResp(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		headers   http.Header
		request   *http.Request
		wantAPI   bool
		wantCode  string
		wantID    string
		wantInErr string
	}{
		{
			name:     "wrapped API error",
			body:     `{"error":{"code":"InvalidModel","message":"model not found","type":"invalid_request_error"}}`,
			headers:  http.Header{model.ServerRequestHeader: []string{"server-request-id"}},
			wantAPI:  true,
			wantCode: "InvalidModel",
			wantID:   "server-request-id",
		},
		{
			name:     "direct API error",
			body:     `{"code":"InvalidModel","message":"model not found","type":"invalid_request_error","request_id":"body-request-id"}`,
			headers:  http.Header{},
			wantAPI:  true,
			wantCode: "InvalidModel",
			wantID:   "body-request-id",
		},
		{
			name:      "nonstandard JSON body",
			body:      `{"detail":"model is invalid"}`,
			headers:   http.Header{model.ServerRequestHeader: []string{"server-request-id"}},
			wantID:    "server-request-id",
			wantInErr: `{"detail":"model is invalid"}`,
		},
		{
			name:      "plain text body",
			body:      "bad gateway",
			headers:   http.Header{},
			request:   requestWithClientID("client-request-id"),
			wantID:    "client-request-id",
			wantInErr: "bad gateway",
		},
		{
			name:      "empty body",
			body:      "",
			headers:   http.Header{},
			wantInErr: "unexpected error response: empty body",
		},
	}

	client := &Client{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := &http.Response{
				StatusCode: http.StatusBadRequest,
				Header:     tt.headers,
				Body:       io.NopCloser(strings.NewReader(tt.body)),
				Request:    tt.request,
			}
			err := client.handleErrorResp(resp)

			if tt.wantAPI {
				var apiErr *model.APIError
				if !errors.As(err, &apiErr) {
					t.Fatalf("error = %T, want *model.APIError", err)
				}
				if apiErr.Code != tt.wantCode || apiErr.RequestId != tt.wantID {
					t.Fatalf("API error = %#v, want code %q and request ID %q", apiErr, tt.wantCode, tt.wantID)
				}
				return
			}

			var requestErr *model.RequestError
			if !errors.As(err, &requestErr) {
				t.Fatalf("error = %T, want *model.RequestError", err)
			}
			if requestErr.Err == nil {
				t.Fatal("RequestError.Err is nil")
			}
			if requestErr.RequestId != tt.wantID {
				t.Fatalf("request ID = %q, want %q", requestErr.RequestId, tt.wantID)
			}
			if !strings.Contains(requestErr.Error(), tt.wantInErr) {
				t.Fatalf("error = %q, want it to contain %q", requestErr, tt.wantInErr)
			}
		})
	}
}

func requestWithClientID(requestID string) *http.Request {
	request, _ := http.NewRequest(http.MethodPost, "https://example.com", nil)
	request.Header.Set(model.ClientRequestHeader, requestID)
	return request
}
