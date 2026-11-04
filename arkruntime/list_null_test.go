// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package arkruntime

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

type listResponseCase struct {
	name      string
	body      string
	count     int
	wantError bool
}

func TestListFilesNullResponse(t *testing.T) {
	const fileID = "file-1"
	const page = `"object":"file","first_id":"file-1","last_id":"file-1","has_more":true`
	cases := []listResponseCase{
		{name: "null", body: `null`},
		{name: "whitespace", body: " \n null \t"},
		{name: "invalid_whitespace", body: "\vnull", wantError: true},
		{name: "null_data", body: `{` + page + `,"data":null}`},
		{name: "null_data_offsets", body: `{` + page + `,"extra":"中文","d\u0061ta":
 null}`},
		{name: "empty", body: `{` + page + `,"data":[]}`},
		{name: "populated", body: `{` + page + `,"data":[{"id":"file-1","object":"file","bytes":1,"created_at":1,"filename":"test.txt","purpose":"user_data","status":"processed","expire_at":0}]}`, count: 1},
		{name: "invalid", body: `{` + page + `,"data":"invalid"}`, wantError: true},
		{name: "invalid_duplicate_data", body: `{` + page + `,"data":"invalid","data":null}`, wantError: true},
		{name: "invalid_duplicate_pagination", body: `{` + page + `,"data":null,"has_more":"invalid","has_more":true}`, wantError: true},
		{name: "valid_duplicate_data", body: `{` + page + `,"data":[],"data":[]}`},
		{name: "missing", body: `{` + page + `}`, wantError: true},
		{name: "wrong_kind", body: `[]`, wantError: true},
	}
	runListResponseCases(t, "/files", cases, func(t *testing.T, c *Client, tc listResponseCase) (int, error) {
		response, err := c.ListFiles(context.Background(), nil)
		if err != nil {
			return 0, err
		}
		if response == nil || response.Data == nil {
			t.Fatal("expected non-nil response and list")
		}
		if tc.name == "null" || tc.name == "whitespace" {
			if response.HasMore || response.FirstID != "" || response.LastID != "" {
				t.Fatalf("empty page has cursors: %+v", response)
			}
		} else if response.FirstID != fileID || response.LastID != fileID || !response.HasMore {
			t.Fatalf("pagination changed: %+v", response)
		}
		if len(response.Data) == 1 && response.Data[0].ID != fileID {
			t.Fatal("file lost")
		}
		return len(response.Data), nil
	})
}

func TestListSessionResourcesNullResponse(t *testing.T) {
	cases := []listResponseCase{
		{name: "null", body: `null`},
		{name: "whitespace", body: " \n null \t"},
		{name: "invalid_whitespace", body: "\vnull", wantError: true},
		{name: "null_data", body: `{"data":null}`},
		{name: "null_data_offsets", body: `{"extra":"中文","d\u0061ta":
 null}`},
		{name: "empty", body: `{"data":[]}`},
		{name: "populated", body: `{"data":[{"id":"resource-1","type":"file","file_id":"file-1"}]}`, count: 1},
		{name: "invalid", body: `{"data":"invalid"}`, wantError: true},
		{name: "invalid_duplicate_data", body: `{"data":"invalid","data":null}`, wantError: true},
		{name: "valid_duplicate_data", body: `{"data":[],"data":[]}`},
		{name: "missing", body: `{}`, wantError: true},
		{name: "wrong_kind", body: `[]`, wantError: true},
	}
	runListResponseCases(t, "/sessions/session-1/resources", cases, func(t *testing.T, c *Client, _ listResponseCase) (int, error) {
		response, err := c.ListSessionResources(context.Background(), "session-1")
		if err != nil {
			return 0, err
		}
		if response == nil || response.Data == nil {
			t.Fatal("expected non-nil response and list")
		}
		if len(response.Data) == 1 && response.Data[0].ID.Value != "resource-1" {
			t.Fatal("resource lost")
		}
		return len(response.Data), nil
	})
}

func runListResponseCases(t *testing.T, path string, cases []listResponseCase, call func(*testing.T, *Client, listResponseCase) (int, error)) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := listResponseServer(t, path, tc.body)
			defer server.Close()
			client := NewClientWithApiKey("test", WithBaseUrl(server.URL))
			count, err := call(t, client, tc)
			if (err != nil) != tc.wantError {
				t.Fatalf("error = %v, wantError = %v", err, tc.wantError)
			}
			if count != tc.count {
				t.Fatalf("got %d items, want %d", count, tc.count)
			}
		})
	}
}

func listResponseServer(t *testing.T, path, body string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != path {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, body)
	}))
}
