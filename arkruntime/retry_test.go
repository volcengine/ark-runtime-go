// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package arkruntime

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/volcengine/ark-runtime-go/arkruntime/model"
)

func TestRetryAfterPrefersMilliseconds(t *testing.T) {
	err := &model.APIError{
		ResponseHeader: http.Header{
			model.RetryAfterMSHeader: []string{"125.5"},
			model.RetryAfterHeader:   []string{"9"},
		},
	}
	delay, ok := retryAfter(err)
	if !ok || delay != 125500*time.Microsecond {
		t.Fatalf("retryAfter() = %s, %v; want 125.5ms, true", delay, ok)
	}
}

func TestRetryAfterParsing(t *testing.T) {
	future := time.Now().Add(2 * time.Second).UTC().Format(http.TimeFormat)
	tests := []struct {
		name   string
		header http.Header
		wantOK bool
		check  func(time.Duration) bool
	}{
		{
			name:   "invalid milliseconds falls back to seconds",
			header: http.Header{model.RetryAfterMSHeader: []string{"bad"}, model.RetryAfterHeader: []string{"0.25"}},
			wantOK: true,
			check:  func(got time.Duration) bool { return got == 250*time.Millisecond },
		},
		{
			name:   "http date",
			header: http.Header{model.RetryAfterHeader: []string{future}},
			wantOK: true,
			check:  func(got time.Duration) bool { return got > 0 && got <= 2*time.Second },
		},
		{
			name:   "negative",
			header: http.Header{model.RetryAfterHeader: []string{"-5"}},
			wantOK: true,
			check:  func(got time.Duration) bool { return got == -5*time.Second },
		},
		{
			name:   "non finite",
			header: http.Header{model.RetryAfterHeader: []string{fmt.Sprint(math.Inf(1))}},
			wantOK: false,
			check:  func(time.Duration) bool { return true },
		},
		{
			name:   "missing",
			header: http.Header{},
			wantOK: false,
			check:  func(time.Duration) bool { return true },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			delay, ok := retryAfter(&model.APIError{ResponseHeader: test.header})
			if ok != test.wantOK || !test.check(delay) {
				t.Fatalf("retryAfter() = %s, %v", delay, ok)
			}
		})
	}
}

func TestNeedRetryErrorHonorsServerOverride(t *testing.T) {
	err := &model.RequestError{
		HTTPStatusCode: http.StatusBadRequest,
		Err:            errors.New("bad request"),
		ResponseHeader: http.Header{model.ShouldRetryHeader: []string{"true"}},
	}
	if !needRetryError(err) {
		t.Fatal("expected X-Should-Retry=true to force a retry")
	}
	err.ResponseHeader.Set(model.ShouldRetryHeader, "false")
	err.HTTPStatusCode = http.StatusInternalServerError
	if needRetryError(err) {
		t.Fatal("expected X-Should-Retry=false to prevent a retry")
	}
}

func TestDoPreservesCustomRetryCount(t *testing.T) {
	var retryCounts []string
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		retryCounts = append(retryCounts, r.Header.Get(model.RetryCountHeader))
		calls++
		if calls == 1 {
			w.Header().Set(model.RetryAfterMSHeader, "1")
			http.Error(w, "retry", http.StatusTooManyRequests)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	client := NewClientWithApiKey("placeholder", WithHTTPClient(server.Client()), WithRetryTimes(1))
	err := client.Do(
		context.Background(), http.MethodGet, server.URL, "", "", nil,
		WithCustomHeader(model.RetryCountHeader, "custom"),
	)
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	if want := "[custom custom]"; fmt.Sprint(retryCounts) != want {
		t.Fatalf("retry counts = %v, want %s", retryCounts, want)
	}
}

func TestRetryableStatuses(t *testing.T) {
	for _, status := range []int{
		http.StatusRequestTimeout,
		http.StatusConflict,
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
	} {
		if !isRetryableStatus(status) {
			t.Fatalf("status %d should be retryable", status)
		}
	}
	if isRetryableStatus(http.StatusBadRequest) {
		t.Fatal("400 bad request should not be retried without an explicit server override")
	}
}

func TestDoUsesServerDelayAndIncrementsRetryCount(t *testing.T) {
	var retryCounts []string
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		retryCounts = append(retryCounts, r.Header.Get(model.RetryCountHeader))
		calls++
		if calls == 1 {
			w.Header().Set(model.RetryAfterMSHeader, "1")
			http.Error(w, "rate limited", http.StatusTooManyRequests)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	client := NewClientWithApiKey(
		"placeholder",
		WithHTTPClient(server.Client()),
		WithRetryTimes(2),
	)
	if err := client.Do(context.Background(), http.MethodGet, server.URL, "", "", nil); err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	if len(retryCounts) != 2 || retryCounts[0] != "0" || retryCounts[1] != "1" {
		t.Fatalf("retry counts = %v, want [0 1]", retryCounts)
	}
}
