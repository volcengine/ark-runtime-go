// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryDelayPreservesSubsecondBackoff(t *testing.T) {
	policy := RetryPolicy{InitialBackoff: 500 * time.Millisecond, MaxBackoff: 8 * time.Second}
	for i, bounds := range [][2]time.Duration{
		{375 * time.Millisecond, 500 * time.Millisecond},
		{750 * time.Millisecond, time.Second},
		{6 * time.Second, 8 * time.Second},
	} {
		retryCount := i
		if i == 2 {
			retryCount = 8
		}
		delay := retryDelay(policy, retryCount, errors.New("retry"))
		if delay < bounds[0] || delay > bounds[1] {
			t.Fatalf("retry %d delay %s outside [%s, %s]", retryCount, delay, bounds[0], bounds[1])
		}
	}
}

func TestRetryDelayPrefersServerValue(t *testing.T) {
	want := 125 * time.Millisecond
	policy := RetryPolicy{
		InitialBackoff: 500 * time.Millisecond,
		MaxBackoff:     8 * time.Second,
		RetryAfter: func(error) (time.Duration, bool) {
			return want, true
		},
	}
	if got := retryDelay(policy, 0, errors.New("retry")); got != want {
		t.Fatalf("retry delay = %s, want %s", got, want)
	}
}

func TestRetryDelayRejectsInvalidServerValues(t *testing.T) {
	for _, serverDelay := range []time.Duration{0, -time.Second, 61 * time.Second} {
		policy := RetryPolicy{
			InitialBackoff: 500 * time.Millisecond,
			MaxBackoff:     8 * time.Second,
			MaxRetryAfter:  60 * time.Second,
			RetryAfter: func(error) (time.Duration, bool) {
				return serverDelay, true
			},
		}
		got := retryDelay(policy, 0, errors.New("retry"))
		if got < 375*time.Millisecond || got > 500*time.Millisecond {
			t.Fatalf("server delay %s produced retry delay %s", serverDelay, got)
		}
	}
}

func TestRetryWithAttemptReportsRetryCount(t *testing.T) {
	var attempts []int
	err := RetryWithAttempt(
		context.Background(),
		RetryPolicy{
			MaxAttempts: 2,
			RetryAfter: func(error) (time.Duration, bool) {
				return 0, true
			},
		},
		func() bool { return true },
		func(retryCount int) error {
			attempts = append(attempts, retryCount)
			return errors.New("retry")
		},
		nil,
		func(error) bool { return true },
	)
	if err == nil {
		t.Fatal("expected final retry error")
	}
	if len(attempts) != 3 || attempts[0] != 0 || attempts[1] != 1 || attempts[2] != 2 {
		t.Fatalf("attempts = %v, want [0 1 2]", attempts)
	}
}
