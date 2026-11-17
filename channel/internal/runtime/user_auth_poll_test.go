// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUserAuthPollIntervalOption(t *testing.T) {
	options, err := (RuntimeOptions{}).Normalize()
	require.NoError(t, err)
	require.Equal(t, 2*time.Second, options.UserAuthPollInterval)
	custom, err := (RuntimeOptions{UserAuthPollInterval: 20 * time.Millisecond}).Normalize()
	require.NoError(t, err)
	require.Equal(t, 20*time.Millisecond, custom.UserAuthPollInterval)
	for _, bad := range []time.Duration{-time.Millisecond, 15*time.Second + time.Nanosecond} {
		_, err := (RuntimeOptions{UserAuthPollInterval: bad}).Normalize()
		require.ErrorIs(t, err, ErrRuntimeConfig)
	}
}

func TestUserAuthPollDelay(t *testing.T) {
	for _, tc := range []struct {
		name     string
		base     time.Duration
		poll     uint64
		jitter   float64
		expected time.Duration
	}{
		{name: "first poll uses base", base: 2 * time.Second, poll: 0, jitter: 1, expected: 2 * time.Second},
		{name: "doubles per poll", base: 2 * time.Second, poll: 2, jitter: 1, expected: 8 * time.Second},
		{name: "caps at fifteen seconds", base: 2 * time.Second, poll: 5, jitter: 1, expected: 15 * time.Second},
		{name: "jitter stays under cap", base: 2 * time.Second, poll: 5, jitter: 1.2, expected: 15 * time.Second},
		{name: "low jitter shortens", base: 2 * time.Second, poll: 0, jitter: 0.8, expected: 1600 * time.Millisecond},
		{name: "short base for tests", base: 10 * time.Millisecond, poll: 1, jitter: 1, expected: 20 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, userAuthPollDelay(tc.base, tc.poll, tc.jitter))
		})
	}
}
