// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"context"
	"math"
	"math/rand"
	"time"
)

type RetryPolicy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	MaxRetryAfter  time.Duration
	RetryAfter     func(error) (time.Duration, bool)
}

func Retry(ctx context.Context,
	rp RetryPolicy,
	isNeedRetry func() bool,
	doFunc func() error, overRetryLimitError error,
	isNeedRetryError func(error) bool,
) error {
	return retry(ctx, rp, isNeedRetry, func(_ int) error {
		return doFunc()
	}, overRetryLimitError, isNeedRetryError)
}

func RetryWithAttempt(ctx context.Context,
	rp RetryPolicy,
	isNeedRetry func() bool,
	doFunc func(int) error, overRetryLimitError error,
	isNeedRetryError func(error) bool,
) error {
	return retry(ctx, rp, isNeedRetry, doFunc, overRetryLimitError, isNeedRetryError)
}

func retry(ctx context.Context,
	rp RetryPolicy,
	isNeedRetry func() bool,
	doFunc func(int) error, overRetryLimitError error,
	isNeedRetryError func(error) bool,
) error {
	var err error
	for numRetriesSincePushback := 0; numRetriesSincePushback <= rp.MaxAttempts; numRetriesSincePushback++ {
		err = doFunc(numRetriesSincePushback)

		// no error: just return on this try
		if err == nil {
			return nil
		}

		// not need retry: first time do return
		if isNeedRetry != nil && !isNeedRetry() {
			return err
		}

		// no need to retry error
		if !isNeedRetryError(err) {
			return err
		}

		// need retry
		if numRetriesSincePushback == rp.MaxAttempts {
			break
		}
		dur := retryDelay(rp, numRetriesSincePushback, err)

		t := time.NewTimer(dur)
		select {
		case <-t.C: // continue next retry
		case <-ctx.Done(): // whole context finish
			t.Stop()
			return ctx.Err()
		}
	}

	// Note: if not set over retry limit error, return last meet error
	if overRetryLimitError == nil {
		return err
	}
	return overRetryLimitError
}

func retryDelay(rp RetryPolicy, retryCount int, err error) time.Duration {
	if rp.RetryAfter != nil {
		if delay, ok := rp.RetryAfter(err); ok {
			maxRetryAfter := rp.MaxRetryAfter
			if maxRetryAfter <= 0 {
				maxRetryAfter = 60 * time.Second
			}
			if delay > 0 && delay <= maxRetryAfter {
				return delay
			}
		}
	}

	delay := time.Duration(float64(rp.InitialBackoff) * math.Pow(2.0, float64(retryCount)))
	if delay > rp.MaxBackoff {
		delay = rp.MaxBackoff
	}
	jitter := 1.0 - 0.25*rand.Float64()
	return time.Duration(float64(delay) * jitter)
}
