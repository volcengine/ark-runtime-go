// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestHookRejectsSuccessAfterDeadline(t *testing.T) {
	options, err := (RuntimeOptions{HookTimeout: time.Millisecond}).Normalize()
	if err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{options: options, hookSlots: make(chan struct{}, 1)}
	err = runtime.callHook(context.Background(), "binding", "turn", hookBeforeTrigger, func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("late success must be rejected, got %v", err)
	}
	if len(runtime.hookSlots) != 0 {
		t.Fatal("returned hook still holds its concurrency slot")
	}
}
