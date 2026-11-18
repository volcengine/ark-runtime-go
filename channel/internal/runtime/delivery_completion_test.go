// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDeliveryCompletionTimeoutPreservesTaskClassification(t *testing.T) {
	for _, failure := range []error{ErrLeaseLost, ErrUnsupportedWork, NewStoreFailure(context.DeadlineExceeded)} {
		t.Run(failure.Error(), func(t *testing.T) {
			options, err := (RuntimeOptions{DeliveryTimeout: time.Millisecond, PollInterval: time.Millisecond}).Normalize()
			require.NoError(t, err)
			runtime := &Runtime{options: options, store: runtimePortStore{complete: func(ctx context.Context, _ Lease, _ DeliveryResult) error {
				<-ctx.Done()
				return errors.Join(failure, ctx.Err())
			}}}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			require.ErrorIs(t, runtime.completeDelivery(ctx, Delivery{}, DeliveryResult{}), failure)
		})
	}
}

type promptFailureAuthorizer struct {
	UserAuthorizer
	failure error
}

func (a promptFailureAuthorizer) AuthPrompt(context.Context, AuthFlowRef) (AuthFlow, error) {
	return AuthFlow{}, a.failure
}

func TestAuthPromptPreservesTaskClassification(t *testing.T) {
	for _, failure := range []error{ErrLeaseLost, ErrUnsupportedWork, NewStoreFailure(errors.New("private database failure"))} {
		t.Run(failure.Error(), func(t *testing.T) {
			options, err := (RuntimeOptions{}).Normalize()
			require.NoError(t, err)
			runtime := &Runtime{options: options}
			ready, err := runtime.pollUserAuth(context.Background(), &job{}, Connection{UserAuthorizer: promptFailureAuthorizer{failure: failure}}, UserAuthRecord{ExpiresAt: time.Now().Add(time.Minute)}, UserGrantQuery{})
			require.False(t, ready)
			require.ErrorIs(t, err, failure)
		})
	}
}
