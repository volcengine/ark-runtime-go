// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"reflect"
	"testing"
)

type selectiveProvider struct{}

func (selectiveProvider) Start(context.Context, func(context.Context, Message) error) error {
	return nil
}
func (selectiveProvider) Deliver(context.Context, Delivery) (string, error) { return "", nil }
func (selectiveProvider) ReportProgress(context.Context, string, ProgressUpdate) (string, error) {
	return "", nil
}
func (selectiveProvider) ReportDurableProgress(context.Context, ProgressCheckpoint, ProgressUpdate) (ProgressCheckpoint, error) {
	panic("unsupported optional capability must never be called")
}
func (selectiveProvider) SupportsProviderCapability(t reflect.Type) bool {
	return t.Name() == "ProgressReporter"
}
func TestProviderPortPreservesOptionalCapabilityAbsence(t *testing.T) {
	var p Provider = selectiveProvider{}
	if _, ok := providerPort[ProgressReporter](p); !ok {
		t.Fatal("supported capability unavailable")
	}
	if _, ok := providerPort[DurableProgressReporter](p); ok {
		t.Fatal("adapter's unimplemented optional capability was advertised")
	}
}
