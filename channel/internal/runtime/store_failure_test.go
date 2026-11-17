// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"strings"
	"testing"
)

func TestStoreFailureReportsSafeKind(t *testing.T) {
	secret := errors.New("driver DSN=password-fixture token=private-fixture")
	for _, tc := range []struct {
		name  string
		cause error
		kind  string
	}{
		{"deadline", errors.Join(context.DeadlineExceeded, secret), "deadline"},
		{"canceled", errors.Join(context.Canceled, secret), "canceled"},
		{"permission", &fs.PathError{Op: "read", Path: "private-fixture", Err: fs.ErrPermission}, "permission"},
		{"io", errors.Join(io.ErrUnexpectedEOF, secret), "io"},
		{"generic", secret, "generic"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := NewStoreFailure(tc.cause)
			if !strings.Contains(err.Error(), tc.kind) || strings.Contains(err.Error(), "private-fixture") || strings.Contains(err.Error(), "password-fixture") {
				t.Fatalf("unsafe or missing diagnostic kind: %q", err.Error())
			}
			if !errors.Is(err, tc.cause) {
				t.Fatal("safe formatting discarded the original cause")
			}
			var failure StoreFailure
			if !errors.As(err, &failure) || !failure.StoreFailure() {
				t.Fatal("safe formatting discarded the infrastructure-failure marker")
			}
		})
	}
}
