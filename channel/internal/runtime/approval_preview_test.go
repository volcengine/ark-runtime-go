// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"strings"
	"testing"
)

func TestApprovalPreviewRedactsToolCredentials(t *testing.T) {
	for _, input := range []string{
		`{"command":"safe-value","nested":{"api_key":"fixture-private-key"},"echo":"fixture-private-key"}`,
		`safe-value Authorization: Bearer fixture-private-key --password fixture-password`,
	} {
		preview := approvalPreview(input)
		if strings.Contains(preview, "fixture-private-key") || strings.Contains(preview, "fixture-password") {
			t.Fatalf("approval preview retained a credential: %q", preview)
		}
		if !strings.Contains(preview, "safe-value") || !strings.Contains(preview, "[REDACTED]") {
			t.Fatalf("approval preview lost useful context or its redaction marker: %q", preview)
		}
	}
}
