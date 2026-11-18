// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package payload

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReferencesRejectsUnreadableRoots(t *testing.T) {
	for _, raw := range []string{"{}", "null", "[]", `{"_payloads":null}`, `{"_payloads":{}}`, `{"_payloads":{"refs":null}}`, `{"_payloads":{"refs":{"body":42}}}`} {
		t.Run(raw, func(t *testing.T) {
			_, err := References("ns", "message", "id", []byte(raw))
			require.ErrorIs(t, err, ErrInvalid)
		})
	}
}

func TestReferencesPreservesOpaqueAndSharedLinks(t *testing.T) {
	refs, err := References("ns", "message", "id", []byte(`{"_work_kind":"future","future":{"unreadable_to_old_domain":true},"_payloads":{"future":7,"refs":{"spec":"shared","future_field":"future_payload"}}}`))
	require.NoError(t, err)
	require.Equal(t, map[string]string{"spec": "shared", "future_field": "future_payload"}, refs)
}
