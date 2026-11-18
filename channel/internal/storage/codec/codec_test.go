// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package codec

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCanonicalScopeRejectsIncompleteIdentity(t *testing.T) {
	scope := Scope{Owner: "owner", Project: "project", Namespace: "stg", RecordKind: "turn", RecordID: "id", Field: "input", PayloadVersion: 1, LogicalSchema: 1}
	first, err := CanonicalScope(scope)
	require.NoError(t, err)
	second, err := CanonicalScope(scope)
	require.NoError(t, err)
	require.Equal(t, first, second)
	scope.RecordID = ""
	_, err = CanonicalScope(scope)
	require.ErrorIs(t, err, ErrInvalidScope)
}

func TestCanonicalScopeHasUnambiguousFieldBoundaries(t *testing.T) {
	a := Scope{Owner: "a/b", Project: "c", Namespace: "stg", RecordKind: "turn", RecordID: "id", Field: "input", PayloadVersion: 1, LogicalSchema: 1}
	b := a
	b.Owner, b.Project = "a", "b/c"
	left, err := CanonicalScope(a)
	require.NoError(t, err)
	right, err := CanonicalScope(b)
	require.NoError(t, err)
	require.NotEqual(t, left, right)
}
