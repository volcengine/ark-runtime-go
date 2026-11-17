// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMountReconciliationRejectsContradictoryResourceIdentity(t *testing.T) {
	for _, other := range []SessionResource{
		{ID: "resource", FileID: "changed", MountPath: "stable.csv", ActualPath: "actual"},
		{ID: "resource", FileID: "changed", MountPath: "another.csv", ActualPath: "another"},
		{ID: "other-resource", FileID: "registered", MountPath: "stable.csv", ActualPath: "actual"},
	} {
		t.Run(other.ID+"/"+other.MountPath, func(t *testing.T) {
			resources := []SessionResource{{ID: "resource", FileID: "registered", MountPath: "stable.csv", ActualPath: "actual"}, other}
			_, _, err := findMountedFile(resources, "original", "stable.csv")
			require.ErrorIs(t, err, errSessionResourceConflict)
		})
	}
}

func TestMountReconciliationPreservesOriginalAndRegisteredFileIDs(t *testing.T) {
	resources := []SessionResource{{ID: "resource", FileID: "registered", MountPath: "stable.csv", ActualPath: "actual"}}
	mounted, found, err := findMountedFile(resources, "original", "stable.csv")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "original", mounted.FileID)
	require.Equal(t, "registered", mounted.RegisteredFileID)
	require.Equal(t, "resource", mounted.ResourceID)
	require.True(t, mounted.ReconciledByPath, "stable path alone does not independently prove the original file identity")
}
