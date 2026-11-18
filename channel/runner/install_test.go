// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package runner

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInstallStorageCreatesNewFileAndNeverOverwritesExisting(t *testing.T) {
	for _, driver := range []string{"embedded", "sqlite"} {
		t.Run(driver, func(t *testing.T) {
			cfg := testConfig(t)
			cfg.Store.Driver = driver
			cfg.DatabasePath = filepath.Join(t.TempDir(), "new-directory", "storage.db")
			require.NoError(t, Install(t.Context(), cfg))
			info, err := os.Stat(cfg.DatabasePath)
			require.NoError(t, err)
			require.Equal(t, os.FileMode(0600), info.Mode().Perm())
			_, err = Status(t.Context(), cfg)
			require.NoError(t, err)
			before, err := os.ReadFile(cfg.DatabasePath)
			require.NoError(t, err)
			require.ErrorContains(t, Install(t.Context(), cfg), "already exists")
			after, err := os.ReadFile(cfg.DatabasePath)
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func TestInstallStorageRejectsExistingEmptyCorruptAndSymlinkFiles(t *testing.T) {
	for _, driver := range []string{"embedded", "sqlite"} {
		for _, content := range []string{"", "corrupt-existing-state"} {
			t.Run(driver+"/"+content, func(t *testing.T) {
				cfg := testConfig(t)
				cfg.Store.Driver = driver
				require.NoError(t, os.WriteFile(cfg.DatabasePath, []byte(content), 0600))
				require.ErrorContains(t, Install(t.Context(), cfg), "already exists")
				after, err := os.ReadFile(cfg.DatabasePath)
				require.NoError(t, err)
				require.Equal(t, content, string(after))
			})
		}
		cfg := testConfig(t)
		cfg.Store.Driver = driver
		target := filepath.Join(t.TempDir(), "existing.db")
		require.NoError(t, os.WriteFile(target, []byte("existing-target"), 0600))
		require.NoError(t, os.Symlink(target, cfg.DatabasePath))
		require.ErrorContains(t, Install(t.Context(), cfg), "already exists")
		after, err := os.ReadFile(target)
		require.NoError(t, err)
		require.Equal(t, "existing-target", string(after))
	}
}

func TestInstallStorageCancelledBeforeStartLeavesPathMissing(t *testing.T) {
	for _, driver := range []string{"embedded", "sqlite", "mysql"} {
		cfg := testConfig(t)
		cfg.Store.Driver = driver
		if driver == "mysql" {
			cfg.Store.DSNRef = "env:UNUSED_DSN"
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		resolver := &countingAdminResolver{}
		require.ErrorIs(t, InstallStorageWith(ctx, cfg, resolver), context.Canceled)
		require.Zero(t, resolver.calls)
		require.NoFileExists(t, cfg.DatabasePath)
	}
	for _, test := range []struct {
		name string
		ctx  context.Context
	}{{name: "missing context"}} {
		t.Run(test.name, func(t *testing.T) {
			require.ErrorContains(t, Install(test.ctx, testConfig(t)), "context required")
		})
	}
}

func TestMissingStorageRemainsMissingAcrossMaintenanceAndDoctor(t *testing.T) {
	for _, driver := range []string{"embedded", "sqlite"} {
		cfg := testConfig(t)
		cfg.Store.Driver = driver
		for _, operation := range adminContextOperations() {
			require.ErrorContains(t, operation(t.Context(), cfg), "database does not exist")
			require.NoFileExists(t, cfg.DatabasePath)
		}
		_, err := Status(t.Context(), cfg)
		require.ErrorContains(t, err, "database does not exist")
		checks := Doctor(t.Context(), cfg, DoctorOptions{Resolver: staticResolver{}})
		for _, check := range checks {
			if check.Name == "database" {
				require.False(t, check.OK)
				require.Contains(t, check.Detail, "install")
			}
		}
		require.NoFileExists(t, cfg.DatabasePath)
	}
}
