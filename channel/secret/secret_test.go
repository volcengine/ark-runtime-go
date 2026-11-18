// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package secret

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidate(t *testing.T) {
	for _, tc := range []struct {
		ref string
		ok  bool
	}{
		{"env:BOT_SECRET", true}, {"file:/tmp/bot-secret", true},
		{"env:", false}, {"env:BAD-NAME", false}, {"file:relative", false},
		{"literal:secret", false}, {"secret", false},
	} {
		t.Run(tc.ref, func(t *testing.T) {
			err := Validate(tc.ref)
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				if strings.Contains(tc.ref, ":") {
					require.NotContains(t, err.Error(), tc.ref)
				}
			}
		})
	}
}

func TestEnvFileResolver(t *testing.T) {
	r := EnvFileResolver{}
	t.Setenv("BOT_SECRET_TEST", "value")
	got, err := r.Resolve("env:BOT_SECRET_TEST")
	require.NoError(t, err)
	require.Equal(t, "value", got)

	path := filepath.Join(t.TempDir(), "secret")
	require.NoError(t, os.WriteFile(path, []byte("file-value\n"), 0o600))
	got, err = r.Resolve("file:" + path)
	require.NoError(t, err)
	require.Equal(t, "file-value", got)

	for _, tc := range []struct {
		name string
		path string
		data string
		mode os.FileMode
	}{
		{"insecure", filepath.Join(t.TempDir(), "insecure"), "wire-secret", 0o644},
		{"oversize", filepath.Join(t.TempDir(), "oversize"), strings.Repeat("x", 64*1024+1), 0o600},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, os.WriteFile(tc.path, []byte(tc.data), tc.mode))
			_, err := r.Resolve("file:" + tc.path)
			require.Error(t, err)
			require.NotContains(t, err.Error(), tc.path)
			require.NotContains(t, err.Error(), "wire-secret")
		})
	}
	t.Setenv("BOT_SECRET_EMPTY", "")
	_, err = r.Resolve("env:BOT_SECRET_EMPTY")
	require.Error(t, err)
	_, err = r.Resolve("env:BOT_SECRET_NOT_PRESENT_IN_TEST")
	require.Error(t, err)
	_, err = r.Resolve("file:" + t.TempDir())
	require.Error(t, err)
	link := filepath.Join(t.TempDir(), "link")
	require.NoError(t, os.Symlink(path, link))
	_, err = r.Resolve("file:" + link)
	require.Error(t, err)
}

func TestResolveFileRejectsPathReplacement(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "secret")
	replacement := filepath.Join(dir, "replacement")
	require.NoError(t, os.WriteFile(path, []byte("original"), 0o600))
	for _, symlink := range []bool{false, true} {
		t.Run(map[bool]string{false: "different file", true: "symlink"}[symlink], func(t *testing.T) {
			require.NoError(t, os.Remove(path))
			require.NoError(t, os.WriteFile(path, []byte("original"), 0o600))
			require.NoError(t, os.WriteFile(replacement, []byte("replacement-secret"), 0o600))
			_, err := resolveFile(path, func(name string) (*os.File, error) {
				require.NoError(t, os.Remove(name))
				if symlink {
					require.NoError(t, os.Symlink(replacement, name))
				} else {
					require.NoError(t, os.Rename(replacement, name))
				}
				return openNoFollow(name)
			})
			require.Error(t, err)
			require.NotContains(t, err.Error(), "replacement-secret")
		})
	}
}
