// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

//go:build !darwin && !linux && !freebsd && !netbsd && !openbsd && !dragonfly && !windows

package embedded

import (
	"errors"
	"os"
)

func tryStoreFileLock(_ *os.File, _ bool) (bool, error) {
	return false, errors.New("embedded path lock unsupported on this platform")
}
func releaseStoreFileLock(_ *os.File) error { return nil }
