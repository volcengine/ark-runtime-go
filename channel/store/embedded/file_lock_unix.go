// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

//go:build darwin || linux || freebsd || netbsd || openbsd || dragonfly

package embedded

import (
	"errors"
	"os"
	"syscall"
)

func tryStoreFileLock(f *os.File, readOnly bool) (bool, error) {
	mode := syscall.LOCK_EX
	if readOnly {
		mode = syscall.LOCK_SH
	}
	err := syscall.Flock(int(f.Fd()), mode|syscall.LOCK_NB)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
		return false, nil
	}
	return false, err
}
func releaseStoreFileLock(f *os.File) error { return syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }
