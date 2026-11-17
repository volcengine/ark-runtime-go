// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

import (
	"os"
	"time"

	"go.etcd.io/bbolt/errors"
)

// lockStorePath 的固定sidecar锁覆盖bbolt离线置换前后的inode变化。
// 文件保留为空锁锚点；删除它会让新进程绕过旧inode上的锁。
func lockStorePath(path string, readOnly bool, timeout time.Duration) (*os.File, error) {
	f, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(timeout)
	for {
		locked, lockErr := tryStoreFileLock(f, readOnly)
		if lockErr != nil {
			_ = f.Close()
			return nil, lockErr
		}
		if locked {
			return f, nil
		}
		if !time.Now().Before(deadline) {
			_ = f.Close()
			return nil, errors.ErrTimeout
		}
		time.Sleep(5 * time.Millisecond)
	}
}
func unlockStorePath(f *os.File) error {
	if f == nil {
		return nil
	}
	err := releaseStoreFileLock(f)
	closeErr := f.Close()
	if err != nil {
		return err
	}
	return closeErr
}
