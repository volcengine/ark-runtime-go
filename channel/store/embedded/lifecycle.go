// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

import (
	"errors"
	"os"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// Install 显式创建全新数据库；已有文件不可被当成空库覆盖。
func Install(path string, options Options) error {
	if options.ReadOnly || path == "" {
		return errInvalid
	}
	if _, err := os.Stat(path); err == nil {
		return core.ErrConflict
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	s, err := openStore(path, options, true)
	if err != nil {
		return err
	}
	return s.Close()
}
