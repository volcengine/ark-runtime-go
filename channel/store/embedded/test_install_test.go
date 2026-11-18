// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

import "os"

// openInstalledFixture 为原业务用例显式准备新文件；Open契约由lifecycle_test独立验证。
func openInstalledFixture(path string, options Options) (*store, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) && !options.ReadOnly {
		if err = Install(path, options); err != nil {
			return nil, err
		}
	}
	return open(path, options)
}
