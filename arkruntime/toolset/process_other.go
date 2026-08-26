// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0
//go:build !unix

package toolset

import "os/exec"

func setProcessGroup(_ *exec.Cmd) {}

func killCommandGroup(cmd *exec.Cmd) {
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
}
