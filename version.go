// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Package arkruntime provides build metadata for the Ark runtime SDK.
package arkruntime

import (
	_ "embed"
	"strings"
)

//go:embed VERSION
var version string

// Version returns the SDK version embedded from the repository's VERSION file.
func Version() string {
	return strings.TrimSpace(version)
}
