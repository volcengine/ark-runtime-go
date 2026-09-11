// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0
//
// Adapted from github.com/ogen-go/ogen v1.20.3 (Apache-2.0).
// See README.md for provenance and compatibility scope.

package validate

import (
	"fmt"

	"github.com/volcengine/ark-runtime-go/arkruntime/internal/ogenregex"
)

// MinLengthError reports that len less than minimum.
type MinLengthError struct {
	Len       int
	MinLength int
}

// MinLengthError implements error.
func (e *MinLengthError) Error() string {
	return fmt.Sprintf("len %d less than minimum %d", e.Len, e.MinLength)
}

// MaxLengthError reports that len greater than maximum.
type MaxLengthError struct {
	Len       int
	MaxLength int
}

// MaxLengthError implements error.
func (e *MaxLengthError) Error() string {
	return fmt.Sprintf("len %d greater than maximum %d", e.Len, e.MaxLength)
}

// NoRegexMatchError reports that value have no regexp match.
type NoRegexMatchError struct {
	Pattern ogenregex.Regexp
}

// MaxLengthError implements error.
func (e *NoRegexMatchError) Error() string {
	return fmt.Sprintf("no regex match: %s", e.Pattern.String())
}
