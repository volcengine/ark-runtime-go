// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package arkruntime

import "fmt"

// notImplementedErr returns the standard NotImplemented error for APIs
// that aren't yet backed by ark-apis generated models. Used by resource
// stubs across the package. See ROADMAP in README.
func notImplementedErr(api string) error { //nolint:unused // stub for unimplemented API methods
	return fmt.Errorf(
		"%s: not implemented in ark-runtime-go. this sdk currently ships only the responses api, backed by ark-apis generated models. see https://github.com/volcengine/ark-apis for the roadmap",
		api,
	)
}
