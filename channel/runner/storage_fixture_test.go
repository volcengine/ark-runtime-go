// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0
package runner

import (
	"github.com/volcengine/ark-runtime-go/channel/internal/handles"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/store/embedded"
)

func openTestEmbedded(path string, options embedded.Options) (core.Store, error) {
	handle, err := embedded.Open(path, options)
	if err != nil {
		return nil, err
	}
	return handles.StoreValue(handle), nil
}
