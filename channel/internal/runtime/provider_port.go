// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import "reflect"

// providerPort 保留外部适配器未实现的可选能力语义，避免包装器错误升级能力。
func providerPort[T any](p Provider) (T, bool) {
	if selective, ok := p.(interface{ SupportsProviderCapability(reflect.Type) bool }); ok && !selective.SupportsProviderCapability(reflect.TypeOf((*T)(nil)).Elem()) {
		var zero T
		return zero, false
	}
	v, ok := p.(T)
	return v, ok
}
