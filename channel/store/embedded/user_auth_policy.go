// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

// UserAuthEncryptionReady 仅报告实际存储是否具备授权提示需要的载荷加密能力。
func (s *store) UserAuthEncryptionReady() bool {
	return s != nil && s.payloadCodec != nil && s.payloadReady && s.payloadNamespace != ""
}
