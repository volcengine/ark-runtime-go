// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"encoding/json"
)

// UnmarshalJSON 兼容旧版单值 identity（"bot" 视为 {group:bot,p2p:bot}；
// "user" 保留为 {p2p:user}，由 validateIdentity 在 S9 前启动拒绝）。
func (b *Binding) UnmarshalJSON(data []byte) error {
	// 别名复用完整字段定义，仅覆盖 identity 的新旧 JSON 兼容形状。
	type plain Binding
	var next Binding
	raw := struct {
		*plain
		Identity json.RawMessage `json:"identity"`
	}{plain: (*plain)(&next)}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*b = next
	if len(raw.Identity) == 0 {
		return nil
	}
	var text string
	if err := json.Unmarshal(raw.Identity, &text); err == nil {
		switch text {
		case "", "none":
			return nil
		case IdentityBot:
			b.Identity = IdentityPolicy{Group: IdentityBot, P2P: IdentityBot}
			return nil
		case IdentityUser:
			b.Identity = IdentityPolicy{P2P: IdentityUser}
			return nil
		default:
			return ErrInvalid
		}
	}
	var policy IdentityPolicy
	if err := json.Unmarshal(raw.Identity, &policy); err != nil {
		return err
	}
	b.Identity = policy
	return nil
}
