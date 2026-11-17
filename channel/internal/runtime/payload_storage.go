// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import "strconv"

// StoredPayloads 只保存 Store 的载荷引用与受理时冻结的归属，不参与领域内容哈希。
// 调用方不得构造或修改此字段；持久实现从已鉴权 Binding 生成它。
type StoredPayloads struct {
	StoredRecord `json:"-"`
	Owner        string            `json:"owner"`
	Project      string            `json:"project"`
	Refs         map[string]string `json:"refs"`
}

// LogicalHash 计算完整冻结输入的逻辑hash，始终忽略调用方提供的缓存值。
func (f FrozenInput) LogicalHash() string {
	f.LogicalDigest = ""
	data, err := EncodeStored(f)
	if err != nil {
		return ""
	}
	return digestBytes(data)
}

func (t Turn) storedInputHash() string {
	contextField := "context:" + strconv.FormatUint(t.Session.Generation, 10)
	if t.Payloads != nil && t.Input.LogicalDigest != "" && t.Input.Context == "" && len(t.Input.Blocks) == 0 && len(t.Input.Mounts) == 0 && (!t.Input.ContextFrozen || t.Payloads.Refs["context"] != "" || t.Payloads.Refs[contextField] != "") && (!t.Input.BlocksFrozen || t.Payloads.Refs["blocks"] != "") {
		return t.Input.LogicalDigest
	}
	return t.Input.LogicalHash()
}
