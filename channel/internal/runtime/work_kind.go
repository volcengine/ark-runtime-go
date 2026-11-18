// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"encoding/json"
	"errors"
)

// ErrUnsupportedWork 表示任务由其他协议处理器拥有；不能据此失败或删除其原记录。
var ErrUnsupportedWork = errors.New("unsupported stored work")

// WorkKindLegacy 是兼容读取早期 JSON 头时使用的默认值，不对应存储列。
const WorkKindLegacy = "legacy"

// WorkKindReaction 保持已发表表情记录的 JSON 编码，不参与 SQL 分派。
const WorkKindReaction = "message_reaction_v1"

// SupportsDeliveryWorkKind 判断早期投递 JSON 头是否可被当前 reader 读取。
func SupportsDeliveryWorkKind(kind string) bool {
	return kind == WorkKindLegacy || kind == WorkKindReaction
}

// StoredWorkKind 只读识别早期 JSON 头；未声明时按 legacy 解码，不用于新写入。
func StoredWorkKind(raw []byte) (string, error) {
	var header struct {
		Kind string `json:"_work_kind"`
	}
	if err := json.Unmarshal(raw, &header); err != nil {
		return "", err
	}
	if header.Kind == "" {
		return WorkKindLegacy, nil
	}
	return header.Kind, nil
}

// SupportsDeliveryBody 判断本基线是否具有相应执行器；opaque可读不等于可以执行。
func SupportsDeliveryBody(body DeliveryBody) bool {
	switch body.Type {
	case BodyRichText, BodySessionFile, BodyText, BodyStreamText, BodyImageStream, BodyArtifact, BodyOutputFile, BodyProgress, BodyReaction:
		return true
	}
	return false
}

// TurnWorkKind 返回明确的工作协议，空值保留历史默认值。
func TurnWorkKind(t Turn) string {
	if t.WorkKind == "" {
		return WorkKindLegacy
	}
	return t.WorkKind
}

// InboundWorkKind 为 User 身份选择独立授权协议，隔离不支持该协议的旧 worker。
func InboundWorkKind(b Binding, chatType string) string {
	if effectiveIdentity(b, chatType) == IdentityUser {
		return WorkKindUserAuth
	}
	return ""
}
