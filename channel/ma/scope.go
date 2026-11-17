// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package ma

import "strings"

// ScopeKind 标识请求属于普通资源还是实际 Session。
type ScopeKind string

const (
	// ScopeMarker 表示不属于特定 Session 的资源请求。
	ScopeMarker ScopeKind = "marker"
	// ScopeSession 表示属于 SessionID 的请求。
	ScopeSession ScopeKind = "session"
)

// RequestScope 为模型 Header 选择提供 Session 范围。
type RequestScope struct {
	Kind      ScopeKind
	SessionID string
}

func requestScope(path string) RequestScope {
	if tail, ok := strings.CutPrefix(path, "/sessions/"); ok {
		id, _, _ := strings.Cut(tail, "/")
		if id != "" {
			return RequestScope{Kind: ScopeSession, SessionID: id}
		}
	}
	return RequestScope{Kind: ScopeMarker}
}
