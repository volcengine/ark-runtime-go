// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package ma

import (
	"context"
	"errors"
	"net/http"
	"strings"
)

const (
	modelHeaderPrefix        = "X-User-Model-Header-"
	maxModelHeaderCount      = 32
	maxModelHeaderNameBytes  = 64
	maxModelHeaderValueBytes = 4096
	maxModelHeaderTotalBytes = 16 << 10
)

// ModelHeaderSource 按请求范围提供普通目标 Header；实现必须遵守 ctx，返回期间不得并发修改 map。
// 每次 events 提交时读取；客户端不修改或持久化返回值。
type ModelHeaderSource func(context.Context, RequestScope) (map[string]string, error)

// ValidateModelHeaders 校验普通目标 Header；名称不得带传输前缀，错误不包含输入。
func ValidateModelHeaders(headers map[string]string) error {
	if len(headers) > maxModelHeaderCount {
		return errors.New("invalid model header set")
	}
	seen := make(map[string]bool, len(headers))
	total := 0
	for name, value := range headers {
		lower := strings.ToLower(name)
		if len(name) == 0 || len(name) > maxModelHeaderNameBytes || !validModelHeaderName(name) || forbiddenModelHeader(lower) || seen[lower] {
			return errors.New("invalid model header set")
		}
		seen[lower] = true
		if len(value) == 0 || len(value) > maxModelHeaderValueBytes || strings.IndexFunc(value, func(c rune) bool { return c < ' ' || c == 0x7f }) >= 0 {
			return errors.New("invalid model header set")
		}
		total += len(modelHeaderPrefix) + len(name) + len(value)
		if total > maxModelHeaderTotalBytes {
			return errors.New("invalid model header set")
		}
	}
	return nil
}

func validModelHeaderName(name string) bool {
	for i := 0; i < len(name); i++ {
		c := name[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("!#$%&'*+-.^_`|~", rune(c)) {
			continue
		}
		return false
	}
	return true
}

func forbiddenModelHeader(name string) bool {
	switch name {
	case "accept", "connection", "content-length", "content-type", "cookie", "forwarded", "host", "proxy-authenticate", "proxy-authorization", "proxy-connection", "set-cookie", "te", "trailer", "transfer-encoding", "upgrade", "via", "x-client-request-id", "x-request-id":
		return true
	}
	for _, prefix := range []string{"ark-agent-", "proxy-", "x-ark-", "x-forwarded-", "x-user-model-header-"} {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

func (c *Client) applyModelHeaders(req *http.Request, path string, scope RequestScope) error {
	if c.modelHeaderSource == nil || req.Method != http.MethodPost || scope.Kind != ScopeSession || path != "/sessions/"+scope.SessionID+"/events" {
		return nil
	}
	headers, err := c.modelHeaderSource(req.Context(), scope)
	if req.Context().Err() != nil {
		return req.Context().Err()
	}
	if err != nil {
		return errors.New("resolve model headers failed")
	}
	if err := ValidateModelHeaders(headers); err != nil {
		return err
	}
	for name, value := range headers {
		req.Header.Set(modelHeaderPrefix+http.CanonicalHeaderKey(name), value)
	}
	return nil
}
