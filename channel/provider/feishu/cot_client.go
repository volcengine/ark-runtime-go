// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// COTLimiter 可由宿主显式共享；总请求不超过每秒 50 次。
type COTLimiter struct {
	mu   sync.Mutex
	next time.Time
}

func (l *COTLimiter) wait(ctx context.Context, interval time.Duration) error {
	l.mu.Lock()
	now := time.Now()
	at := l.next
	if at.Before(now) {
		at = now
	}
	l.next = at.Add(interval)
	l.mu.Unlock()
	timer := time.NewTimer(time.Until(at))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type cotHandle struct {
	CotID     string `json:"cot_id"`
	MessageID string `json:"message_id"`
}

type cotEvent struct {
	Type      string `json:"event_type"`
	Content   string `json:"content"`
	Timestamp int64  `json:"timestamp"`
}

func validCOTID(id string) bool {
	if id == "" || len(id) > 256 {
		return false
	}
	for _, r := range id {
		if r != '_' && r != '-' && (r < '0' || r > '9') && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') {
			return false
		}
	}
	return true
}

func parseCOTHandle(value string) (cotHandle, error) {
	var handle cotHandle
	if len(value) > 1024 || json.Unmarshal([]byte(value), &handle) != nil || !validCOTID(handle.CotID) || !validCOTID(handle.MessageID) {
		return handle, failure{"invalid cot handle", false, false, "invalid_cot_handle"}
	}
	return handle, nil
}

func (p *Provider) cotRequest(ctx context.Context, method, path string, query url.Values, body any) ([]byte, error) {
	if err := p.cotLimit.wait(ctx, 50*time.Millisecond); err != nil {
		return nil, failure{"cot request not sent", false, true, "cot_not_sent"}
	}
	if err := p.cotShared.wait(ctx, 20*time.Millisecond); err != nil {
		return nil, failure{"cot request not sent", false, true, "cot_not_sent"}
	}
	token, _, err := p.BotToken(ctx)
	if err != nil {
		return nil, failure{"cot token unavailable", false, true, "cot_token_unavailable"}
	}
	var data []byte
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return nil, failure{"invalid cot request", false, false, "invalid_cot_request"}
		}
	}
	base := strings.TrimRight(p.baseURL, "/")
	if base == "" {
		base = "https://open.feishu.cn"
	}
	target := base + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(data))
	if err != nil {
		return nil, failure{"invalid cot request", false, false, "invalid_cot_request"}
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json;charset=utf-8")
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, transportFailure(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil || len(raw) > 1<<20 {
		return nil, failure{"cot result unknown", true, false, "cot_response_unknown"}
	}
	var result struct {
		Code *int `json:"code"`
	}
	if json.Unmarshal(raw, &result) != nil {
		return nil, failure{"cot result unknown", true, false, "cot_response_unknown"}
	}
	code := 0
	if result.Code != nil {
		code = *result.Code
	} else if method != http.MethodPost || path != "/open-apis/im/v1/message_cot" {
		return nil, failure{"cot result unknown", true, false, "cot_response_unknown"}
	}
	if err := responseFailure(resp.StatusCode, code, code == 0, true); err != nil {
		return nil, err
	}
	return raw, nil
}
