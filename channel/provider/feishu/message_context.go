// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"sync"
	"time"
	"unicode/utf8"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

const (
	messageContextBudget   = 300 * time.Millisecond
	messageContextCapacity = 256
	messageContextTTL      = 10 * time.Minute
	messageContextErrorTTL = time.Minute
)

// 缓存属于单个 Provider，应用、owner/project、epoch 与 Secret 均由其冻结 Binding 隔离。
type messageContextKey struct{ tenant, kind, id string }
type messageContextEntry struct {
	details core.MessageContext
	err     error
	expires time.Time
}
type messageContextCache struct {
	mu      sync.Mutex
	entries map[messageContextKey]messageContextEntry
}

var _ core.ChannelContextProvider = (*Provider)(nil)

// ChannelProvider 返回上下文的稳定来源，不依赖网络查询成功。
func (*Provider) ChannelProvider() string { return "feishu" }

// ResolveMessageContext 在共享短截止时间内尽力补充展示信息，不修改消息身份。
func (p *Provider) ResolveMessageContext(ctx context.Context, message core.Message) (core.MessageContext, error) {
	ctx, cancel := context.WithTimeout(ctx, messageContextBudget)
	defer cancel()
	var details core.MessageContext
	var chatErr, userErr error
	if message.ChatType == "group" {
		details, chatErr = p.cachedMessageContext(ctx, messageContextKey{message.TenantKey, "chat", message.ChatID})
	}
	if message.SenderID != "" {
		user, err := p.cachedMessageContext(ctx, messageContextKey{message.TenantKey, "user", message.SenderID})
		details.SenderName, userErr = user.SenderName, err
	}
	return details, errors.Join(chatErr, userErr)
}

func (p *Provider) cachedMessageContext(ctx context.Context, key messageContextKey) (core.MessageContext, error) {
	if key.tenant == "" || key.id == "" || len(key.id) > 256 || !utf8.ValidString(key.id) || p.client == nil {
		return core.MessageContext{}, errors.New("feishu message context identity invalid")
	}
	now := time.Now()
	cache := &p.messageContextCache
	cache.mu.Lock()
	entry, exists := cache.entries[key]
	cache.mu.Unlock()
	if exists && now.Before(entry.expires) {
		return entry.details, entry.err
	}
	if err := ctx.Err(); err != nil {
		return core.MessageContext{}, err
	}
	var details core.MessageContext
	var err error
	if key.kind == "user" {
		details.SenderName, err = p.LookupUserName(ctx, key.id)
	} else {
		details, err = p.lookupMessageChat(ctx, key.id)
	}
	ttl := messageContextTTL
	if err != nil {
		var apiErr core.APIErrorCode
		// 只负缓存稳定 API 拒绝；超时、限流与服务端故障允许下一轮重新尝试。
		if !errors.As(err, &apiErr) || apiErr.HTTPStatus() == http.StatusTooManyRequests || apiErr.HTTPStatus() >= http.StatusInternalServerError || apiErr.HTTPStatus() == http.StatusOK && apiErr.ErrorCode() == "0" {
			return details, err
		}
		ttl = messageContextErrorTTL
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.entries == nil {
		cache.entries = make(map[messageContextKey]messageContextEntry)
	}
	if len(cache.entries) >= messageContextCapacity {
		var victim messageContextKey
		var earliest time.Time
		for candidate, value := range cache.entries {
			if earliest.IsZero() || value.expires.Before(earliest) {
				victim, earliest = candidate, value.expires
			}
		}
		delete(cache.entries, victim)
	}
	cache.entries[key] = messageContextEntry{details: details, err: err, expires: time.Now().Add(ttl)}
	return details, err
}

func (p *Provider) lookupMessageChat(ctx context.Context, chatID string) (core.MessageContext, error) {
	response, err := p.client.Get(ctx, "/open-apis/im/v1/chats/"+url.PathEscape(chatID), nil, larkcore.AccessTokenTypeTenant)
	if err != nil || response == nil {
		return core.MessageContext{}, errors.Join(errors.New("feishu message chat unavailable"), ctx.Err())
	}
	var body struct {
		Code int `json:"code"`
		Data struct {
			Name string `json:"name"`
			Mode string `json:"chat_mode"`
		} `json:"data"`
	}
	if len(response.RawBody) > 64<<10 || json.Unmarshal(response.RawBody, &body) != nil || response.StatusCode != http.StatusOK || body.Code != 0 {
		return core.MessageContext{}, &displayNameError{status: response.StatusCode, code: body.Code}
	}
	details := core.MessageContext{}
	if utf8.RuneCountInString(body.Data.Name) <= 256 {
		details.ChatName = body.Data.Name
	}
	if body.Data.Mode == "group" || body.Data.Mode == "topic" {
		details.ChatMode = body.Data.Mode
	}
	return details, nil
}
