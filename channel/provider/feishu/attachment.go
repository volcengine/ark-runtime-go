// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

var _ core.AttachmentFetcher = (*Provider)(nil)

// FetchAttachment 以应用身份下载消息资源（GET /im/v1/messages/:message_id/resources/:file_key）。
// 只能下载当前消息自身引用的资源；响应体以流交给调用方，不在内存缓冲整文件。
func (p *Provider) FetchAttachment(ctx context.Context, message core.Message, attachment core.Attachment) (core.DownloadedAttachment, error) {
	if p.client == nil || message.MessageID == "" || attachment.Key == "" || attachment.Kind != "image" && attachment.Kind != "file" {
		return core.DownloadedAttachment{}, errors.New("feishu attachment invalid")
	}
	if strings.ContainsAny(message.MessageID+attachment.Key, "/?#") {
		return core.DownloadedAttachment{}, errors.New("feishu attachment invalid")
	}
	token, err := p.tenantToken(ctx)
	if err != nil {
		return core.DownloadedAttachment{}, err
	}
	base := strings.TrimRight(p.baseURL, "/")
	if base == "" {
		base = lark.FeishuBaseUrl
	}
	target := base + "/open-apis/im/v1/messages/" + url.PathEscape(message.MessageID) + "/resources/" + url.PathEscape(attachment.Key) + "?type=" + attachment.Kind
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return core.DownloadedAttachment{}, errors.New("feishu attachment request invalid")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := p.downloadClient.Do(req)
	if err != nil {
		return core.DownloadedAttachment{}, errors.New("feishu attachment download failed")
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return core.DownloadedAttachment{}, errors.New("feishu attachment download rejected")
	}
	contentType := resp.Header.Get("Content-Type")
	if err := attachmentJSONError(resp); err != nil {
		_ = resp.Body.Close()
		return core.DownloadedAttachment{}, err
	}
	name := attachment.FileName
	if _, params, err := mime.ParseMediaType(resp.Header.Get("Content-Disposition")); err == nil && params["filename"] != "" && name == "" {
		name = path.Base(params["filename"])
	}
	if name == "" {
		name = attachment.Kind
	}
	return core.DownloadedAttachment{Body: resp.Body, FileName: name, MimeType: contentType, Size: resp.ContentLength}, nil
}

// maxAttachmentErrorBytes 限制 JSON 错误探测的内存；文件前缀随后原样放回下载流。
const maxAttachmentErrorBytes = 4 << 10

// attachmentJSONError 区分真实文件响应与飞书 JSON 业务错误，不能只凭 MIME 拒绝 JSON 附件。
func attachmentJSONError(resp *http.Response) error {
	mediaType, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if !strings.EqualFold(mediaType, "application/json") {
		return nil
	}
	disposition, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Disposition"))
	if strings.EqualFold(disposition, "attachment") {
		return nil
	}
	prefix, err := io.ReadAll(io.LimitReader(resp.Body, maxAttachmentErrorBytes+1))
	if err != nil {
		return errors.New("feishu attachment download failed")
	}
	resp.Body = struct {
		io.Reader
		io.Closer
	}{io.MultiReader(bytes.NewReader(prefix), resp.Body), resp.Body}
	var envelope struct {
		Code *int    `json:"code"`
		Msg  *string `json:"msg"`
	}
	if len(prefix) <= maxAttachmentErrorBytes && json.Unmarshal(prefix, &envelope) == nil &&
		envelope.Code != nil && *envelope.Code != 0 && envelope.Msg != nil {
		return errors.New("feishu attachment unavailable")
	}
	return nil
}

// tenantToken 获取 tenant_access_token，供附件下载等应用身份调用使用。
func (p *Provider) tenantToken(ctx context.Context) (string, error) {
	token, _, err := p.BotToken(ctx)
	return token, err
}

// BotToken 返回 Bot 执行身份令牌（tenant_access_token）及其过期时间，供 Runtime 写入 Vault。
func (p *Provider) BotToken(ctx context.Context) (string, time.Time, error) {
	ctx, cancel := boundedContext(ctx)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return "", time.Time{}, err
	}
	if p.client == nil {
		return "", time.Time{}, errors.New("feishu token unavailable")
	}
	if token, expiry, ok := p.tenantTokens.load(time.Now()); ok {
		return token, expiry, nil
	}
	select {
	case p.tenantTokens.refresh <- struct{}{}:
	case <-ctx.Done():
		return "", time.Time{}, ctx.Err()
	}
	defer func() { <-p.tenantTokens.refresh }()
	if err := ctx.Err(); err != nil {
		return "", time.Time{}, err
	}
	// 等待刷新期间前一个持有者可能已写入缓存；必须在发请求前再次检查。
	if token, expiry, ok := p.tenantTokens.load(time.Now()); ok {
		return token, expiry, nil
	}
	started := time.Now()
	token, expiry, err := p.fetchTenantToken(ctx)
	if err != nil {
		return "", time.Time{}, err
	}
	margin := min(expiry.Sub(started)/10, 5*time.Minute)
	p.tenantTokens.mu.Lock()
	p.tenantTokens.token, p.tenantTokens.expiresAt, p.tenantTokens.refreshAt = token, expiry, expiry.Add(-margin)
	p.tenantTokens.mu.Unlock()
	return token, expiry, nil
}

type tenantTokenCache struct {
	mu                   sync.Mutex
	refresh              chan struct{}
	token                string
	expiresAt, refreshAt time.Time
}

func (c *tenantTokenCache) load(now time.Time) (string, time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.token, c.expiresAt, c.token != "" && now.Before(c.refreshAt)
}

var _ core.BotTokenSource = (*Provider)(nil)

func (p *Provider) fetchTenantToken(ctx context.Context) (string, time.Time, error) {
	if p.client == nil {
		return "", time.Time{}, errors.New("feishu token unavailable")
	}
	ctx, cancel := boundedContext(ctx)
	defer cancel()
	started := time.Now()
	resp, err := p.client.Post(ctx, "/open-apis/auth/v3/tenant_access_token/internal", map[string]string{"app_id": p.binding.AppID, "app_secret": p.appSecret}, larkcore.AccessTokenTypeNone)
	if err != nil || resp == nil || resp.StatusCode != http.StatusOK {
		return "", time.Time{}, errors.New("feishu token unavailable")
	}
	var body struct {
		Code   int    `json:"code"`
		Token  string `json:"tenant_access_token"`
		Expire int64  `json:"expire"`
	}
	if err := decodeBody(resp.RawBody, &body); err != nil || body.Code != 0 || body.Token == "" || body.Expire <= 0 || body.Expire > int64((1<<63-1)/time.Second) {
		return "", time.Time{}, errors.New("feishu token unavailable")
	}
	return body.Token, started.Add(time.Duration(body.Expire) * time.Second), nil
}

func decodeBody(data []byte, value any) error { return json.Unmarshal(data, value) }
