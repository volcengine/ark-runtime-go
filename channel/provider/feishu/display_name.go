// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"unicode/utf8"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
)

type displayNameError struct{ status, code int }

func (e displayNameError) Error() string {
	return fmt.Sprintf("feishu display name unavailable (http %d, code %d)", e.status, e.code)
}
func (e displayNameError) HTTPStatus() int   { return e.status }
func (e displayNameError) ErrorCode() string { return strconv.Itoa(e.code) }

// LookupBotName 读取当前应用机器人的展示名，不启动消息接收器。
func (p *Provider) LookupBotName(ctx context.Context) (string, error) {
	return p.lookupName(ctx, "/open-apis/bot/v3/info", "bot")
}

// LookupChatName 尽力读取当前应用可见的群名称，不启动消息接收器或申请额外权限。
func (p *Provider) LookupChatName(ctx context.Context, chatID string) (string, error) {
	return p.lookupDisplayName(ctx, chatID, false)
}

// LookupUserName 尽力按 open_id 读取展示名；缺 contact 权限时返回稳定错误供宿主保留 ID。
func (p *Provider) LookupUserName(ctx context.Context, openID string) (string, error) {
	return p.lookupDisplayName(ctx, openID, true)
}

func (p *Provider) lookupDisplayName(ctx context.Context, id string, user bool) (string, error) {
	if id == "" || len(id) > 256 || !utf8.ValidString(id) || p.client == nil {
		return "", errors.New("feishu display name identity invalid")
	}
	path := "/open-apis/im/v1/chats/" + url.PathEscape(id)
	kind := "chat"
	if user {
		path = "/open-apis/contact/v3/users/" + url.PathEscape(id) + "?user_id_type=open_id"
		kind = "user"
	}
	return p.lookupName(ctx, path, kind)
}

func (p *Provider) lookupName(ctx context.Context, path, kind string) (string, error) {
	if p.client == nil {
		return "", errors.New("feishu display name client unavailable")
	}
	ctx, cancel := boundedContext(ctx)
	defer cancel()
	response, err := p.client.Get(ctx, path, nil, larkcore.AccessTokenTypeTenant)
	if err != nil || response == nil {
		return "", errors.Join(errors.New("feishu display name unavailable"), ctx.Err())
	}
	if len(response.RawBody) > 64<<10 {
		return "", displayNameError{status: response.StatusCode}
	}
	var body struct {
		Code int `json:"code"`
		Bot  struct {
			AppName string `json:"app_name"`
		} `json:"bot"`
		Data struct {
			Name string `json:"name"`
			User struct {
				Name string `json:"name"`
			} `json:"user"`
		} `json:"data"`
	}
	if json.Unmarshal(response.RawBody, &body) != nil || response.StatusCode != http.StatusOK || body.Code != 0 {
		return "", displayNameError{status: response.StatusCode, code: body.Code}
	}
	name := body.Data.Name
	if kind == "user" {
		name = body.Data.User.Name
	}
	if kind == "bot" {
		name = body.Bot.AppName
	}
	if name == "" || utf8.RuneCountInString(name) > 256 {
		return "", errors.New("feishu display name invalid")
	}
	return name, nil
}
