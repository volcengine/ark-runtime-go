// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package ma

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/environment"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// wireEnvironment 是 GET /api/v3/environments/:id 的响应形状（只取合并所需字段）。
type wireEnvironment struct {
	Config struct {
		Type string            `json:"type"`
		Env  map[string]string `json:"env"`
	} `json:"config"`
}

// GetEnvironment 读取公开 Environment 快照的 config.type 与明文 config.env。
// 不读取 networking/packages/setup_script/tos 等字段（这些在 environment_with_overrides
// 创建时不参与，由平台保持原值）。
func (c *Client) GetEnvironment(ctx context.Context, id string) (core.EnvironmentConfig, error) {
	if strings.TrimSpace(id) == "" || !validVaultID(id) {
		return core.EnvironmentConfig{}, errors.New("environment id required")
	}
	result, err := requestSDK(c, ctx, http.MethodGet, "/environments/"+id, sdkCallOptions{}, func(ctx context.Context, sdk *arkruntime.Client, options []arkruntime.RequestOption) (*environment.Environment, error) {
		return sdk.GetEnvironment(ctx, id, options...)
	})
	if err != nil {
		return core.EnvironmentConfig{}, err
	}
	var wire wireEnvironment
	if err := sdkProjection(result, &wire); err != nil {
		return core.EnvironmentConfig{}, err
	}
	return core.EnvironmentConfig{Type: wire.Config.Type, Env: wire.Config.Env}, nil
}

var _ core.EnvironmentReader = (*Client)(nil)
