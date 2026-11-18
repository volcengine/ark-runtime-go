// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package ma

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"unicode"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/session"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// wireResource 是公开 Session Resource 的响应形状（字段名与 resource.thrift 对齐）。
type wireResource struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	FileID    string `json:"file_id"`
	MountPath string `json:"mount_path"`
}

// wireResourceList 是 ListSessionResources 的响应外层。
type wireResourceList struct {
	Data []wireResource `json:"data"`
}

// CreateSessionResource 通过公开接口把已上传的文件挂载到 Session 沙箱。
// 结果未知时调用方必须先用 ListSessionResources 对账，不得直接重试。
func (c *Client) CreateSessionResource(ctx context.Context, sessionID, fileID, mountPath string) (core.SessionResource, error) {
	path, err := sessionPath(sessionID)
	if err != nil {
		return core.SessionResource{}, err
	}
	if fileID == "" || !flatResourceInput(mountPath) {
		return core.SessionResource{}, errors.New("ma session resource invalid")
	}
	body := map[string]string{"file_id": fileID, "type": "file", "mount_path": mountPath}
	request, err := sdkBody[session.CreateSessionResourceRequest](body)
	if err != nil {
		return core.SessionResource{}, err
	}
	result, err := requestSDK(c, ctx, http.MethodPost, path+"/resources", sdkCallOptions{}, func(ctx context.Context, sdk *arkruntime.Client, options []arkruntime.RequestOption) (*session.SessionResource, error) {
		// The current generated create model has no mount_path field.
		options = append(options, arkruntime.WithExtraBody(map[string]interface{}{"mount_path": mountPath}))
		return sdk.CreateSessionResource(ctx, sessionID, request, options...)
	})
	if err != nil {
		return core.SessionResource{}, err
	}
	var resource wireResource
	if err := sdkProjection(result, &resource); err != nil {
		return core.SessionResource{}, err
	}
	return toSessionResource(resource, mountPath)
}

// ListSessionResources 读取 Session 的全部挂载资源，用于创建结果未知时的对账。
func (c *Client) ListSessionResources(ctx context.Context, sessionID string) ([]core.SessionResource, error) {
	path, err := sessionPath(sessionID)
	if err != nil {
		return nil, err
	}
	result, err := requestSDK(c, ctx, http.MethodGet, path+"/resources", sdkCallOptions{}, func(ctx context.Context, sdk *arkruntime.Client, options []arkruntime.RequestOption) (*session.ListSessionResourcesResponse, error) {
		return sdk.ListSessionResources(ctx, sessionID, options...)
	})
	if err != nil {
		return nil, err
	}
	var list wireResourceList
	if err := sdkProjection(result, &list); err != nil {
		return nil, err
	}
	resources := make([]core.SessionResource, 0, len(list.Data))
	for _, resource := range list.Data {
		if resource.Type != "file" {
			continue
		}
		converted, err := toSessionResource(resource, resource.MountPath)
		if err != nil {
			return nil, err
		}
		resources = append(resources, converted)
	}
	return resources, nil
}

func toSessionResource(resource wireResource, mountPath string) (core.SessionResource, error) {
	if resource.MountPath != "" {
		mountPath = resource.MountPath
	}
	// Console 的真实响应为裸绝对路径；兼容旧适配器的 file URI 与相对路径。
	// 已有资源可以包含合法子目录；只有本 SDK 的 Create 输入要求平铺文件名。
	const absolutePrefix = "/mnt/session/uploads/"
	relative := mountPath
	actual := mountPath
	switch {
	case strings.HasPrefix(mountPath, "file://"+absolutePrefix):
		relative = strings.TrimPrefix(mountPath, "file://"+absolutePrefix)
	case strings.HasPrefix(mountPath, absolutePrefix):
		relative = strings.TrimPrefix(mountPath, absolutePrefix)
	case strings.HasPrefix(mountPath, "/"), strings.Contains(mountPath, ":"):
		return core.SessionResource{}, errors.New("ma session resource invalid mount path")
	default:
		actual = core.MountedActualPath(relative)
	}
	if strings.Contains(relative, "\\") || strings.IndexFunc(relative, unicode.IsControl) >= 0 {
		return core.SessionResource{}, errors.New("ma session resource invalid mount path")
	}
	for _, segment := range strings.Split(relative, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return core.SessionResource{}, errors.New("ma session resource invalid mount path")
		}
	}
	return core.SessionResource{
		ID:         resource.ID,
		FileID:     resource.FileID,
		MountPath:  relative,
		ActualPath: actual,
	}, nil
}

func flatResourceInput(name string) bool {
	return strings.TrimSpace(name) != "" && !strings.ContainsAny(name, "/\\") && !strings.Contains(name, "..") && strings.IndexFunc(name, unicode.IsControl) < 0
}
