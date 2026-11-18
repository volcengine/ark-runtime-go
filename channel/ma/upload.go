// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package ma

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/file"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// UploadFile 使用 SDK 流式上传；原始 Reader 的生命周期由调用方管理。
func (c *Client) UploadFile(ctx context.Context, upload core.FileUpload) (string, error) {
	if upload.Reader == nil || strings.TrimSpace(upload.FileName) == "" {
		return "", errors.New("ma upload invalid")
	}
	purpose := upload.Purpose
	if purpose == "" {
		purpose = "user_data"
	}
	if purpose != "user_data" && purpose != "agent" {
		return "", errors.New("ma upload purpose unsupported")
	}
	result, err := requestSDK(c, ctx, http.MethodPost, "/files", sdkCallOptions{Timeout: c.uploadTimeout}, func(ctx context.Context, sdk *arkruntime.Client, options []arkruntime.RequestOption) (*file.FileObject, error) {
		return sdk.UploadFileStream(ctx, &file.FileCreateRequest{Purpose: file.Purpose(purpose), ExpireAt: file.NewOptInt64(time.Now().Add(30 * 24 * time.Hour).Unix())}, io.NopCloser(upload.Reader), arkruntime.FileUploadOptions{FileName: sanitizeFileName(upload.FileName), ContentType: upload.MimeType, MaxBytes: maxUploadBytes}, options...)
	})
	if err != nil {
		return "", err
	}
	var object fileObject
	if err := sdkProjection(result, &object); err != nil {
		return "", err
	}
	return c.waitFileActive(ctx, object)
}
