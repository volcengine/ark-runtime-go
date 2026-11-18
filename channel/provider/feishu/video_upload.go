// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"context"
	"io"
	"os"

	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"

	"github.com/volcengine/ark-runtime-go/channel/internal/media"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

const maxVideoProbeBytes = 30 << 20

// prepareVideoReader 复用可定位输入；流式输入仅缓存有界前缀，探测后完整重放原视频。
func prepareVideoReader(ctx context.Context, content core.FileContent) (io.Reader, int, func(), error) {
	noop := func() {}
	if err := ctx.Err(); err != nil {
		return nil, 0, noop, err
	}
	if content.Size > maxVideoProbeBytes {
		return content.Body, 0, noop, nil
	}
	if reader, ok := content.Body.(io.ReadSeeker); ok {
		position, err := reader.Seek(0, io.SeekCurrent)
		if err != nil {
			return reader, 0, noop, nil
		}
		duration, _ := media.MP4DurationMS(reader)
		if _, err := reader.Seek(position, io.SeekStart); err != nil {
			return nil, 0, noop, err
		}
		return reader, duration, noop, nil
	}
	file, err := os.CreateTemp("", "ark-channel-video-*")
	if err != nil {
		// 尚未读取原输入，临时目录不可用时仍可沿用不带时长的上传。
		return content.Body, 0, noop, nil
	}
	cleanup := func() { _ = file.Close(); _ = os.Remove(file.Name()) }
	count, err := io.Copy(file, io.LimitReader(videoContextReader{ctx: ctx, reader: content.Body}, maxVideoProbeBytes+1))
	if err != nil {
		cleanup()
		return nil, 0, noop, err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		cleanup()
		return nil, 0, noop, err
	}
	if count > maxVideoProbeBytes {
		return io.MultiReader(file, content.Body), 0, cleanup, nil
	}
	duration, _ := media.MP4DurationMS(file)
	return file, duration, cleanup, nil
}

type videoContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r videoContextReader) Read(buffer []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(buffer)
}

// uploadArtifactFile 统一两类视频消息的文件上传，时长未知时省略表单字段。
func (p *Provider) uploadArtifactFile(ctx context.Context, artifact core.Artifact, content core.FileContent, fileType string) (string, error) {
	var reader io.Reader = content.Body
	duration := 0
	if fileType == "mp4" {
		var cleanup func()
		var err error
		reader, duration, cleanup, err = prepareVideoReader(ctx, content)
		if err != nil {
			return "", failure{"feishu video read failed", false, true, "artifact_download"}
		}
		defer cleanup()
	}
	body := larkim.NewCreateFileReqBodyBuilder().FileType(fileType).FileName(safeFileName(content.FileName, &artifact)).File(reader)
	if duration > 0 {
		body.Duration(duration)
	}
	resp, err := p.client.Im.File.Create(ctx, larkim.NewCreateFileReqBuilder().Body(body.Build()).Build())
	if err != nil {
		return "", transportFailure(err)
	}
	if resp == nil || resp.ApiResp == nil || !resp.Success() || resp.Data == nil || resp.Data.FileKey == nil {
		return "", failure{"feishu file upload failed", false, true, "artifact_upload"}
	}
	return *resp.Data.FileKey, nil
}
