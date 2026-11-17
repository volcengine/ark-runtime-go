// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"context"
	"encoding/json"
	"path"
	"strings"
	"time"

	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

var (
	_ core.ArtifactDeliverer     = (*Provider)(nil)
	_ core.ArtifactLinkDeliverer = (*Provider)(nil)
	_ core.InlineImageUploader   = (*Provider)(nil)
)

// maxImageBytes 是飞书图片上传上限（10 MiB）；更大的图片按文件发送。
const maxImageBytes = 10 << 20

// UploadInlineImage 只上传图片并返回引用，正文渲染与消息投递分别由各自租约控制。
func (p *Provider) UploadInlineImage(ctx context.Context, content core.FileContent) (string, error) {
	if p.client == nil || content.Body == nil || content.Size <= 0 || content.Size > maxImageBytes {
		return "", failure{"feishu inline image invalid", false, false, "invalid_image"}
	}
	resp, err := p.client.Im.Image.Create(ctx, larkim.NewCreateImageReqBuilder().Body(larkim.NewCreateImageReqBodyBuilder().ImageType("message").Image(content.Body).Build()).Build())
	if err != nil {
		return "", transportFailure(err)
	}
	if resp == nil || resp.ApiResp == nil || !resp.Success() || resp.Data == nil || resp.Data.ImageKey == nil || !strings.HasPrefix(*resp.Data.ImageKey, "img_") {
		return "", failure{"feishu image upload failed", false, true, "artifact_upload"}
	}
	return *resp.Data.ImageKey, nil
}

// DeliverArtifact 把产物上传到飞书并按回复目标发送图片、视频或文件消息。
//
// 图片在大小已知且不超过 10 MiB 时发图片消息；mp4 视频上传为 file_type=mp4 后发 media 消息
// （封面可选，未设置时飞书显示默认封面）；其余发文件消息。上传得到的 image_key / file_key
// 不持久化：重试时重新上传，消息发送以 delivery.UUID 去重，因此不会产生重复消息，只可能多出
// 未引用的上传对象。
func (p *Provider) DeliverArtifact(ctx context.Context, delivery core.Delivery, content core.FileContent) (string, error) {
	if p.client == nil || delivery.UUID == "" || !delivery.Body.IsArtifact() || content.Body == nil {
		return "", failure{"feishu artifact invalid", false, false, "invalid_delivery"}
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	artifact := delivery.Body.Artifact
	name := safeFileName(content.FileName, &artifact)
	if artifact.Kind == "image" && content.Size > 0 && content.Size <= maxImageBytes {
		resp, err := p.client.Im.Image.Create(ctx, larkim.NewCreateImageReqBuilder().Body(larkim.NewCreateImageReqBodyBuilder().ImageType("message").Image(content.Body).Build()).Build())
		if err != nil {
			return "", transportFailure(err)
		}
		if resp == nil || resp.ApiResp == nil || !resp.Success() || resp.Data == nil || resp.Data.ImageKey == nil {
			return "", failure{"feishu image upload failed", false, true, "artifact_upload"}
		}
		payload, _ := json.Marshal(map[string]string{"image_key": *resp.Data.ImageKey})
		return p.send(ctx, delivery, "image", string(payload))
	}
	fileType := feishuFileType(name)
	key, err := p.uploadArtifactFile(ctx, artifact, content, fileType)
	if err != nil {
		return "", err
	}
	payload, _ := json.Marshal(map[string]string{"file_key": key})
	if fileType == "mp4" {
		return p.send(ctx, delivery, "media", string(payload))
	}
	return p.send(ctx, delivery, "file", string(payload))
}

// DeliverArtifactLink 以文本消息发送超过文件上限的产物下载链接；链接不进日志与错误信息。
func (p *Provider) DeliverArtifactLink(ctx context.Context, delivery core.Delivery, name, url string) (string, error) {
	if p.client == nil || delivery.UUID == "" || !delivery.Body.IsArtifact() || !strings.HasPrefix(url, "https://") && !strings.HasPrefix(url, "http://") {
		return "", failure{"feishu artifact link invalid", false, false, "invalid_delivery"}
	}
	artifact := delivery.Body.Artifact
	name = safeFileName(name, &artifact)
	content, err := json.Marshal(struct {
		Text string `json:"text"`
	}{Text: "文件「" + name + "」超过飞书发送上限，请通过链接下载：\n" + url})
	if err != nil {
		return "", failure{"feishu artifact link invalid", false, false, "invalid_delivery"}
	}
	ctx, cancel := boundedContext(ctx)
	defer cancel()
	return p.send(ctx, delivery, "text", string(content))
}

// feishuFileType 映射飞书文件上传的 file_type；未知类型用 stream。
func feishuFileType(name string) string {
	switch strings.ToLower(path.Ext(name)) {
	case ".pdf":
		return "pdf"
	case ".doc", ".docx":
		return "doc"
	case ".xls", ".xlsx", ".csv":
		return "xls"
	case ".ppt", ".pptx":
		return "ppt"
	case ".mp4":
		return "mp4"
	case ".opus":
		return "opus"
	}
	return "stream"
}

func safeFileName(name string, artifact *core.Artifact) string {
	if strings.TrimSpace(name) == "" {
		name = artifact.FileName
	}
	name = path.Base(strings.ReplaceAll(strings.TrimSpace(name), "\\", "/"))
	if name == "" || name == "." || name == "/" {
		name = artifact.FileID
		if name == "" {
			name = "file"
		}
		switch artifact.Kind {
		case "image":
			name += ".png"
		case "video":
			name += ".mp4"
		}
	}
	if len(name) > 200 {
		name = name[len(name)-200:]
	}
	return name
}
