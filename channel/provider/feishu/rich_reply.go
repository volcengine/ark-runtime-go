// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

var (
	_ core.InlineMediaUploader = (*Provider)(nil)
	_ core.RichTextDeliverer   = (*Provider)(nil)
)

// UploadInlineMedia 上传图片或 MP4 为富文本资源，不创建可见消息。
func (p *Provider) UploadInlineMedia(ctx context.Context, artifact core.Artifact, content core.FileContent) (core.RichTextPart, error) {
	if p.client == nil || content.Body == nil || content.Size <= 0 {
		return core.RichTextPart{}, failure{"feishu media invalid", false, false, "invalid_media"}
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if artifact.Kind == "image" {
		if content.Size > maxImageBytes {
			return core.RichTextPart{}, failure{"feishu image too large", false, false, "media_too_large"}
		}
		reader := bufio.NewReader(content.Body)
		prefix, err := reader.Peek(512)
		if err != nil && err != io.EOF || !strings.HasPrefix(http.DetectContentType(prefix), "image/") {
			return core.RichTextPart{}, failure{"feishu image content invalid", false, false, "invalid_image"}
		}
		content.Body = io.NopCloser(reader)
		key, err := p.UploadInlineImage(ctx, content)
		return core.RichTextPart{Type: "image", ResourceKey: key}, err
	}
	if artifact.Kind != "video" || content.Size > 30<<20 {
		return core.RichTextPart{}, failure{"feishu media invalid", false, false, "invalid_media"}
	}
	reader := bufio.NewReader(content.Body)
	prefix, err := reader.Peek(12)
	if err != nil || string(prefix[4:8]) != "ftyp" {
		return core.RichTextPart{}, failure{"feishu video content invalid", false, false, "invalid_media_content"}
	}
	content.Body = io.NopCloser(reader)
	key, err := p.uploadArtifactFile(ctx, artifact, content, "mp4")
	if err != nil {
		return core.RichTextPart{}, err
	}
	if !strings.HasPrefix(key, "file_") {
		return core.RichTextPart{}, failure{"feishu media upload failed", false, true, "artifact_upload"}
	}
	return core.RichTextPart{Type: "video", ResourceKey: key}, nil
}

// DeliverRichText 按持久化顺序发送一条 post，图片和视频各占独立段落。
func (p *Provider) DeliverRichText(ctx context.Context, delivery core.Delivery) (string, error) {
	if p.client == nil || ctx == nil || delivery.UUID == "" || delivery.Body.Type != core.BodyRichText || !core.CanRichText(delivery.Body.Parts) {
		return "", failure{"feishu rich text invalid", false, false, "invalid_delivery"}
	}
	var paragraphs [][]map[string]string
	for _, part := range delivery.Body.Parts {
		var node map[string]string
		switch part.Type {
		case "text":
			node = map[string]string{"tag": "md", "text": part.Text}
		case "image":
			if !strings.HasPrefix(part.ResourceKey, "img_") {
				return "", failure{"feishu image invalid", false, false, "invalid_image"}
			}
			node = map[string]string{"tag": "img", "image_key": part.ResourceKey}
		case "video":
			if !strings.HasPrefix(part.ResourceKey, "file_") {
				return "", failure{"feishu media invalid", false, false, "invalid_media"}
			}
			node = map[string]string{"tag": "media", "file_key": part.ResourceKey}
		}
		paragraphs = append(paragraphs, []map[string]string{node})
	}
	content, err := json.Marshal(map[string]any{"zh_cn": map[string]any{"content": paragraphs}})
	if err != nil || len(content) > 28000 {
		return "", failure{"feishu rich text too large", false, false, "invalid_delivery"}
	}
	ctx, cancel := boundedContext(ctx)
	defer cancel()
	return p.send(ctx, delivery, "post", string(content))
}
