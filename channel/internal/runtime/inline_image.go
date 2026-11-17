// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"net/url"
	"strings"

	"github.com/volcengine/ark-runtime-go/channel/internal/markdown"
)

// InlineImageUploader 将图片上传为当前 Provider 可在 Markdown 中引用的资源，不发送消息。
type InlineImageUploader interface {
	UploadInlineImage(context.Context, FileContent) (string, error)
}

// prepareAnswerImages 在结算前生成稳定正文；上传对象本身不产生可见消息，结算失败可安全重新准备。
// 只使用已经过轮次、目录和数量限制的产物；无法内嵌时保留完整引用，原附件仍按现有策略投递。
func (r *Runtime) prepareAnswerImages(ctx context.Context, j *job, answer string, artifacts []Artifact) (string, []Artifact) {
	if !j.binding.StreamFinalAnswer || !CanStreamText(answer) {
		return answer, artifacts
	}
	fallback := markdown.ReplaceImages(answer, func(image markdown.Image) string { return image.Code() })
	// 会话切换和挂载提示也是最终正文的一部分，必须在删除附件前计入两种视图的大小。
	if len(markdown.Images(answer)) > 0 {
		j.answerImageFallback = fallback
		for _, text := range []string{answer, fallback} {
			if replies := r.textDelivery(j, "answer", text); len(replies) != 1 || !CanStreamText(replies[0].Body.Text) {
				j.disableAnswerStream = true
				return answer, artifacts
			}
		}
	}
	uploader, supported := providerPort[InlineImageUploader](j.connection.Provider)
	available := make(map[string]Artifact, len(artifacts))
	if j.binding.DeliverArtifacts && supported {
		for _, artifact := range artifacts {
			if artifact.Kind == "image" {
				available[artifact.key()] = artifact
				if artifact.Path != "" {
					available["path:"+artifact.Path] = artifact
				}
			}
		}
	}
	references := map[string]string{}
	embedded := map[string]bool{}
	text := markdown.ReplaceImages(answer, func(image markdown.Image) string {
		key := image.Destination
		if decoded, err := url.PathUnescape(key); err == nil {
			key = decoded
		}
		if normalized, ok := normalizeOutputPath(key); ok {
			key = "path:" + normalized
		}
		artifact, allowed := available[key]
		if !allowed {
			return image.Code()
		}
		key = artifact.key()
		reference, attempted := references[key]
		if !attempted {
			reference = r.uploadAnswerImage(ctx, j, uploader, artifact)
			references[key] = reference
		}
		if reference == "" {
			return image.Code()
		}
		embedded[key] = true
		return image.WithDestination(reference)
	})
	if replies := r.textDelivery(j, "answer", text); len(replies) != 1 || !CanStreamText(replies[0].Body.Text) {
		// 转换超限时沿用完整普通文本及原附件，不能把图片 key 当成已成功展示。
		j.disableAnswerStream = true
		return answer, artifacts
	}
	var remaining []Artifact
	for _, artifact := range artifacts {
		if !embedded[artifact.key()] {
			remaining = append(remaining, artifact)
		}
	}
	return text, remaining
}

func (r *Runtime) uploadAnswerImage(ctx context.Context, j *job, uploader InlineImageUploader, artifact Artifact) string {
	if j.checkLease(ctx) != nil {
		return ""
	}
	var content FileContent
	var err error
	if artifact.Path != "" && artifact.FileID == "" {
		d := Delivery{Body: OutputFileBody(artifact), OutputSessionID: j.Session.SessionID}
		content, _, err = r.downloadOutput(ctx, j.connection, d)
	} else {
		content, _, err = r.downloadArtifact(ctx, j.connection, artifact)
	}
	if err != nil {
		return ""
	}
	if content.Body == nil {
		return ""
	}
	defer func() { _ = content.Body.Close() }()
	if j.checkLease(ctx) != nil {
		return ""
	}
	reference, err := uploader.UploadInlineImage(ctx, content)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(reference)
}
