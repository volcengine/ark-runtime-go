// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
)

// RichTextPart 是已准备好的文字或媒体片段；资源 key 只在加密投递正文中保存。
type RichTextPart struct {
	Type        string `json:"type"`
	Text        string `json:"text,omitempty"`
	ResourceKey string `json:"resource_key,omitempty"`
}

type wireRichTextBody struct {
	Type  BodyType       `json:"type"`
	Parts []RichTextPart `json:"parts"`
}

// InlineMediaUploader 上传媒体，不发送消息；用于最终回复结算前准备稳定资源引用。
type InlineMediaUploader interface {
	UploadInlineMedia(context.Context, Artifact, FileContent) (RichTextPart, error)
}

// RichTextDeliverer 将持久化片段作为一条富文本回复投递，必须复用 Delivery UUID 去重。
type RichTextDeliverer interface {
	DeliverRichText(context.Context, Delivery) (string, error)
}

// RichTextBody 构造完整最终答案的富文本载荷。
func RichTextBody(parts []RichTextPart) DeliveryBody {
	return DeliveryBody{Type: BodyRichText, Parts: parts}
}

// CanRichText 校验媒体片段与保守的正文预算，为渠道 JSON 包装预留空间。
func CanRichText(parts []RichTextPart) bool {
	media := false
	for _, p := range parts {
		switch p.Type {
		case "text":
			if p.ResourceKey != "" {
				return false
			}
		case "image", "video":
			if p.ResourceKey == "" || len(p.ResourceKey) > 512 || p.Text != "" {
				return false
			}
			media = true
		default:
			return false
		}
	}
	b, err := json.Marshal(parts)
	escaped, escapeErr := json.Marshal(string(b))
	return media && err == nil && escapeErr == nil && len(escaped) <= 24000
}

func mediaMimeMismatch(a Artifact) bool {
	mime := strings.ToLower(strings.TrimSpace(strings.Split(a.MimeType, ";")[0]))
	if mime == "" || mime == "application/octet-stream" {
		return false
	}
	return a.Kind == "image" && !strings.HasPrefix(mime, "image/") || a.Kind == "video" && mime != "video/mp4"
}

func (r *Runtime) prepareRichReply(ctx context.Context, j *job, answer string, artifacts []Artifact) (string, []Artifact) {
	uploader, ok := providerPort[InlineMediaUploader](j.connection.Provider)
	_, deliverable := providerPort[RichTextDeliverer](j.connection.Provider)
	if !ok || !deliverable || !j.binding.DeliverArtifacts || len(j.answerReferences) == 0 {
		return answer, artifacts
	}
	available := make(map[string]Artifact)
	for _, a := range artifacts {
		if a.Kind == "image" || a.Kind == "video" {
			available[a.key()] = a
		}
	}
	prepared := make(map[string]RichTextPart)
	attempted, embedded, invalid := make(map[string]bool), make(map[string]bool), make(map[string]bool)
	var parts []RichTextPart
	appendText := func(text string) {
		if text == "" {
			return
		}
		if len(parts) > 0 && parts[len(parts)-1].Type == "text" {
			parts[len(parts)-1].Text += text
			return
		}
		parts = append(parts, RichTextPart{Type: "text", Text: text})
	}
	if notices := r.textDelivery(j, "answer", ""); len(notices) == 1 {
		appendText(notices[0].Body.Text + "\n\n")
	}
	position := 0
	for _, ref := range j.answerReferences {
		a, allowed := available[ref.Artifact.key()]
		if !allowed {
			continue
		}
		key := a.key()
		if mediaMimeMismatch(a) {
			invalid[key] = true
			continue
		}
		if !attempted[key] {
			attempted[key] = true
			if content, _, err := r.downloadArtifact(ctx, j.connection, a); err == nil && content.Body != nil {
				if j.checkLease(ctx) == nil {
					part, err := uploader.UploadInlineMedia(ctx, a, content)
					var failure DeliveryFailure
					if errors.As(err, &failure) && (failure.ErrorKind() == "invalid_image" || failure.ErrorKind() == "invalid_media_content") {
						invalid[key] = true
					}
					if err == nil && part.Type == a.Kind && part.ResourceKey != "" {
						prepared[key] = part
					}
				}
				_ = content.Body.Close()
			}
		}
		part, uploaded := prepared[key]
		if !uploaded {
			continue
		}
		appendText(answer[position:ref.Start])
		parts = append(parts, part)
		position = ref.End
		embedded[key] = true
	}
	appendText(answer[position:])
	var invalidNames []string
	var remaining []Artifact
	seen := make(map[string]bool)
	for _, a := range artifacts {
		if seen[a.key()] {
			continue
		}
		seen[a.key()] = true
		if invalid[a.key()] {
			invalidNames = append(invalidNames, a.FileName)
			continue
		}
		if !embedded[a.key()] {
			remaining = append(remaining, a)
		}
	}
	if len(invalidNames) > 0 {
		notice := "\n\n（文件「" + strings.Join(invalidNames, "、") + "」的实际类型与图片/视频不符，未发送。）"
		answer += notice
		appendText(notice)
	}
	if !CanRichText(parts) {
		// 预算超限时保留正文和所有有效附件，上传对象未产生可见消息。
		if len(invalidNames) == 0 {
			return answer, artifacts
		}
		var valid []Artifact
		for _, a := range artifacts {
			if !invalid[a.key()] {
				valid = append(valid, a)
			}
		}
		return answer, valid
	}
	j.richAnswer = parts
	j.disableAnswerStream = true
	return answer, remaining
}
