// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"unicode"
)

// AttachmentLimits 限制单条消息可处理的附件。
type AttachmentLimits struct {
	// MaxCount 是单条消息最多处理的附件数，默认 20。
	MaxCount int `json:"max_count"`
	// MaxBytes 是单个附件的最大字节数，默认 99,999,999 字节（严格小于十进制 100 MB）。
	MaxBytes int64 `json:"max_bytes"`
	// MaxTotalBytes 是单条消息附件的总字节上限，默认 500 MB（十进制）。
	MaxTotalBytes int64 `json:"max_total_bytes"`
}

func (l AttachmentLimits) normalize() (AttachmentLimits, error) {
	if l.MaxCount == 0 {
		l.MaxCount = 20
	}
	if l.MaxBytes == 0 {
		l.MaxBytes = 99_999_999
		// 仅配置总额的旧配置继续有效，缺省单文件上限不超过显式总额。
		if l.MaxTotalBytes > 0 {
			l.MaxBytes = min(l.MaxBytes, l.MaxTotalBytes)
		}
	}
	if l.MaxTotalBytes == 0 {
		l.MaxTotalBytes = 500_000_000
	}
	if l.MaxCount < 0 || l.MaxCount > 20 || l.MaxBytes < 0 || l.MaxBytes > 512<<20 || l.MaxTotalBytes < l.MaxBytes || l.MaxTotalBytes > 2<<30 {
		return AttachmentLimits{}, ErrRuntimeConfig
	}
	return l, nil
}

// DownloadedAttachment 是 Provider 拉取到的附件内容；Body 由调用方关闭。
type DownloadedAttachment struct {
	Body     io.ReadCloser
	FileName string
	MimeType string
	// Size 为 -1 表示未知，读取时按上限截断判错。
	Size int64
}

// AttachmentFetcher 是 Provider 可选实现的附件下载端口。实现必须以应用身份下载、限制单次读取
// 大小，且不得把资源内容写入日志。
type AttachmentFetcher interface {
	FetchAttachment(ctx context.Context, message Message, attachment Attachment) (DownloadedAttachment, error)
}

// FileUpload 是上传到 Ark Files 的请求；Reader 由调用方关闭。
type FileUpload struct {
	Reader   io.Reader
	FileName string
	MimeType string
	// Purpose 为 user_data（模型输入，默认）或 agent（挂载到 Session 工作目录）。
	Purpose string
}

// FileUploader 是 MAClient 可选实现的 Files 上传端口，返回可被模型或资源挂载引用的 file_id。
type FileUploader interface {
	UploadFile(ctx context.Context, upload FileUpload) (string, error)
}

// FileURLResolver 用宿主身份为 self_hosted 附件获取预签名下载地址，不向 Agent 暴露 API Key。
// 返回的链接须在当前 Turn 生命周期内有效，并作为敏感输入持久化。
type FileURLResolver interface {
	ResolveFileURL(context.Context, string) (string, error)
}

// errAttachmentRejected 表示附件被限额或类型策略拒绝；这是确定性的用户侧问题，不重试。
var errAttachmentRejected = errors.New("attachment rejected")

// attachmentLimitError 保留可安全展示的限额事实，错误分类继续兼容 attachment_rejected。
type attachmentLimitError struct {
	kind    string
	index   int
	actual  int64
	limit   int64
	atLeast bool
}

func (e *attachmentLimitError) Error() string {
	return fmt.Sprintf("attachment %s limit exceeded: actual=%d limit=%d", e.kind, e.actual, e.limit)
}

func (e *attachmentLimitError) Unwrap() error { return errAttachmentRejected }

func (e *attachmentLimitError) reply() string {
	if e.kind == "count" {
		return fmt.Sprintf("附件数量超出限制：本次 %d 个，最多支持 %d 个。请减少附件后重试。", e.actual, e.limit)
	}
	amount := fmt.Sprintf("%d bytes", e.actual)
	if e.atLeast {
		amount = "已读取至少 " + amount
	}
	if e.kind == "file_size" {
		return fmt.Sprintf("单个附件大小超出限制：第 %d 个附件（%s），上限 %d bytes。请缩小附件后重试。", e.index, amount, e.limit)
	}
	if !e.atLeast {
		amount = "已累计 " + amount
	}
	return fmt.Sprintf("附件总大小超出限制：%s，合计上限 %d bytes。请减少或缩小附件后重试。", amount, e.limit)
}

// attachmentFailureReply 只展示本地生成的限额说明，不回显下载或上传服务的错误正文。
func attachmentFailureReply(err error) string {
	var limitErr *attachmentLimitError
	if errors.As(err, &limitErr) {
		return limitErr.reply()
	}
	return replyAttachment
}

// blockType 依据 MIME 与扩展名决定模型输入块类型：图片、视频与 PDF 有对应模型输入块；其余格式
// 模型无法直接读取，返回 notice，由环境与挂载结果决定说明、路径或下载链接。
func blockType(mime, fileName string) string {
	mime = strings.ToLower(strings.TrimSpace(strings.SplitN(mime, ";", 2)[0]))
	switch {
	case strings.HasPrefix(mime, "image/"):
		return "image"
	case strings.HasPrefix(mime, "video/"):
		return "video"
	case mime == "application/pdf":
		return "document"
	}
	switch strings.ToLower(path.Ext(fileName)) {
	case ".png", ".jpg", ".jpeg", ".gif", ".webp":
		return "image"
	case ".mp4", ".mov", ".avi":
		return "video"
	case ".pdf":
		return "document"
	}
	return InputBlockNotice
}

// maxNoticeNameRunes 是 notice 文本中文件名的最大字符数。
const maxNoticeNameRunes = 120

// noticeName 把文件名清洗为单行、有限长度的展示文本：控制字符替换为空格，避免伪造多行指令。
func noticeName(name string) string {
	cleaned := strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, name)
	cleaned = strings.Join(strings.Fields(cleaned), " ")
	if runes := []rune(cleaned); len(runes) > maxNoticeNameRunes {
		cleaned = string(runes[:maxNoticeNameRunes-1]) + "…"
	}
	if cleaned == "" {
		return "未命名文件"
	}
	return cleaned
}

// countingReader 在超过上限时返回错误，防止大文件被完整读入。
type countingReader struct {
	reader io.Reader
	limit  int64
	read   int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.read += int64(n)
	if r.read > r.limit {
		return n, errAttachmentRejected
	}
	return n, err
}

// prepareAttachments 下载并上传附件，返回需冻结的输入块。任一附件失败即整体失败：部分附件
// 输入会让 Agent 基于不完整信息作答。
func (r *Runtime) prepareAttachments(ctx context.Context, j *job, connection Connection) ([]InputBlock, error) {
	attachments := j.msg.Attachments
	fetcher, ok := providerPort[AttachmentFetcher](connection.Provider)
	uploader, uok := connection.MAClient.(FileUploader)
	if !ok || !uok {
		return nil, errAttachmentRejected
	}
	limits := r.options.Attachments
	if len(attachments) > limits.MaxCount {
		return nil, &attachmentLimitError{kind: "count", actual: int64(len(attachments)), limit: int64(limits.MaxCount)}
	}
	blocks := make([]InputBlock, 0, len(attachments))
	var total int64
	for index, attachment := range attachments {
		downloaded, err := fetcher.FetchAttachment(ctx, j.msg, attachment)
		if err != nil {
			return nil, err
		}
		name := downloaded.FileName
		if name == "" {
			name = attachment.FileName
		}
		if name == "" {
			name = attachment.Kind
		}
		kind := blockType(downloaded.MimeType, name)
		if attachment.Kind == "image" {
			kind = "image"
		}
		// 所有上传事实与 Environment 解耦；即使本代只显示说明，换代后仍可复用文件。
		if downloaded.Size > limits.MaxBytes {
			_ = downloaded.Body.Close()
			return nil, &attachmentLimitError{kind: "file_size", index: index + 1, actual: downloaded.Size, limit: limits.MaxBytes}
		}
		if downloaded.Size > 0 && total+downloaded.Size > limits.MaxTotalBytes {
			_ = downloaded.Body.Close()
			return nil, &attachmentLimitError{kind: "total_size", actual: total + downloaded.Size, limit: limits.MaxTotalBytes}
		}
		remaining := limits.MaxTotalBytes - total
		if remaining > limits.MaxBytes {
			remaining = limits.MaxBytes
		}
		counter := &countingReader{reader: downloaded.Body, limit: remaining}
		if err := j.checkLease(ctx); err != nil {
			_ = downloaded.Body.Close()
			return nil, err
		}
		purpose := "user_data"
		if kind == InputBlockNotice {
			purpose = "agent"
		}
		fileID, err := uploader.UploadFile(ctx, FileUpload{Reader: counter, FileName: name, MimeType: downloaded.MimeType, Purpose: purpose})
		_ = downloaded.Body.Close()
		if counter.read > remaining {
			if remaining == limits.MaxBytes {
				return nil, &attachmentLimitError{kind: "file_size", index: index + 1, actual: counter.read, limit: limits.MaxBytes, atLeast: true}
			}
			return nil, &attachmentLimitError{kind: "total_size", actual: total + counter.read, limit: limits.MaxTotalBytes, atLeast: true}
		}
		if err != nil {
			return nil, err
		}
		total += counter.read
		block := InputBlock{Type: kind, Key: attachment.Key, FileID: fileID, Title: noticeName(name), MimeType: downloaded.MimeType}
		blocks = append(blocks, block)
	}
	return blocks, nil
}

// freezeInput 在首次提交前准备附件并持久冻结。已冻结或没有附件时直接返回；失败返回错误分类：
// attachment_rejected（用户侧确定性失败）或 attachment_unavailable（可重试）。
func (r *Runtime) freezeInput(ctx context.Context, j *job, connection Connection) (string, error) {
	if len(j.msg.Attachments) == 0 || j.Input.BlocksFrozen {
		return "", nil
	}
	blocks, err := r.prepareAttachments(ctx, j, connection)
	if err != nil {
		status := "failed"
		kind := "attachment_unavailable"
		if errors.Is(err, errAttachmentRejected) {
			status, kind = "rejected", "attachment_rejected"
		}
		r.reportTurn(EventAttachment, status, kind, j)
		return kind, err
	}
	if err := r.updateTurn(ctx, j, TurnUpdate{Phase: j.Phase, Session: j.Session, Submit: j.Submit, Input: FrozenInput{BlocksFrozen: true, Blocks: blocks}}); err != nil {
		return "", err
	}
	r.reportTurn(EventAttachment, "prepared", "", j)
	return "", nil
}
