// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"strings"
)

// MountedFile 保存附件在具体 Session 的挂载结果，或 self_hosted 的跳过挂载与下载链接。
// 冻结后重试复用；Session 换代后旧记录失效，需要重新准备并冻结新记录。
type MountedFile struct {
	StoredRecord `json:"-"`
	// MessageID 是附件所属的渠道消息 ID，与 Key 一起唯一标识一个挂载来源。
	MessageID string `json:"message_id,omitempty"`
	// Key 是渠道资源键（如飞书 file_key），与 Message.Attachments 对应。
	Key string `json:"key,omitempty"`
	// FileID 是 Ark Files 的文件 ID。
	FileID string `json:"file_id"`
	// SessionID 是该挂载实际所属的 Session，换代后必须重新挂载。
	SessionID string `json:"session_id,omitempty"`
	// RegisteredFileID 是平台将源文件复制到 Session 后重新注册的文件 ID。
	RegisteredFileID string `json:"registered_file_id,omitempty"`
	// ReconciledByPath 表示未知结果只通过稳定完整路径找回；平台未回传源 file_id，
	// 因而该记录不能独立证明源文件内容，也无法识别同租户主动替换同一路径。
	ReconciledByPath bool `json:"reconciled_by_path,omitempty"`
	// ResourceID 是 Session 资源 ID；结果未知、仅靠 List 对账到已挂载记录时可能为空。
	ResourceID string `json:"resource_id,omitempty"`
	// MountPath 是请求挂载时使用的平铺路径（仅文件名，不含目录分隔符）。
	MountPath string `json:"mount_path"`
	// ActualPath 是沙箱内访问该文件的实际路径。
	ActualPath string `json:"actual_path"`
	// Skipped 为 true 表示当前环境未挂载；self_hosted 使用 DownloadURL 提供下载指引。
	Skipped bool `json:"skipped,omitempty"`
	// DownloadURL 是 self_hosted 无法挂载时的预签名下载地址，随本代输入加密冻结。
	DownloadURL string `json:"download_url,omitempty"`
	// SkipReason 冻结本轮无法挂载的原因；环境类型缺失时不能借当前可变配置猜测。
	SkipReason string `json:"skip_reason,omitempty"`
}

// SessionResource 是挂载后平台返回的资源描述。
type SessionResource struct {
	ID         string
	FileID     string
	MountPath  string
	ActualPath string
}

// ResourceMounter 把已上传的 Files 文件挂载到指定 Session 的沙箱。实现必须保证：
//   - 结果未知时不重复 POST，由调用方先按 ListSessionResources 对账；
//   - 返回的 ActualPath 在该 Session 沙箱内真实可读。
type ResourceMounter interface {
	// CreateSessionResource 挂载一个 type=file 资源；mountPath 必须是平铺文件名。
	CreateSessionResource(ctx context.Context, sessionID, fileID, mountPath string) (SessionResource, error)
	// ListSessionResources 列出当前 Session 的全部挂载，用于未知结果对账。
	ListSessionResources(ctx context.Context, sessionID string) ([]SessionResource, error)
}

var (
	// ErrSessionResourceUnsupported 表示当前 Session 的环境不支持资源挂载（如 self_hosted）。
	ErrSessionResourceUnsupported = errors.New("session resource mount unsupported")
	// errSessionResourceConflict 表示对账发现同一挂载路径已被不同文件占用。
	errSessionResourceConflict = errors.New("session resource mount conflict")
)

// uploadsMountPrefix 是沙箱实际文件路径前缀，与 Console resourceMountPathPrefix 对齐。
const uploadsMountPrefix = "/mnt/session/uploads/"

// actualMountPath 返回挂载路径在沙箱内的实际访问路径（包内使用）。
func actualMountPath(mountPath string) string {
	return uploadsMountPrefix + mountPath
}

// MountedActualPath 返回挂载路径在沙箱内的实际访问路径，供适配器构造路径时使用。
func MountedActualPath(mountPath string) string {
	return actualMountPath(mountPath)
}

// MountText 返回模型附件的工具访问路径说明，图片/视频仍保留各自的模型输入块。
func (b InputBlock) MountText(path string) string {
	return "[用户发送了文件「" + noticeName(b.Title) + "」]" + mountPathNotice(path)
}

// validateFlatMountName 校验挂载文件名必须是平铺的：不含目录分隔符、穿越片段与控制字符。
// 校验通过后返回清洗后的单行名称。
func validateFlatMountName(name string) (string, error) {
	cleaned := noticeName(name)
	if cleaned == "未命名文件" || strings.ContainsAny(cleaned, "/\\") || strings.Contains(cleaned, "..") {
		return "", errors.New("invalid mount name")
	}
	return cleaned, nil
}
