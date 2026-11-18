// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
)

// freezeMounts 在 Session 已绑定后、发送输入前，把本轮已上传的附件挂载到该 Session 沙箱：
//   - cloud 关闭文件挂载或没有附件时冻结空结果；
//   - 已冻结时复用旧记录；换代导致旧 Session 失效时调用方应已重置 FrozenInput；
//   - self_hosted 冻结 skipped 记录和预签名下载地址，保留多模态块并提供下载指引；
//   - 创建结果未知先按挂载路径 + file_id List 对账，找不到才重试，不盲目重复 POST；
//   - 同一挂载路径被别的文件占用按冲突失败。
func (r *Runtime) freezeMounts(ctx context.Context, j *job, connection Connection, remote Session) error {
	if j.Input.MountsFrozen {
		return nil
	}
	if !hasMountedFiles(j.Input.Blocks) {
		return r.persistMounts(j, nil)
	}
	environmentType, err := r.attachmentEnvironment(ctx, j, remote)
	if err != nil {
		return err
	}
	if environmentType == environmentSelfHosted {
		return r.freezeDownloadURLs(ctx, j, connection)
	}
	if !j.binding.FileMounts {
		return r.persistMounts(j, nil)
	}
	if environmentType != "cloud" {
		reason := "environment_unknown"
		r.reportTurn(EventAttachment, "mount_skipped", reason, j)
		return r.persistMounts(j, skippedMounts(j, reason))
	}
	mounter, ok := connection.MAClient.(ResourceMounter)
	if !ok {
		return errors.New("ma client does not support session resources")
	}
	mounts := make([]MountedFile, 0, len(j.Input.Blocks))
	for _, block := range j.Input.Blocks {
		if block.FileID == "" {
			continue
		}
		name, err := attachmentMountName(j.ID, block)
		if err != nil {
			return r.failTurn(j, "mount_rejected")
		}
		mounted, err := r.mountOne(ctx, mounter, j.Session.SessionID, block, name)
		if err != nil {
			return err
		}
		mounted.MessageID = j.msg.MessageID
		mounted.Key = block.Key
		mounted.SessionID = j.Session.SessionID
		mounts = append(mounts, mounted)
	}
	return r.persistMounts(j, mounts)
}

// attachmentEnvironment 只采纳创建快照或 Session 自身的冻结环境类型，不回查当前可变配置。
func (r *Runtime) attachmentEnvironment(ctx context.Context, j *job, remote Session) (string, error) {
	kind, err := r.envSnapshot(ctx, j)
	if err != nil || kind != "" {
		return kind, err
	}
	return remote.EnvironmentType, nil
}

// freezeDownloadURLs 在发送前冻结本代下载地址，重试使用相同文本以维持回显校验与发送防重。
func (r *Runtime) freezeDownloadURLs(ctx context.Context, j *job, connection Connection) error {
	resolver, ok := connection.MAClient.(FileURLResolver)
	if !ok {
		return NewPreparationError(errors.New("ma client does not support file download urls"), true)
	}
	mounts := skippedMounts(j, "")
	for i := range mounts {
		if err := j.checkLease(ctx); err != nil {
			return err
		}
		link, err := resolver.ResolveFileURL(ctx, mounts[i].FileID)
		if err != nil {
			return err
		}
		if link == "" {
			return errors.New("file download url unavailable")
		}
		mounts[i].DownloadURL = link
	}
	return r.persistMounts(j, mounts)
}

// envSnapshot 读取当前 Turn 绑定的 Session 快照，返回创建请求使用的 Environment 类型。
func (r *Runtime) envSnapshot(ctx context.Context, j *job) (string, error) {
	if j.Session.SnapshotID == "" {
		return "", nil
	}
	snapshot, err := r.store.GetSessionSnapshot(ctx, j.Session.SnapshotID)
	if err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", NewStoreFailure(err)
	}
	if snapshot == nil {
		return "", NewStoreFailure(errors.New("session snapshot missing"))
	}
	if override := snapshot.Spec.EnvironmentOverride; override != nil {
		return override.Type, nil
	}
	// 没有身份/Hook env 时不发送 override，但环境类型仍在冻结阶段读取并保存。
	return snapshot.Spec.EnvironmentType, nil
}

func hasMountedFiles(blocks []InputBlock) bool {
	for _, block := range blocks {
		if block.FileID != "" {
			return true
		}
	}
	return false
}

func skippedMounts(j *job, reason string) []MountedFile {
	mounts := make([]MountedFile, 0, len(j.Input.Blocks))
	for _, block := range j.Input.Blocks {
		if block.FileID != "" {
			mounts = append(mounts, MountedFile{MessageID: j.msg.MessageID, Key: block.Key, FileID: block.FileID, SessionID: j.Session.SessionID, Skipped: true, SkipReason: reason})
		}
	}
	return mounts
}

func mountSkipNotice(mounts []MountedFile) string {
	for _, mounted := range mounts {
		if mounted.SkipReason == "environment_unknown" {
			return "文件挂载未生效：当前会话缺少创建时冻结的环境类型；请用 /new 新建会话后再试。"
		}
	}
	return ""
}

// presentedAttachmentBlocks 只调整本代提交给模型的呈现，不改写已冻结的上传事实。
// PDF 有本代实际路径时按路径处理，否则保留 document 块；兼容旧代保存的 notice/document。
func presentedAttachmentBlocks(blocks []InputBlock, mounts []MountedFile) []InputBlock {
	presented := append([]InputBlock(nil), blocks...)
	paths := make(map[string]string, len(mounts))
	for _, mounted := range mounts {
		if !mounted.Skipped {
			paths[mounted.FileID] = mounted.ActualPath
		}
	}
	for i, block := range presented {
		if blockType(block.MimeType, block.Title) == "document" {
			presented[i].Type = "document"
			if paths[block.FileID] != "" {
				presented[i].Type = InputBlockNotice
			}
		}
	}
	return presented
}

// mountOne 处理单个附件的挂载与未知结果对账。
func (r *Runtime) mountOne(ctx context.Context, mounter ResourceMounter, sessionID string, block InputBlock, mountPath string) (MountedFile, error) {
	// 上次 POST 可能成功后未能冻结本地结果；每次进入都先只读对账。
	resources, err := mounter.ListSessionResources(ctx, sessionID)
	if err != nil {
		return MountedFile{}, err
	}
	if mounted, found, err := findMountedFile(resources, block.FileID, mountPath); found || err != nil {
		return mounted, err
	}
	resource, err := mounter.CreateSessionResource(ctx, sessionID, block.FileID, mountPath)
	if err == nil {
		return MountedFile{FileID: block.FileID, RegisteredFileID: resource.FileID, ResourceID: resource.ID, MountPath: mountPath, ActualPath: resource.ActualPath}, nil
	}
	if isResourceConflict(err) {
		return MountedFile{}, err
	}
	// 结果未知（超时、5xx、断连）：先按路径与文件 ID 对账，确认同一挂载后复用。
	resources, listErr := mounter.ListSessionResources(ctx, sessionID)
	if listErr != nil {
		return MountedFile{}, listErr
	}
	if mounted, found, err := findMountedFile(resources, block.FileID, mountPath); found || err != nil {
		return mounted, err
	}
	if definitiveReject(err) {
		return MountedFile{}, NewPreparationError(err, true)
	}
	return MountedFile{}, err
}

func findMountedFile(resources []SessionResource, fileID, mountPath string) (MountedFile, bool, error) {
	var found *SessionResource
	for _, resource := range resources {
		if resource.MountPath != mountPath {
			continue
		}
		if found != nil && (found.ID != resource.ID || found.FileID != resource.FileID || found.ActualPath != resource.ActualPath) {
			return MountedFile{}, false, errSessionResourceConflict
		}
		resourceCopy := resource
		found = &resourceCopy
	}
	if found != nil {
		for _, resource := range resources {
			if resource.ID == found.ID && (resource.FileID != found.FileID || resource.MountPath != found.MountPath || resource.ActualPath != found.ActualPath) {
				return MountedFile{}, false, errSessionResourceConflict
			}
		}
		// Console 重注册后只返回新 file_id，没有原 ID/内容摘要；完整稳定路径是当前
		// 唯一找回依据。显式标记这一有限证明，不能据此宣称已验证源文件内容。
		return MountedFile{FileID: fileID, RegisteredFileID: found.FileID, ResourceID: found.ID, MountPath: mountPath, ActualPath: found.ActualPath, ReconciledByPath: true}, true, nil
	}
	return MountedFile{}, false, nil
}

func (r *Runtime) persistMounts(j *job, mounts []MountedFile) error {
	saveCtx, cancel := shortContext()
	defer cancel()
	return r.updateTurn(saveCtx, j, TurnUpdate{
		Phase:   j.Phase,
		Session: j.Session,
		Submit:  j.Submit,
		Input:   FrozenInput{MountsFrozen: true, Mounts: mounts},
	})
}

// attachmentMountName 生成稳定且平铺的挂载文件名：短稳定前缀 + 清洗后的原名，
// 避免同名覆盖、路径穿越，也让同一条消息内不同附件不会互相冲突。
func attachmentMountName(turnID string, block InputBlock) (string, error) {
	cleaned, err := validateFlatMountName(block.Title)
	if err != nil {
		return "", err
	}
	return stableKey(turnID, block.Key, block.FileID)[:12] + "-" + cleaned, nil
}

// isResourceConflict 识别平台返回的挂载冲突错误（路径已存在）。
func isResourceConflict(err error) bool {
	var code APIErrorCode
	return errors.As(err, &code) && code.HTTPStatus() == 409
}
