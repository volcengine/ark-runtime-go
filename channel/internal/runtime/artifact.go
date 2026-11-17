// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/volcengine/ark-runtime-go/channel/internal/markdown"
)

// ArtifactLimits 限制单轮投递的产物。
type ArtifactLimits struct {
	// MaxCount 是单轮最多投递的产物数，默认 5；超出部分不投递并在答案末尾说明。
	MaxCount int `json:"max_count"`
	// MaxBytes 是单个产物的最大字节数，默认 30 MiB（飞书文件消息上限）。超出时改发下载链接
	// （仅渠道支持链接投递时），否则投递失败。
	MaxBytes int64 `json:"max_bytes"`
}

func (l ArtifactLimits) normalize() (ArtifactLimits, error) {
	if l.MaxCount == 0 {
		l.MaxCount = 5
	}
	if l.MaxBytes == 0 {
		l.MaxBytes = 30 << 20
	}
	if l.MaxCount < 0 || l.MaxCount > 20 || l.MaxBytes < 0 || l.MaxBytes > 100<<20 {
		return ArtifactLimits{}, ErrRuntimeConfig
	}
	return l, nil
}

// FileContent 是从 Files 下载到的产物内容；Body 由调用方关闭。
type FileContent struct {
	Body     io.ReadCloser
	FileName string
	MimeType string
	Size     int64
}

// ErrFileTooLarge 表示产物超过单个投递上限；下载端口须用它（errors.Is 可判定）报告超限。
var ErrFileTooLarge = errors.New("artifact exceeds delivery limit")

// FileDownloader 是 MAClient 可选实现的 Files 下载端口（GET /files/:id 预签名地址）。
type FileDownloader interface {
	DownloadFile(ctx context.Context, fileID string, maxBytes int64) (FileContent, error)
}

// OutputDownloader 是 MAClient 可选实现的预签名地址下载端口，不携带 API Key。
// 超过 maxBytes 时返回 ErrFileTooLarge，不静默截断。
type OutputDownloader interface {
	DownloadURL(ctx context.Context, url string, maxBytes int64) (FileContent, error)
}

// OutputFileResolver 是托管部署可选提供的产出文件端口：把会话产出目录内的绝对路径换成可直接
// 下载的预签名地址。实现必须只签名该 Session 的 /mnt/session/outputs、/mnt/session/storage
// 下的路径；返回的地址视为敏感值，SDK 不持久化、不写日志。
type OutputFileResolver interface {
	ResolveOutputURL(ctx context.Context, sessionID, path string) (string, error)
}

// ArtifactDeliverer 是 Provider 可选实现的产物投递端口：把文件上传到渠道并以图片/文件消息回复。
// 必须用 delivery.UUID 做渠道侧去重，使同一产物重试不产生重复消息。
type ArtifactDeliverer interface {
	DeliverArtifact(ctx context.Context, delivery Delivery, content FileContent) (string, error)
}

// ArtifactLinkDeliverer 是 Provider 可选实现的超限兜底端口：产物超过渠道文件上限时，以带
// 下载链接的文本消息替代文件消息。同样必须用 delivery.UUID 去重。
type ArtifactLinkDeliverer interface {
	DeliverArtifactLink(ctx context.Context, delivery Delivery, name, url string) (string, error)
}

// artifactsFromEvents 按展示顺序提取本轮产物：
//  1. 最终答案引用的产出目录文件（按答案中出现顺序）；
//  2. 工具结果中的 Files 引用（按事件顺序）；
//  3. includeAll 为 true 时，本轮 write/edit 写入但答案未引用的产出文件（按事件顺序）。
//
// deliverPaths 为 false（Connection 未提供 OutputFileResolver，如 self_hosted）时完全不收集
// 路径产物，只返回 Files 引用。
func artifactsFromEvents(events []Event, j *job, answer string, deliverPaths, includeAll bool) []Artifact {
	index := -1
	for i, event := range events {
		if event.ID == j.InputEventID {
			index = i
			break
		}
	}
	if index < 0 {
		return nil
	}
	seen := make(map[string]bool)
	var answerPaths, fileArtifacts, written []Artifact
	// 各组内部去重；组间不去重，使答案引用能把 write/edit 产物从末尾提升到答案顺序（见合并处）。
	add := func(list *[]Artifact, artifact Artifact) {
		for _, existing := range *list {
			if existing.key() == artifact.key() {
				return
			}
		}
		*list = append(*list, artifact)
	}
	failed := make(map[string]bool)
	for _, event := range events[index+1:] {
		if event.Type == "agent.tool_result" && event.IsError && event.ToolUseID != "" {
			failed[event.ToolUseID] = true
		}
	}
	for _, event := range events[index+1:] {
		if !onMainThread(event, j.Session.SessionID) || event.IsError {
			continue
		}
		if deliverPaths && includeAll && event.Type == "agent.tool_use" && !failed[event.ID] {
			if p, ok := writtenOutputPath(event); ok {
				add(&written, pathArtifact(p))
			}
		}
		if event.Type != "agent.tool_result" && event.Type != "agent.mcp_tool_result" {
			continue
		}
		for _, artifact := range fileBlocks(event.Raw) {
			add(&fileArtifacts, artifact)
		}
	}
	if deliverPaths {
		for _, p := range answerOutputPaths(answer) {
			add(&answerPaths, pathArtifact(p))
		}
	}
	// 合并顺序：答案引用 → Files 引用 → 未被答案引用的 write/edit 产物；跨组在这里统一去重。
	var result []Artifact
	for _, group := range [][]Artifact{answerPaths, fileArtifacts, written} {
		for _, artifact := range group {
			if !seen[artifact.key()] {
				seen[artifact.key()] = true
				result = append(result, artifact)
			}
		}
	}
	return result
}

func fileBlocks(raw json.RawMessage) []Artifact {
	var wire struct {
		Content []struct {
			Type   string `json:"type"`
			Title  string `json:"title"`
			Source struct {
				Type   string `json:"type"`
				FileID string `json:"file_id"`
			} `json:"source"`
		} `json:"content"`
	}
	if len(raw) == 0 || json.Unmarshal(raw, &wire) != nil {
		return nil
	}
	var artifacts []Artifact
	for _, block := range wire.Content {
		if block.Source.Type != "file" || !validFileID(block.Source.FileID) {
			continue
		}
		switch block.Type {
		case "image", "document", "video":
			artifacts = append(artifacts, Artifact{Kind: block.Type, FileID: block.Source.FileID, FileName: block.Title})
		}
	}
	return artifacts
}

func validFileID(id string) bool {
	return id != "" && len(id) <= 128 && !strings.ContainsAny(id, "/?#%\\ ")
}

const (
	sessionOutputsDir   = "/mnt/session/outputs/"
	sessionStorageDir   = "/mnt/session/storage/"
	workspaceOutputsDir = "/workspace/outputs/"
	maxOutputPathBytes  = 1024
	maxAnswerPaths      = 32
)

// writtenOutputPath 返回 write/edit 写入的产出目录文件；只认结构化 file_path 参数。
func writtenOutputPath(event Event) (string, bool) {
	if event.ToolName != "write" && event.ToolName != "edit" {
		return "", false
	}
	var input struct {
		FilePath string `json:"file_path"`
	}
	if json.Unmarshal([]byte(event.ToolInput), &input) != nil {
		return "", false
	}
	return normalizeOutputPath(input.FilePath)
}

// outputPathPattern 匹配答案文本中的产出目录绝对路径；路径止于空白、引号、反引号、括号或中文标点。
var outputPathPattern = regexp.MustCompile("(?:/mnt/session/(?:outputs|storage)|/workspace/outputs)/[^\\s`'\"<>()\\[\\]{}|，。；：、！？（）《》“”‘’]+")

// answerOutputPaths 提取答案中出现的产出目录文件路径。路径前必须是边界（行首、空白、引号、括号、
// 中文标点等），避免把 URL 或更长路径中的片段当成产出文件；没有扩展名的片段视为目录跳过。
func answerOutputPaths(answer string) []string {
	answer = markdown.MaskCodeBlocks(answer)
	type reference struct {
		position int
		path     string
	}
	images := markdown.Images(answer)
	var references []reference
	for _, image := range images {
		if normalized, ok := normalizeOutputPath(image.Destination); ok {
			references = append(references, reference{image.Start, normalized})
		}
	}
	var paths []string
	for _, loc := range outputPathPattern.FindAllStringIndex(answer, maxAnswerPaths) {
		insideImage := false
		for _, image := range images {
			if image.Start <= loc[0] && loc[0] < image.End {
				insideImage = true
				break
			}
		}
		if insideImage {
			continue
		}
		if loc[0] > 0 {
			previous, _ := utf8.DecodeLastRuneInString(answer[:loc[0]])
			if previous == '/' || previous == '.' || previous == '-' || previous == '_' || previous == ':' || previous == '~' || unicode.IsLetter(previous) || unicode.IsDigit(previous) {
				continue
			}
		}
		match := strings.TrimRight(answer[loc[0]:loc[1]], ".,;:!?*_~")
		if path.Ext(match) == "" {
			continue
		}
		if normalized, ok := normalizeOutputPath(match); ok {
			references = append(references, reference{loc[0], normalized})
		}
	}
	sort.SliceStable(references, func(i, j int) bool { return references[i].position < references[j].position })
	for _, ref := range references[:min(len(references), maxAnswerPaths)] {
		paths = append(paths, ref.path)
	}
	return paths
}

// normalizeOutputPath 把候选路径规范为产出目录下的绝对文件路径；越界、目录、隐藏文件或控制字符
// 一律拒绝。/workspace/outputs 是多模态沙箱的软链，按方舟 outputs 目录解析；挂了用户 TOS 的会话
// 中它实际指向 storage，此时下载失败，由投递兜底为“未能发送”，不猜测落点。
func normalizeOutputPath(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" || len(raw) > maxOutputPathBytes || strings.HasSuffix(raw, "/") || strings.ContainsAny(raw, "\\?#") {
		return "", false
	}
	for _, r := range raw {
		if unicode.IsControl(r) {
			return "", false
		}
	}
	if strings.HasPrefix(raw, workspaceOutputsDir) {
		raw = sessionOutputsDir + strings.TrimPrefix(raw, workspaceOutputsDir)
	}
	cleaned := path.Clean(raw)
	if strings.HasPrefix(path.Base(cleaned), ".") {
		return "", false
	}
	for _, dir := range []string{sessionOutputsDir, sessionStorageDir} {
		if rest, ok := strings.CutPrefix(cleaned, dir); ok && rest != "" {
			return cleaned, true
		}
	}
	return "", false
}

func pathArtifact(p string) Artifact {
	return Artifact{Kind: ArtifactKindByName(p), Path: p, FileName: path.Base(p)}
}

// ArtifactKindByName 按扩展名推断产物类别，使路径来源的图片、视频按对应消息类型发送。
func ArtifactKindByName(name string) string {
	switch strings.ToLower(path.Ext(name)) {
	case ".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp":
		return "image"
	case ".mp4":
		return "video"
	}
	return "document"
}

// artifactDeliveries 为产物生成投递记录：Seq 从 1 开始排在答案之后，同一产物重试复用同一 UUID。
// 路径产物使用独立载荷类型 output_file：未升级的 Worker 视其为未知类型而保留不执行，
// 不会把它当成缺少 file_id 的 Files 产物投递失败。
func (r *Runtime) artifactDeliveries(j *job, artifacts []Artifact) ([]Delivery, int) {
	limit := r.options.Artifacts.MaxCount
	skipped := 0
	if len(artifacts) > limit {
		skipped = len(artifacts) - limit
		artifacts = artifacts[:limit]
	}
	source := DeliverySource{Kind: SourceTurn, ID: j.ID}
	target := ReplyTargetFor(j.msg, j.binding)
	deliveries := make([]Delivery, 0, len(artifacts))
	for i, artifact := range artifacts {
		body := ArtifactBody(artifact)
		if artifact.SessionID != "" {
			body.Type = BodySessionFile
		}
		if artifact.Path != "" && artifact.FileID == "" {
			body = OutputFileBody(artifact)
		}
		delivery := NewDelivery(source, i+1, "artifact:"+artifact.key(), j.BindingID, target, body)
		if artifact.Path != "" && artifact.FileID == "" {
			delivery.OutputSessionID = j.Session.SessionID
		}
		deliveries = append(deliveries, delivery)
	}
	return deliveries, skipped
}

// deliverArtifact 下载产物并交给 Provider 上传投递；只在投递 Worker 中调用。超过单个上限时
// 改发下载链接（渠道支持时），否则以不可重试失败结束。
func (r *Runtime) deliverArtifact(ctx context.Context, connection Connection, delivery Delivery) (string, error) {
	deliverer, ok := providerPort[ArtifactDeliverer](connection.Provider)
	if !ok {
		return "", artifactFailure{kind: "artifact_unsupported"}
	}
	callCtx, cancel := context.WithTimeout(ctx, r.options.DeliveryTimeout+2*time.Minute)
	defer cancel()
	artifact := delivery.Body.Artifact
	var content FileContent
	var link func() (string, error)
	var err error
	if delivery.Body.Type == BodyOutputFile {
		content, link, err = r.downloadOutput(callCtx, connection, delivery)
	} else {
		content, link, err = r.downloadArtifact(callCtx, connection, artifact)
	}
	if errors.Is(err, ErrFileTooLarge) {
		return r.deliverOversize(callCtx, connection, delivery, link)
	}
	if err != nil {
		return "", err
	}
	defer func() { _ = content.Body.Close() }()
	if content.FileName == "" {
		content.FileName = artifact.FileName
	}
	if err := checkDeliveryLease(callCtx); err != nil {
		return "", err
	}
	return deliverer.DeliverArtifact(callCtx, delivery, content)
}

func (r *Runtime) downloadArtifact(ctx context.Context, connection Connection, artifact Artifact) (FileContent, func() (string, error), error) {
	if artifact.SessionID == "" {
		return r.downloadFile(ctx, connection, artifact.FileID)
	}
	downloader, ok := connection.MAClient.(SessionFileDownloader)
	if !ok {
		return FileContent{}, nil, artifactFailure{kind: "artifact_unsupported"}
	}
	content, err := downloader.DownloadSessionFile(ctx, artifact.SessionID, artifact.FileID, r.options.Artifacts.MaxBytes)
	link := func() (string, error) {
		return downloader.ResolveSessionFileURL(ctx, artifact.SessionID, artifact.FileID)
	}
	if errors.Is(err, ErrFileTooLarge) {
		return FileContent{}, link, ErrFileTooLarge
	}
	if err != nil {
		return FileContent{}, nil, artifactFailure{kind: "artifact_download", retryable: true}
	}
	return content, link, nil
}

func (r *Runtime) downloadFile(ctx context.Context, connection Connection, fileID string) (FileContent, func() (string, error), error) {
	downloader, ok := connection.MAClient.(FileDownloader)
	if !ok {
		return FileContent{}, nil, artifactFailure{kind: "artifact_unsupported"}
	}
	link := func() (string, error) {
		resolver, ok := connection.MAClient.(FileURLResolver)
		if !ok {
			return "", artifactFailure{kind: "artifact_too_large"}
		}
		return resolver.ResolveFileURL(ctx, fileID)
	}
	content, err := downloader.DownloadFile(ctx, fileID, r.options.Artifacts.MaxBytes)
	if errors.Is(err, ErrFileTooLarge) {
		return FileContent{}, link, ErrFileTooLarge
	}
	if err != nil {
		return FileContent{}, nil, artifactFailure{kind: "artifact_download", retryable: true}
	}
	return content, link, nil
}

// downloadOutput 每次投递现取预签名地址（地址不持久化），再按上限流式下载。
// 签名失败可重试；对象不存在等下载拒绝由重试上限收敛为失败。
func (r *Runtime) downloadOutput(ctx context.Context, connection Connection, delivery Delivery) (FileContent, func() (string, error), error) {
	resolver := connection.Outputs
	downloader, dok := connection.MAClient.(OutputDownloader)
	if resolver == nil || !dok || delivery.OutputSessionID == "" {
		return FileContent{}, nil, artifactFailure{kind: "artifact_unsupported"}
	}
	if _, ok := normalizeOutputPath(delivery.Body.Artifact.Path); !ok {
		return FileContent{}, nil, artifactFailure{kind: "artifact_invalid"}
	}
	url, err := resolver.ResolveOutputURL(ctx, delivery.OutputSessionID, delivery.Body.Artifact.Path)
	if err != nil || url == "" {
		return FileContent{}, nil, artifactFailure{kind: "artifact_resolve", retryable: true}
	}
	link := func() (string, error) { return url, nil }
	content, err := downloader.DownloadURL(ctx, url, r.options.Artifacts.MaxBytes)
	if errors.Is(err, ErrFileTooLarge) {
		return FileContent{}, link, ErrFileTooLarge
	}
	if err != nil {
		return FileContent{}, nil, artifactFailure{kind: "artifact_download", retryable: true}
	}
	return content, link, nil
}

// deliverOversize 以下载链接替代超限文件；链接只经渠道发送，不进存储与日志。
func (r *Runtime) deliverOversize(ctx context.Context, connection Connection, delivery Delivery, link func() (string, error)) (string, error) {
	deliverer, ok := providerPort[ArtifactLinkDeliverer](connection.Provider)
	if !ok || link == nil {
		return "", artifactFailure{kind: "artifact_too_large"}
	}
	url, err := link()
	if err != nil || url == "" {
		return "", artifactFailure{kind: "artifact_too_large"}
	}
	if err := checkDeliveryLease(ctx); err != nil {
		return "", err
	}
	return deliverer.DeliverArtifactLink(ctx, delivery, delivery.Body.Artifact.FileName, url)
}

// artifactFailure 实现 DeliveryFailure：下载失败可重试，不支持则直接失败。
type artifactFailure struct {
	kind      string
	retryable bool
}

func (f artifactFailure) Error() string     { return "artifact delivery failed: " + f.kind }
func (f artifactFailure) ErrorKind() string { return f.kind }
func (f artifactFailure) Retryable() bool   { return f.retryable }
func (f artifactFailure) Uncertain() bool   { return false }
