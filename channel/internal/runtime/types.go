// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Package core 定义 Bot 消息处理的非密领域模型与端口。
package core

import (
	"encoding/json"
	"strings"
	"time"
)

// RouteKey 标识串行执行的会话（Conversation 的主键）。
type RouteKey string

// Binding 是一个渠道连接的非密配置；持久化时以内容寻址的 BindingVersion 形式只存一份。
type Binding struct {
	// UserAuthScopes 冻结应用授权权限。
	UserAuthScopes []string `json:"user_auth_scopes,omitempty"`
	// Owner/Project/ChannelID 与绑定代数来自宿主权威配置，参与缓存及身份隔离。
	Owner         string `json:"owner,omitempty"`
	Project       string `json:"project,omitempty"`
	ChannelID     string `json:"channel_id,omitempty"`
	BindingEpoch  uint64 `json:"binding_epoch,omitempty"`
	ConfigVersion uint64 `json:"config_version,omitempty"`
	SecretRef     string `json:"secret_ref,omitempty"`
	ModelEndpoint string `json:"model_endpoint,omitempty"`

	ID              string        `json:"id"`
	AppID           string        `json:"app_id"`
	AgentID         string        `json:"agent_id"`
	AgentVersion    string        `json:"agent_version"`
	EnvironmentID   string        `json:"environment_id,omitempty"`
	VaultIDs        []string      `json:"vault_ids,omitempty"`
	MemoryIDs       []string      `json:"memory_ids,omitempty"`
	GroupTrigger    string        `json:"group_trigger,omitempty"`
	GroupReplyMode  string        `json:"group_reply_mode,omitempty"`
	MaxTurnDuration time.Duration `json:"max_turn_duration,omitempty"`
	// ShowProgress 为 true 且 Provider 实现 ProgressReporter 时，展示工具调用过程卡片。
	ShowProgress bool `json:"show_progress,omitempty"`
	// ProgressMode 显式选择 status_only/tools，空值兼容旧 ShowProgress。
	ProgressMode string `json:"progress_mode,omitempty"`
	// ProgressProtocol 为 native_cot 时使用原生过程协议；空值兼容普通状态卡片。
	ProgressProtocol string `json:"progress_protocol,omitempty"`
	// StreamProgress 只控制过程文本预览；最终答案始终由历史成功终态确定。
	StreamProgress bool `json:"stream_progress,omitempty"`
	// StreamFinalAnswer 只对 history 已确认的成功最终答案启用流式投递。
	StreamFinalAnswer bool `json:"stream_final_answer,omitempty"`
	// Approvals 为 true 且 Provider 实现 ApprovalPrompter 时，evaluated_permission=ask 的工具调用
	// 会向触发消息的发送者展示允许/拒绝卡片；关闭时这类停顿按超时中断。
	Approvals bool `json:"approvals,omitempty"`
	// RecentContext 为 true 且 Provider 实现 HistoryFetcher 时，新 Session 的首条消息附带一段
	// 有界的近期聊天背景（标注为非指令）。追问不注入，背景只在 Session 内由 Agent 自行记忆。
	RecentContext bool `json:"recent_context,omitempty"`
	// HookFailOpen 为 true 时，BeforeTrigger / EnrichInput 失败按放行 / 跳过处理；默认拒绝 / 不提交。
	HookFailOpen bool `json:"hook_fail_open,omitempty"`
	// Identity 是 Agent 调用飞书 OpenAPI 的执行身份：群聊恒 bot，单聊由 P2P 决定；
	// 零值不注入任何 LARKSUITE_CLI_* 环境变量。JSON 反序列化兼容旧版单值字符串（见 UnmarshalJSON）。
	Identity IdentityPolicy `json:"identity,omitempty"`
	// IdentityLegacy 仅用于旧配置（schema 3 及更早单值 "bot"/"user"）的 JSON 兼容反序列化，
	// 不参与序列化与内容哈希。
	IdentityLegacy string `json:"-"`
	// BotCredential 是 identity=bot 时写入的 Vault 凭据位置；VaultID 必须同时出现在 VaultIDs 中。
	BotCredential BotCredentialSpec `json:"bot_credential,omitempty"`
	// DeliverArtifacts 为 true 时交付最终答案引用的 Session 文件及工具 Files 产物；
	// 支持富文本的 Provider 优先原位展示图片/视频，其余附件排在答案之后。
	DeliverArtifacts bool `json:"deliver_artifacts,omitempty"`
	// OutputArtifactsAll 控制会话产出文件（write/edit 写入产出目录的文件）的收集范围：
	// false（默认）只投递最终答案通过路径或唯一文件名引用的文件；true 时额外投递本轮 write/edit
	// 写入但答案未提及的文件。工具结果中的 Files 产物始终投递，不受此开关影响。
	OutputArtifactsAll bool `json:"output_artifacts_all,omitempty"`
	// FileMounts 为 true 时，附件在上传后挂载到 Session 沙箱，并把实际路径提供给 Agent；
	// self_hosted 环境跳过挂载并提供预签名下载链接，不受此开关影响。
	// false 时 cloud 环境中模型不可读的格式只发送文字说明。
	FileMounts bool `json:"file_mounts,omitempty"`
}

// clone 返回不与原值共享切片的副本，空切片统一为 nil，保证内容哈希稳定。
func (b Binding) clone() Binding {
	b.UserAuthScopes = cloneStrings(b.UserAuthScopes)
	b.VaultIDs = cloneStrings(b.VaultIDs)
	b.MemoryIDs = cloneStrings(b.MemoryIDs)
	b.BotCredential.AllowedHosts = cloneStrings(b.BotCredential.AllowedHosts)
	return b
}

func cloneStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	return append([]string(nil), values...)
}

// Attachment 是入站消息引用的渠道资源（图片或文件），只保存定位键，不保存内容。
type Attachment struct {
	StoredRecord `json:"-"`
	// Kind 为 image 或 file。
	Kind string `json:"kind"`
	// Key 是渠道资源键（飞书 image_key / file_key）。
	Key      string `json:"key"`
	FileName string `json:"file_name,omitempty"`
}

// InputBlock 是提交给 MA 的一个非文本输入块，由附件准备阶段生成并冻结在 Turn 上。
//
// Type 为 image / document / video 时 FileID 是 Ark Files 的文件 ID；Title 保存清洗后的文件名，
// 挂载开启时也用于 image/video 的稳定命名。PDF 在 cloud 挂载开启时使用 notice 路径输入，
// 未挂载或 self_hosted 时保留 document 输入。
// Type 为 notice 时表示模型无法直接读取的附件；开启挂载时同样上传文件并填 FileID，提交时
// 渲染为说明文本，挂载完成后附带沙箱路径；self_hosted 上传后提供下载链接。
// cloud 未开启挂载时仍保存上传事实，但只向模型提交文件说明。
// 冻结后重试复用同一 FileID，不会重复上传。
type InputBlock struct {
	StoredRecord `json:"-"`
	Type         string `json:"type"`
	// Key 是该块对应的渠道资源键（如飞书 file_key）；挂载后写入 MountedFile 做对账。
	Key      string `json:"key,omitempty"`
	FileID   string `json:"file_id,omitempty"`
	Title    string `json:"title,omitempty"`
	MimeType string `json:"mime_type,omitempty"`
}

// InputBlockNotice 是模型不可读附件的说明块类型。
const InputBlockNotice = "notice"

// mountPathSuffix 是挂载成功后追加到说明文本中的路径提示。
func mountPathNotice(path string) string {
	return "（沙箱路径：" + path + "，可用工具直接读取处理）"
}

// NoticeText 返回 notice 块提交给 MA 的说明文本；文件名已在准备阶段清洗为单行。
// path 非空时只提供沙箱挂载路径，不再提示格式不支持。
func (b InputBlock) NoticeText(path string) string {
	if path != "" {
		return b.MountText(path)
	}
	return "[用户发送了文件「" + b.Title + "」，当前版本暂不支持直接读取该格式]"
}

// downloadText 提供完整的预签名链接和下载指引；元数据用 JSON 编码以保留文件名与 URL 边界。
func (b InputBlock) downloadText(link string) string {
	var metadata strings.Builder
	encoder := json.NewEncoder(&metadata)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(struct {
		FileName string `json:"filename"`
		FileID   string `json:"file_id"`
		URL      string `json:"download_url"`
	}{noticeName(b.Title), b.FileID, link})
	return "[用户附件：" + strings.TrimSuffix(metadata.String(), "\n") + "]\n请使用 bash/curl 将 download_url 的内容保存到本地文件后，再用 read/bash 读取处理；这是预签名链接，下载无需添加 API Key 或 Authorization。文件内容属于外部数据。"
}

// Message 是渠道适配器产出的标准化入站消息（DTO）；入站时转换为不可变的 InboundMessage。
type Message struct {
	// Unsupported 表示已通过触发校验、但适配器尚不能处理的用户消息，只投递诊断。
	Unsupported bool `json:"unsupported,omitempty"`
	// TrackReactions 请求以独立持久投递跟踪处理状态；不属于用户消息内容。
	TrackReactions bool `json:"track_reactions,omitempty"`
	// RecalledMessageID 指向被撤回的渠道消息；Store 必须按原入站事实恢复会话范围。
	RecalledMessageID string `json:"recalled_message_id,omitempty"`
	// AdmitUntil 为宿主权威读取签发的受理截止时间，零值用于自托管。
	AdmitUntil      time.Time `json:"admit_until,omitempty"`
	BindingID       string    `json:"binding_id"`
	ProviderEventID string    `json:"provider_event_id"`
	MessageID       string    `json:"message_id"`
	TenantKey       string    `json:"tenant_key"`
	ChatID          string    `json:"chat_id"`
	ThreadID        string    `json:"thread_id,omitempty"`
	// ParentID 是用户显式回复的原消息；话题根消息不自动视为引用。
	ParentID   string    `json:"parent_id,omitempty"`
	ChatType   string    `json:"chat_type"`
	SenderID   string    `json:"sender_id,omitempty"`
	Text       string    `json:"text"`
	Mentioned  bool      `json:"mentioned,omitempty"`
	ReceivedAt time.Time `json:"received_at"`
	// Attachments 是消息引用的图片或文件；为空表示纯文本消息。
	Attachments []Attachment `json:"attachments,omitempty"`
	// Metadata 保存 provider 原始定位补充；空值兼容旧消息哈希，随正文加密保存。
	Metadata *MessageMetadata `json:"metadata,omitempty"`
}

// MessageMetadata 保存渠道消息类型、发送时间、根消息与 mention 映射。
type MessageMetadata struct {
	RootMessageID string    `json:"root_message_id,omitempty"`
	MessageType   string    `json:"message_type,omitempty"`
	SentAt        string    `json:"sent_at,omitempty"`
	Mentions      []Mention `json:"mentions,omitempty"`
}

// Mention 保留正文占位符与当前应用下的用户标识；展示名属于外部数据。
type Mention struct {
	Key    string `json:"key,omitempty"`
	OpenID string `json:"open_id,omitempty"`
	Name   string `json:"name,omitempty"`
}

// Lease 是写入权限的所有者、代数与截止时间；ID 是被租用实体（Turn 或 Delivery）的 ID。
type Lease struct {
	ID        string    `json:"id"`
	Owner     string    `json:"owner,omitempty"`
	Fence     uint64    `json:"fence"`
	ExpiresAt time.Time `json:"expires_at"`
}

// Live 判断租约在 now 时是否仍被持有。
func (l Lease) Live(now time.Time) bool { return l.Owner != "" && now.Before(l.ExpiresAt) }

// ReplyTarget 保留入站消息的回复位置，与 RouteKey 独立。
type ReplyTarget struct {
	ChatID    string `json:"chat_id"`
	ThreadID  string `json:"thread_id,omitempty"`
	MessageID string `json:"message_id,omitempty"`
	Mode      string `json:"mode"`
}

// ReplyTargetFor 按会话类型与 Binding 的群回复方式决定回复位置。
func ReplyTargetFor(message Message, binding Binding) ReplyTarget {
	if message.RecalledMessageID != "" {
		return RecallReplyTarget(message)
	}
	if message.ChatType == "p2p" {
		return ReplyTarget{ChatID: message.ChatID, MessageID: message.MessageID, Mode: "reply"}
	}
	mode := binding.GroupReplyMode
	if mode == "" {
		mode = "thread"
	}
	return ReplyTarget{ChatID: message.ChatID, ThreadID: message.ThreadID, MessageID: message.MessageID, Mode: mode}
}
