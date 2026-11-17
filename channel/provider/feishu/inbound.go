// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// NormalizeWebhookEvent 将宿主已验签、解密并确认租户归属的消息事件转换为通用入站消息。
// 本函数不承担验签；宿主必须在成功持久化消息后才能应答飞书。
func NormalizeWebhookEvent(raw []byte, binding core.Binding, botOpenID string) (core.Message, bool, error) {
	var event larkim.P2MessageReceiveV1
	if len(raw) > 1<<20 || json.Unmarshal(raw, &event) != nil || event.EventV2Base == nil || event.EventV2Base.Header == nil {
		return core.Message{}, false, errors.New("feishu message event invalid")
	}
	if event.EventV2Base.Header.EventType == "im.message.recalled_v1" {
		return normalizeRecallJSON(raw, binding)
	}
	if event.EventV2Base.Header.EventType != "im.message.receive_v1" {
		return core.Message{}, false, errors.New("feishu message event invalid")
	}
	return normalize(&event, binding, botOpenID)
}

func normalize(event *larkim.P2MessageReceiveV1, binding core.Binding, botOpenID string) (core.Message, bool, error) {
	var empty core.Message
	if event == nil || event.EventV2Base == nil || event.EventV2Base.Header == nil || event.Event == nil || event.Event.Sender == nil || event.Event.Message == nil {
		return empty, false, errors.New("feishu event incomplete")
	}
	header, sender, message := event.EventV2Base.Header, event.Event.Sender, event.Event.Message
	if header.AppID != binding.AppID || header.EventID == "" || header.TenantKey == "" {
		return empty, false, errors.New("feishu event identity invalid")
	}
	if sender.SenderType == nil || *sender.SenderType != "user" {
		return empty, false, nil
	}
	if message.MessageType == nil {
		return empty, false, nil
	}
	messageType := *message.MessageType
	if sender.SenderId == nil || sender.SenderId.OpenId == nil || *sender.SenderId.OpenId == "" ||
		message.MessageId == nil || *message.MessageId == "" || message.ChatId == nil || *message.ChatId == "" ||
		message.ChatType == nil || message.Content == nil {
		return empty, false, errors.New("feishu text event missing fields")
	}
	chatType := *message.ChatType
	if chatType != "p2p" && chatType != "group" {
		return empty, false, errors.New("feishu chat type unsupported")
	}
	mentioned := false
	var ownKeys []string
	for _, mention := range message.Mentions {
		if mention != nil && mention.Id != nil && mention.Id.OpenId != nil && *mention.Id.OpenId == botOpenID {
			mentioned = true
			if mention.Key != nil && *mention.Key != "" {
				ownKeys = append(ownKeys, *mention.Key)
			}
		}
	}
	if chatType == "group" && binding.GroupTrigger == "mention" && !mentioned {
		return empty, false, nil
	}
	if chatType == "group" && binding.GroupTrigger != "all" && binding.GroupTrigger != "mention" {
		return empty, false, errors.New("feishu group trigger unsupported")
	}
	unsupported := messageType != "text" && messageType != "image" && messageType != "file" && messageType != "post" && messageType != "media"
	var text string
	var attachments []core.Attachment
	if !unsupported {
		var err error
		text, attachments, err = parseContent(messageType, *message.Content)
		if err != nil {
			return empty, false, err
		}
	}
	for _, key := range ownKeys {
		text = removeMentionToken(text, key)
	}
	text = strings.TrimSpace(text)
	if !unsupported && text == "" && len(attachments) == 0 {
		return empty, false, nil
	}
	createMillis, err := strconv.ParseInt(header.CreateTime, 10, 64)
	if err != nil || createMillis <= 0 {
		return empty, false, errors.New("feishu event timestamp invalid")
	}
	return core.Message{
		Unsupported:    unsupported,
		TrackReactions: true,
		BindingID:      binding.ID, ProviderEventID: header.EventID, MessageID: *message.MessageId,
		TenantKey: header.TenantKey, ChatID: *message.ChatId, ThreadID: deref(message.ThreadId),
		ParentID: deref(message.ParentId),
		ChatType: chatType, SenderID: *sender.SenderId.OpenId, Text: text,
		Mentioned: mentioned, ReceivedAt: time.UnixMilli(createMillis), Attachments: attachments,
		Metadata: messageMetadata(message, botOpenID),
	}, true, nil
}

func messageMetadata(message *larkim.EventMessage, botOpenID string) *core.MessageMetadata {
	metadata := &core.MessageMetadata{RootMessageID: deref(message.RootId), MessageType: deref(message.MessageType)}
	if millis, err := strconv.ParseInt(deref(message.CreateTime), 10, 64); err == nil && millis > 0 {
		metadata.SentAt = time.UnixMilli(millis).UTC().Format(time.RFC3339Nano)
	}
	for _, mention := range message.Mentions {
		if mention == nil || mention.Id == nil || deref(mention.Id.OpenId) == botOpenID {
			continue
		}
		metadata.Mentions = append(metadata.Mentions, core.Mention{Key: deref(mention.Key), OpenID: deref(mention.Id.OpenId), Name: deref(mention.Name)})
	}
	return metadata
}

// parseContent 解析文本、图片、文件、视频与富文本消息。视频复用文件附件，不额外下载封面。
func parseContent(messageType, raw string) (string, []core.Attachment, error) {
	switch messageType {
	case "text":
		var content struct {
			Text *string `json:"text"`
		}
		if err := json.Unmarshal([]byte(raw), &content); err != nil || content.Text == nil {
			return "", nil, errors.New("feishu text content invalid")
		}
		return *content.Text, nil, nil
	case "image":
		var content struct {
			ImageKey string `json:"image_key"`
		}
		if err := json.Unmarshal([]byte(raw), &content); err != nil || content.ImageKey == "" {
			return "", nil, errors.New("feishu image content invalid")
		}
		return "", []core.Attachment{{Kind: "image", Key: content.ImageKey}}, nil
	case "file", "media":
		var content struct {
			FileKey  string `json:"file_key"`
			FileName string `json:"file_name"`
		}
		if err := json.Unmarshal([]byte(raw), &content); err != nil || content.FileKey == "" {
			return "", nil, errors.New("feishu file content invalid")
		}
		if messageType == "media" && content.FileName == "" {
			content.FileName = "video.mp4"
		}
		return "", []core.Attachment{{Kind: "file", Key: content.FileKey, FileName: content.FileName}}, nil
	case "post":
		return parsePost(raw)
	}
	return "", nil, errors.New("feishu message type unsupported")
}

type postNode struct {
	Tag      string `json:"tag"`
	Text     string `json:"text"`
	Href     string `json:"href"`
	ImageKey string `json:"image_key"`
	UserID   string `json:"user_id"`
	UserName string `json:"user_name"`
}

type postBody struct {
	Title   string       `json:"title"`
	Content [][]postNode `json:"content"`
	Files   []postFile   `json:"files"`
}

type postFile struct {
	FileKey  string `json:"file_key"`
	FileName string `json:"file_name"`
	IsFolder bool   `json:"is_folder"`
}

func parsePost(raw string) (string, []core.Attachment, error) {
	var body postBody
	if err := json.Unmarshal([]byte(raw), &body); err != nil || body.Content == nil && body.Title == "" && body.Files == nil {
		// 多语言post优先zh_cn、en_us，其余按locale排序，保证重放文本与附件稳定。
		var localized map[string]postBody
		if json.Unmarshal([]byte(raw), &localized) != nil || len(localized) == 0 {
			return "", nil, errors.New("feishu post content invalid")
		}
		locales := make([]string, 0, len(localized))
		for locale := range localized {
			locales = append(locales, locale)
		}
		sort.Strings(locales)
		selected := locales[0]
		for _, preferred := range []string{"zh_cn", "en_us"} {
			if _, exists := localized[preferred]; exists {
				selected = preferred
				break
			}
		}
		body = localized[selected]
	}
	var lines []string
	if body.Title != "" {
		lines = append(lines, body.Title)
	}
	var attachments []core.Attachment
	for _, paragraph := range body.Content {
		var line strings.Builder
		for _, node := range paragraph {
			switch node.Tag {
			case "text", "md":
				line.WriteString(node.Text)
			case "a":
				line.WriteString(node.Text)
				if node.Href != "" {
					line.WriteString(" (" + node.Href + ")")
				}
			case "at":
				if node.UserName != "" {
					line.WriteString("@" + node.UserName)
				} else if node.UserID != "" {
					line.WriteString("@_user")
				}
			case "img":
				if node.ImageKey != "" {
					attachments = append(attachments, core.Attachment{Kind: "image", Key: node.ImageKey})
				}
			}
		}
		lines = append(lines, line.String())
	}
	for _, file := range body.Files {
		if strings.TrimSpace(file.FileKey) == "" || file.IsFolder {
			return "", nil, errors.New("feishu post file invalid")
		}
		attachments = append(attachments, core.Attachment{Kind: "file", Key: file.FileKey, FileName: file.FileName})
	}
	return strings.Join(lines, "\n"), attachments, nil
}

func removeMentionToken(text, key string) string {
	matchStart := -1
	for offset := 0; offset < len(text); {
		index := strings.Index(text[offset:], key)
		if index < 0 {
			break
		}
		index += offset
		end := index + len(key)
		leftBound := index == 0
		if !leftBound {
			r, _ := utf8.DecodeLastRuneInString(text[:index])
			leftBound = !mentionTokenRune(r)
		}
		rightBound := end == len(text)
		if !rightBound {
			r, _ := utf8.DecodeRuneInString(text[end:])
			rightBound = !mentionTokenRune(r)
		}
		if leftBound && rightBound {
			if matchStart >= 0 {
				// 事件没有 mention 位置；多个相同 token 无法区分原文，保留完整正文。
				return text
			}
			matchStart = index
		}
		offset = end
	}
	if matchStart >= 0 {
		return text[:matchStart] + text[matchStart+len(key):]
	}
	return text
}

func mentionTokenRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_'
}

func deref(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func (p *Provider) handle(ctx context.Context, event *larkim.P2MessageReceiveV1, accept func(context.Context, core.Message) error) error {
	message, ok, err := normalize(event, p.binding, p.botOpenID)
	if err != nil || !ok {
		return err
	}
	return accept(ctx, message)
}
