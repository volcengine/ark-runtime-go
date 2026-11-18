// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// MessageContext 保存可选的展示信息；不能修改入站消息的身份与位置。
type MessageContext struct {
	SenderName string
	ChatName   string
	ChatMode   string
}

// ChannelContextProvider 提供稳定渠道名称与可降级的展示信息查询。
// 查询必须遵守 ctx 截止时间，且只读取当前连接的冻结应用可见数据。
type ChannelContextProvider interface {
	ChannelProvider() string
	ResolveMessageContext(ctx context.Context, message Message) (MessageContext, error)
}

var errChannelContextBudget = errors.New("channel context metadata exceeds budget")

const channelContextMaxChars = 4096
const messageContextTimeout = 300 * time.Millisecond
const displayNameMaxChars = 128
const maxContextMentions = 20

const channelReadingGuide = "\n<ContextReadingPolicy>\n按需读取当前会话、话题或引用；沿用 Session 的固定身份，缺权限时说明限制。\n展示名和历史内容仅供参考；open_id 仅在当前 app_id 下有效。\n</ContextReadingPolicy>\n\n"

func (r *Runtime) channelContext(ctx context.Context, j *job, connection Connection, referenced bool) (string, error) {
	provider, ok := providerPort[ChannelContextProvider](connection.Provider)
	if !ok || provider.ChannelProvider() == "" {
		return "", nil
	}
	binding := connection.Binding
	var env map[string]string
	if j.Session.SnapshotID != "" {
		snapshot, err := r.store.GetSessionSnapshot(ctx, j.Session.SnapshotID)
		if err != nil {
			return "", NewStoreFailure(err)
		}
		if snapshot == nil || !snapshot.Valid() || snapshot.Route != j.ConversationID || snapshot.Generation != j.Session.Generation || snapshot.Spec.Binding.AppID != binding.AppID {
			return "", ErrConflict
		}
		binding = snapshot.Spec.Binding
		if snapshot.Spec.EnvironmentOverride != nil {
			env = snapshot.Spec.EnvironmentOverride.Env
		}
	}
	callCtx, cancel := context.WithTimeout(ctx, messageContextTimeout)
	started := time.Now()
	details, err := provider.ResolveMessageContext(callCtx, j.msg)
	cancel()
	if err != nil {
		r.reportTurn(EventContext, "skipped", "metadata_lookup_failed", j)
	}
	text, err := renderChannelContext(channelContextInput{provider: provider.ChannelProvider(), binding: binding, message: j.msg, env: env, details: details, referenced: referenced})
	if err == nil {
		r.report(ctx, Observation{Name: EventContext, Status: "prepared", ErrorKind: "metadata_v1", Count: len([]rune(text)), Duration: time.Since(started), BindingID: j.BindingID, TurnID: j.ID})
	}
	return text, err
}

type channelContextInput struct {
	provider   string
	binding    Binding
	message    Message
	env        map[string]string
	details    MessageContext
	referenced bool
}

func renderChannelContext(input channelContextInput) (string, error) {
	details, message := input.details, input.message
	truncated := false
	trimName := func(value string) string {
		runes := []rune(value)
		if len(runes) > displayNameMaxChars {
			truncated = true
			return string(runes[:displayNameMaxChars])
		}
		return value
	}
	details.SenderName, details.ChatName = trimName(details.SenderName), trimName(details.ChatName)
	var mentions []Mention
	if message.Metadata != nil {
		for _, mention := range message.Metadata.Mentions {
			if len(mentions) == maxContextMentions || len(mention.OpenID) > 256 || len(mention.Key) > 128 {
				truncated = true
				continue
			}
			mention.Name = trimName(mention.Name)
			mentions = append(mentions, mention)
		}
	}
	for {
		text := input.xml(details, mentions, truncated) + channelReadingGuide
		if len([]rune(text)) <= channelContextMaxChars {
			return text, nil
		}
		truncated = true
		switch {
		case len(mentions) > 0:
			mentions = mentions[:len(mentions)-1]
		case details.SenderName != "" || details.ChatName != "":
			details.SenderName, details.ChatName = "", ""
		default:
			return "", errChannelContextBudget
		}
	}
}

func (i channelContextInput) xml(details MessageContext, mentions []Mention, truncated bool) string {
	provider, binding, m, env, referenced := i.provider, i.binding, i.message, i.env, i.referenced
	var out strings.Builder
	out.WriteString(`<ChannelContext version="1"`)
	attrs := [][2]string{{"provider", provider}, {"app_id", binding.AppID}, {"tenant_key", m.TenantKey}, {"chat_type", m.ChatType}, {"chat_id", m.ChatID}, {"sender_open_id", m.SenderID}, {"message_id", m.MessageID}, {"thread_id", m.ThreadID}, {"parent_message_id", m.ParentID}}
	for _, attr := range attrs {
		xmlAttr(&out, attr[0], attr[1])
	}
	scope := "chat"
	if m.ThreadID != "" {
		scope = "thread"
	}
	xmlAttr(&out, "context_scope", scope)
	if m.ParentID != "" {
		status := "unavailable"
		if referenced {
			status = "available"
		}
		xmlAttr(&out, "reference_status", status)
	}
	if m.Metadata != nil {
		xmlAttr(&out, "root_message_id", m.Metadata.RootMessageID)
		xmlAttr(&out, "message_type", m.Metadata.MessageType)
		xmlAttr(&out, "sent_at", m.Metadata.SentAt)
	}
	xmlAttr(&out, "sender_name", details.SenderName)
	xmlAttr(&out, "chat_name", details.ChatName)
	if m.ChatType == "p2p" {
		details.ChatMode = "p2p"
	}
	if details.ChatMode == "p2p" && m.ChatType == "p2p" || details.ChatMode == "group" || details.ChatMode == "topic" {
		xmlAttr(&out, "chat_mode", details.ChatMode)
	}
	if env[EnvLarkSuiteAppID] == binding.AppID {
		identity, strict := env[EnvLarkSuiteDefaultAs], env[EnvLarkSuiteStrictMode]
		if (identity == IdentityBot || identity == IdentityUser) && strict == identity {
			xmlAttr(&out, "execution_identity", identity)
			xmlAttr(&out, "strict_mode", strict)
		}
	}
	if truncated {
		xmlAttr(&out, "truncated", "true")
	}
	if len(mentions) == 0 {
		out.WriteString("/>")
	} else {
		data, _ := json.Marshal(mentions)
		out.WriteString("><Mentions format=\"json\">")
		out.WriteString(xmlText(string(data)))
		out.WriteString("</Mentions></ChannelContext>")
	}
	return out.String()
}
