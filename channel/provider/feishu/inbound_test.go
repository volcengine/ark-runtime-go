// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func str(v string) *string { return &v }

func inbound(senderType, chatType, msgType, content string, mentions ...*larkim.MentionEvent) *larkim.P2MessageReceiveV1 {
	return &larkim.P2MessageReceiveV1{
		EventV2Base: &larkevent.EventV2Base{Header: &larkevent.EventHeader{EventID: "ev-1", AppID: "app-1", TenantKey: "tenant-1", CreateTime: "1700000000000"}},
		Event: &larkim.P2MessageReceiveV1Data{
			Sender:  &larkim.EventSender{SenderType: str(senderType), SenderId: &larkim.UserId{OpenId: str("ou-sender")}},
			Message: &larkim.EventMessage{MessageId: str("om-1"), ChatId: str("oc-1"), ThreadId: str("ot-1"), ChatType: str(chatType), MessageType: str(msgType), Content: str(content), Mentions: mentions},
		},
	}
}

func TestNormalizeInbound(t *testing.T) {
	self := &larkim.MentionEvent{Key: str("@_user_1"), Id: &larkim.UserId{OpenId: str("ou-bot")}, MentionedType: str("bot")}
	other := &larkim.MentionEvent{Key: str("@_user_2"), Id: &larkim.UserId{OpenId: str("ou-other")}, MentionedType: str("user")}
	for _, tc := range []struct {
		name         string
		groupTrigger string
		event        *larkim.P2MessageReceiveV1
		want         bool
		text         string
	}{
		{"p2p", "mention", inbound("user", "p2p", "text", `{"text":"hello"}`), true, "hello"},
		{"bot", "all", inbound("bot", "group", "text", `{"text":"hello"}`), false, ""},
		{"sticker", "all", inbound("user", "group", "sticker", `{"file_key":"x"}`), true, ""},
		{"other mention", "mention", inbound("user", "group", "text", `{"text":"@_user_2 hi"}`, other), false, ""},
		{"self mention", "mention", inbound("user", "group", "text", `{"text":"@_user_1 @_user_2 hi"}`, self, other), true, "@_user_2 hi"},
		{"prefix collision", "mention", inbound("user", "group", "text", `{"text":"@_user_1 @_user_10 hi"}`, self, &larkim.MentionEvent{Key: str("@_user_10"), Id: &larkim.UserId{OpenId: str("ou-other")}}), true, "@_user_10 hi"},
		{"literal substring", "mention", inbound("user", "group", "text", `{"text":"@_user_1 prefix@_user_1suffix hi"}`, self), true, "prefix@_user_1suffix hi"},
		{"underscore suffix", "mention", inbound("user", "group", "text", `{"text":"@_user_1 @_user_1_extra hi"}`, self), true, "@_user_1_extra hi"},
		{"ambiguous literal token", "mention", inbound("user", "group", "text", `{"text":"@_user_1 said @_user_1 hi"}`, self), true, "@_user_1 said @_user_1 hi"},
		{"all", "all", inbound("user", "group", "text", `{"text":"hello"}`), true, "hello"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok, err := normalize(tc.event, core.Binding{ID: "binding-1", AppID: "app-1", GroupTrigger: tc.groupTrigger}, "ou-bot")
			require.NoError(t, err)
			require.Equal(t, tc.want, ok)
			if ok {
				require.Equal(t, tc.text, got.Text)
				require.Equal(t, "tenant-1", got.TenantKey)
				require.Equal(t, "ot-1", got.ThreadID)
				require.Equal(t, "ev-1", got.ProviderEventID)
				require.Equal(t, time.UnixMilli(1700000000000), got.ReceivedAt)
			}
		})
	}
}

func TestNormalizeVideoUsesFileAttachment(t *testing.T) {
	for _, tc := range []struct {
		name, content, fileName string
		invalid                 bool
	}{
		{"named", `{"file_key":"video_1","file_name":"演示.mp4","image_key":"cover_1","duration":26000}`, "演示.mp4", false},
		{"unnamed", `{"file_key":"video_1","image_key":"cover_1"}`, "video.mp4", false},
		{"missing key", `{"image_key":"cover_1"}`, "", true},
		{"malformed", `{`, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, accepted, err := normalize(inbound("user", "p2p", "media", tc.content), core.Binding{ID: "binding-1", AppID: "app-1"}, "ou-bot")
			if tc.invalid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.True(t, accepted)
			require.Empty(t, got.Text)
			require.Equal(t, []core.Attachment{{Kind: "file", Key: "video_1", FileName: tc.fileName}}, got.Attachments)
			require.Equal(t, core.MessageUser, core.ClassifyMessage(got))
		})
	}
}

func TestNormalizeUnsupportedRespectsTriggerAndIdentity(t *testing.T) {
	self := &larkim.MentionEvent{Key: str("@_user_1"), Id: &larkim.UserId{OpenId: str("ou-bot")}}
	for _, tc := range []struct {
		name, sender, chat, trigger, messageType string
		mentions                                 []*larkim.MentionEvent
		accepted                                 bool
	}{
		{"private sticker", "user", "p2p", "mention", "sticker", nil, true},
		{"private audio", "user", "p2p", "mention", "audio", nil, true},
		{"future type", "user", "p2p", "mention", "future_type", nil, true},
		{"mentioned group", "user", "group", "mention", "sticker", []*larkim.MentionEvent{self}, true},
		{"unmentioned group", "user", "group", "mention", "sticker", nil, false},
		{"all group", "user", "group", "all", "sticker", nil, true},
		{"bot", "bot", "p2p", "all", "sticker", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			event := inbound(tc.sender, tc.chat, tc.messageType, `{"file_key":"ignored"}`, tc.mentions...)
			event.EventV2Base.Header.EventType = "im.message.receive_v1"
			raw, err := json.Marshal(event)
			require.NoError(t, err)
			got, accepted, err := NormalizeWebhookEvent(raw, core.Binding{ID: "binding-1", AppID: "app-1", GroupTrigger: tc.trigger}, "ou-bot")
			require.NoError(t, err)
			require.Equal(t, tc.accepted, accepted)
			if !accepted {
				return
			}
			require.Equal(t, core.MessageControl, core.ClassifyMessage(got))
			require.Equal(t, "om-1", got.MessageID)
			require.Equal(t, "ou-sender", got.SenderID)
			require.Empty(t, got.Text)
			require.Empty(t, got.Attachments)
		})
	}
}

func TestInboundFailureIsNotAcknowledged(t *testing.T) {
	want := errors.New("persist failed")
	p := &Provider{binding: core.Binding{ID: "binding-1", AppID: "app-1", GroupTrigger: "all"}, botOpenID: "ou-bot"}
	err := p.handle(context.Background(), inbound("user", "group", "text", `{"text":"hello"}`), func(context.Context, core.Message) error { return want })
	require.ErrorIs(t, err, want)
	bad := inbound("user", "group", "text", `{"text":"hello"}`)
	bad.Event.Message.MessageId = nil
	require.Error(t, p.handle(context.Background(), bad, func(context.Context, core.Message) error { t.Fatal("accepted bad event"); return nil }))
	for _, mutate := range []func(*larkim.P2MessageReceiveV1){
		func(e *larkim.P2MessageReceiveV1) { e.EventV2Base.Header.TenantKey = "" },
		func(e *larkim.P2MessageReceiveV1) { e.EventV2Base.Header.AppID = "other-app" },
		func(e *larkim.P2MessageReceiveV1) { e.Event.Message.Content = str("bad-json") },
	} {
		bad = inbound("user", "group", "text", `{"text":"hello"}`)
		mutate(bad)
		require.Error(t, p.handle(context.Background(), bad, func(context.Context, core.Message) error { t.Fatal("accepted bad event"); return nil }))
	}
}

func TestNormalizeMentionedGroupStopIsControl(t *testing.T) {
	self := &larkim.MentionEvent{Key: str("@_user_1"), Id: &larkim.UserId{OpenId: str("ou-bot")}}
	event := inbound("user", "group", "text", `{"text":"@_user_1 /stop"}`, self)
	event.EventV2Base.Header.EventType = "im.message.receive_v1"
	raw, err := json.Marshal(event)
	require.NoError(t, err)
	message, accepted, err := NormalizeWebhookEvent(raw, core.Binding{ID: "binding-1", AppID: "app-1", GroupTrigger: "mention"}, "ou-bot")
	require.NoError(t, err)
	require.True(t, accepted)
	require.True(t, message.Mentioned)
	require.Equal(t, "/stop", message.Text)
	require.Equal(t, "ot-1", message.ThreadID)
	require.Equal(t, core.MessageControl, core.ClassifyMessage(message))
}

func TestNormalizeAttachments(t *testing.T) {
	binding := core.Binding{ID: "binding-1", AppID: "app-1", GroupTrigger: "all"}
	got, ok, err := normalize(inbound("user", "p2p", "image", `{"image_key":"img_1"}`), binding, "ou-bot")
	require.NoError(t, err)
	require.True(t, ok)
	require.Empty(t, got.Text)
	require.Equal(t, []core.Attachment{{Kind: "image", Key: "img_1"}}, got.Attachments)
	got, ok, err = normalize(inbound("user", "p2p", "file", `{"file_key":"file_1","file_name":"report.pdf"}`), binding, "ou-bot")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []core.Attachment{{Kind: "file", Key: "file_1", FileName: "report.pdf"}}, got.Attachments)
	post := `{"title":"周报","content":[[{"tag":"text","text":"看下这张图 "},{"tag":"a","text":"链接","href":"https://example.com"}],[{"tag":"img","image_key":"img_2"}]]}`
	got, ok, err = normalize(inbound("user", "p2p", "post", post), binding, "ou-bot")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "周报\n看下这张图 链接 (https://example.com)", got.Text)
	require.Equal(t, []core.Attachment{{Kind: "image", Key: "img_2"}}, got.Attachments)
	localized := `{"zh_cn":{"title":"","content":[[{"tag":"text","text":"你好"}]]}}`
	got, ok, err = normalize(inbound("user", "p2p", "post", localized), binding, "ou-bot")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "你好", got.Text)
	_, _, err = normalize(inbound("user", "p2p", "image", `{}`), binding, "ou-bot")
	require.Error(t, err)
}

func TestNormalizePostFiles(t *testing.T) {
	for _, tc := range []struct {
		name, raw   string
		attachments []core.Attachment
		invalid     bool
	}{
		{"reported PDF", `{"title":"","content":[[{"tag":"text","text":"做个视频"}]],"content_v2":[[{"tag":"text","text":"做个视频"}]],"files":[{"file_key":"file_pdf","file_name":"OpenViking GTM 资料汇总.pdf","is_folder":false}]}`, []core.Attachment{{Kind: "file", Key: "file_pdf", FileName: "OpenViking GTM 资料汇总.pdf"}}, false},
		{"files only", `{"files":[{"file_key":"file_pdf","file_name":"report.pdf"}]}`, []core.Attachment{{Kind: "file", Key: "file_pdf", FileName: "report.pdf"}}, false},
		{"localized", `{"en_us":{"files":[{"file_key":"english"}]},"zh_cn":{"files":[{"file_key":"chinese"}]}}`, []core.Attachment{{Kind: "file", Key: "chinese"}}, false},
		{"image and files", `{"content":[[{"tag":"img","image_key":"img_1"}]],"files":[{"file_key":"file_pdf"}]}`, []core.Attachment{{Kind: "image", Key: "img_1"}, {Kind: "file", Key: "file_pdf"}}, false},
		{"missing key", `{"content":[[{"tag":"text","text":"读取附件"}]],"files":[{"file_name":"report.pdf"}]}`, nil, true},
		{"folder", `{"content":[[{"tag":"text","text":"读取附件"}]],"files":[{"file_key":"folder","is_folder":true}]}`, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok, err := normalize(inbound("user", "p2p", "post", tc.raw), core.Binding{ID: "binding-1", AppID: "app-1"}, "ou-bot")
			if tc.invalid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, tc.attachments, got.Attachments)
		})
	}
}

func TestNormalizeWebhookEventAfterHostVerification(t *testing.T) {
	event := inbound("user", "p2p", "text", `{"text":"hello"}`)
	event.EventV2Base.Header.EventType = "im.message.receive_v1"
	raw, err := json.Marshal(event)
	require.NoError(t, err)
	got, ok, err := NormalizeWebhookEvent(raw, core.Binding{ID: "binding-1", AppID: "app-1", GroupTrigger: "mention"}, "ou-bot")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "hello", got.Text)
	_, _, err = NormalizeWebhookEvent(raw, core.Binding{ID: "binding-1", AppID: "foreign"}, "ou-bot")
	require.Error(t, err)
	_, _, err = NormalizeWebhookEvent([]byte(`{"header":{"event_type":"other"}}`), core.Binding{}, "ou-bot")
	require.Error(t, err)
}

func TestLocalizedPostSelectionIsStableForTextAndAttachments(t *testing.T) {
	for _, tc := range []struct{ name, raw, want, image string }{
		{"preferred chinese", `{"en_us":{"title":"English","content":[[{"tag":"img","image_key":"english"}]]},"zh_cn":{"title":"中文","content":[[{"tag":"img","image_key":"chinese"}]]}}`, "中文", "chinese"},
		{"preferred english", `{"ja_jp":{"title":"日本語","content":[[{"tag":"img","image_key":"japanese"}]]},"en_us":{"title":"English","content":[[{"tag":"img","image_key":"english"}]]}}`, "English", "english"},
		{"sorted fallback", `{"zz_zz":{"title":"last","content":[[{"tag":"img","image_key":"last"}]]},"aa_aa":{"title":"first","content":[[{"tag":"img","image_key":"first"}]]}}`, "first", "first"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for i := 0; i < 128; i++ {
				message, ok, err := normalize(inbound("user", "p2p", "post", tc.raw), core.Binding{ID: "binding", AppID: "app-1"}, "bot")
				require.NoError(t, err)
				require.True(t, ok)
				require.Equal(t, tc.want, message.Text)
				require.Equal(t, []core.Attachment{{Kind: "image", Key: tc.image}}, message.Attachments)
			}
		})
	}
}
