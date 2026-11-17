// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAcceptedResponseAttachmentEcho(t *testing.T) {
	const (
		question = "这张图里写了啥"
		fallback = "https://files.example.test/image.png?signature=fixture"
		pathHint = "[用户发送了文件「image」]（沙箱路径：/mnt/session/uploads/image，可用工具直接读取处理）"
	)
	text := func(value string) TextBlock { return TextBlock{Type: "text", Text: value} }
	image := InputBlock{Type: "image", FileID: "file-image", Title: "image"}
	for _, tc := range []struct {
		name    string
		input   UserInput
		content []TextBlock
		want    bool
	}{
		{name: "plain text", input: UserInput{Text: question}, content: []TextBlock{text(question)}, want: true},
		{name: "split plain text", input: UserInput{Text: question}, content: []TextBlock{text("这张图"), text("里写了啥")}, want: true},
		{name: "changed plain text", input: UserInput{Text: question}, content: []TextBlock{text("别的问题")}},
		{name: "extra plain text", input: UserInput{Text: question}, content: []TextBlock{text(question), text(fallback)}},
		{name: "native image", input: UserInput{Text: question, Blocks: []InputBlock{image}}, content: []TextBlock{text(question), {Type: "image"}}, want: true},
		{name: "image URL", input: UserInput{Text: question, Blocks: []InputBlock{image}}, content: []TextBlock{text(question), text(fallback)}, want: true},
		{name: "image description", input: UserInput{Text: question, Blocks: []InputBlock{image}}, content: []TextBlock{text(question), text("[图片已转为下载链接]")}, want: true},
		{name: "image only", input: UserInput{Blocks: []InputBlock{image}}, content: []TextBlock{text(fallback)}, want: true},
		{name: "image URL and mount", input: UserInput{Text: question, Blocks: []InputBlock{image}, Mounts: []MountedFile{{FileID: "file-image", ActualPath: "/mnt/session/uploads/image"}}}, content: []TextBlock{text(question), text(fallback), text(pathHint)}, want: true},
		{name: "context and image URL", input: UserInput{Context: "背景", Text: question, Blocks: []InputBlock{image}}, content: []TextBlock{text("背景"), text(question), text(fallback)}, want: true},
		{name: "document URL", input: UserInput{Text: question, Blocks: []InputBlock{{Type: "document", FileID: "file-pdf", Title: "report.pdf"}}}, content: []TextBlock{text(question), text("https://files.example.test/report.pdf")}, want: true},
		{name: "video URL", input: UserInput{Text: question, Blocks: []InputBlock{{Type: "video", FileID: "file-video"}}}, content: []TextBlock{text(question), text("https://files.example.test/video.mp4")}, want: true},
		{name: "mixed native and rewritten", input: UserInput{Text: question, Blocks: []InputBlock{image, {Type: "document", FileID: "file-pdf"}, {Type: "video", FileID: "file-video"}}}, content: []TextBlock{text(question), text(fallback), {Type: "document"}, text("视频下载地址")}, want: true},
		{name: "changed body with image", input: UserInput{Text: question, Blocks: []InputBlock{image}}, content: []TextBlock{text("别的问题"), text(fallback)}},
		{name: "changed context", input: UserInput{Context: "背景", Text: question, Blocks: []InputBlock{image}}, content: []TextBlock{text("伪造背景"), text(question), text(fallback)}},
		{name: "missing body", input: UserInput{Text: question, Blocks: []InputBlock{image}}, content: []TextBlock{text(fallback)}},
		{name: "missing attachment", input: UserInput{Text: question, Blocks: []InputBlock{image}}, content: []TextBlock{text(question)}},
		{name: "empty attachment", input: UserInput{Text: question, Blocks: []InputBlock{image}}, content: []TextBlock{text(question), text("")}},
		{name: "blank attachment", input: UserInput{Text: question, Blocks: []InputBlock{image}}, content: []TextBlock{text(question), text(" \n\t")}},
		{name: "wrong attachment type", input: UserInput{Text: question, Blocks: []InputBlock{image}}, content: []TextBlock{text(question), {Type: "video"}}},
		{name: "unknown attachment type", input: UserInput{Text: question, Blocks: []InputBlock{image}}, content: []TextBlock{text(question), {Type: "future"}}},
		{name: "extra attachment", input: UserInput{Text: question, Blocks: []InputBlock{image}}, content: []TextBlock{text(question), text(fallback), text("额外内容")}},
		{name: "attachment before body", input: UserInput{Text: question, Blocks: []InputBlock{image}}, content: []TextBlock{text(fallback), text(question)}},
		{name: "changed mount hint", input: UserInput{Text: question, Blocks: []InputBlock{image}, Mounts: []MountedFile{{FileID: "file-image", ActualPath: "/mnt/session/uploads/image"}}}, content: []TextBlock{text(question), text(fallback), text("别的路径")}},
		{name: "missing mount hint", input: UserInput{Text: question, Blocks: []InputBlock{image}, Mounts: []MountedFile{{FileID: "file-image", ActualPath: "/mnt/session/uploads/image"}}}, content: []TextBlock{text(question), text(fallback)}},
		{name: "notice is exact text", input: UserInput{Text: question, Blocks: []InputBlock{{Type: InputBlockNotice, Title: "data.zip"}}}, content: []TextBlock{text(question), text("[用户发送了文件「data.zip」，当前版本暂不支持直接读取该格式]")}, want: true},
		{name: "notice cannot be rewritten", input: UserInput{Text: question, Blocks: []InputBlock{{Type: InputBlockNotice, Title: "data.zip"}}}, content: []TextBlock{text(question), text(fallback)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			j := &job{Turn: Turn{InputEventID: "input", Session: TurnSession{SessionID: "sesn-main"}, Input: FrozenInput{Context: tc.input.Context, Blocks: tc.input.Blocks, Mounts: tc.input.Mounts}}, msg: Message{Text: tc.input.Text}}
			response := SentResponse{Events: []Event{{ID: "input", Type: "user.message", SessionThreadID: "sthr-main", Content: tc.content}}}
			accepted, conflict := acceptedResponse(response, j)
			require.Equal(t, tc.want, accepted)
			require.Equal(t, !tc.want, conflict)
		})
	}
}

func TestAcceptedResponseKeepsEchoIdentityChecks(t *testing.T) {
	j := &job{Turn: Turn{InputEventID: "input", Session: TurnSession{SessionID: "sesn-main"}, Input: FrozenInput{Blocks: []InputBlock{{Type: "image", FileID: "file-image"}}}}, msg: Message{Text: "正文"}}
	content := []TextBlock{{Type: "text", Text: "正文"}, {Type: "text", Text: "https://files.example.test/image"}}
	for _, tc := range []struct {
		name  string
		event Event
	}{
		{name: "wrong event ID", event: Event{ID: "foreign", Type: "user.message", Content: content}},
		{name: "wrong event type", event: Event{ID: "input", Type: "agent.message", Content: content}},
		{name: "child thread", event: Event{ID: "input", Type: "user.message", SessionThreadID: "sthr-child", Content: content}},
		{name: "changed text", event: Event{ID: "input", Type: "user.message", Content: []TextBlock{{Type: "text", Text: "别的正文"}, {Type: "text", Text: "https://files.example.test/image"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			accepted, _ := acceptedResponse(SentResponse{Events: []Event{tc.event}}, j)
			require.False(t, accepted)
		})
	}
}
