// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeOutputPath(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want string
		ok   bool
	}{
		{"/mnt/session/outputs/report.md", "/mnt/session/outputs/report.md", true},
		{"/mnt/session/outputs/sub/a b.png", "/mnt/session/outputs/sub/a b.png", true},
		{"/mnt/session/storage/x.mp4", "/mnt/session/storage/x.mp4", true},
		{"/workspace/outputs/clip.mp4", "/mnt/session/outputs/clip.mp4", true},
		{"/mnt/session/outputs/../uploads/secret.csv", "", false},
		{"/mnt/session/outputs/../../etc/passwd", "", false},
		{"/mnt/session/outputs/", "", false},
		{"/mnt/session/outputs", "", false},
		{"/mnt/session/outputs/.hidden", "", false},
		{"/mnt/session/uploads/in.csv", "", false},
		{"/tmp/a.md", "", false},
		{"/mnt/session/outputs/a.md?x=1", "", false},
		{"/mnt/session/outputs/a\nb.md", "", false},
		{"", "", false},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			got, ok := normalizeOutputPath(tc.raw)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestAnswerOutputPaths(t *testing.T) {
	answer := "搞定啦！\n- 位置： `/mnt/session/outputs/markdown-示例.md`\n- 图：/mnt/session/outputs/a.png。\n" +
		"- 目录 /mnt/session/outputs/ 与 /mnt/session/outputs/sub\n- 越界 /mnt/session/outputs/../uploads/x.csv\n" +
		"- 视频（/workspace/outputs/clip.mp4）\n- 链接 https://example.com/mnt/session/outputs/fake.md\n" +
		"- 嵌套 /home/u/mnt/session/outputs/nested.md\n/mnt/session/storage/user.pdf"
	require.Equal(t, []string{
		"/mnt/session/outputs/markdown-示例.md",
		"/mnt/session/outputs/a.png",
		"/mnt/session/outputs/clip.mp4",
		"/mnt/session/storage/user.pdf",
	}, answerOutputPaths(answer), "paths embedded in URLs or longer paths are ignored")
}

func TestAnswerOutputPathsPreservesMarkdownDestinations(t *testing.T) {
	answer := "![一](/mnt/session/outputs/a\\(1\\).png)\n![二](</mnt/session/outputs/a b.png>)\n/mnt/session/outputs/end.pdf"
	require.Equal(t, []string{"/mnt/session/outputs/a(1).png", "/mnt/session/outputs/a b.png", "/mnt/session/outputs/end.pdf"}, answerOutputPaths(answer))
}

func TestArtifactsFromEventsCollectsOutputPaths(t *testing.T) {
	j := &job{Turn: Turn{InputEventID: "input", Session: TurnSession{SessionID: "sesn-1"}}}
	events := []Event{
		{ID: "input", Type: "user.message"},
		{ID: "w1", Type: "agent.tool_use", ToolName: "write", ToolInput: `{"file_path":"/mnt/session/outputs/report.md","content":"x"}`},
		{ID: "r1", Type: "agent.tool_result", ToolUseID: "w1"},
		{ID: "w2", Type: "agent.tool_use", ToolName: "write", ToolInput: `{"file_path":"/mnt/session/outputs/failed.md"}`},
		{ID: "r2", Type: "agent.tool_result", ToolUseID: "w2", IsError: true},
		{ID: "w3", Type: "agent.tool_use", ToolName: "write", ToolInput: `{"file_path":"/tmp/scratch.md"}`},
		{ID: "e1", Type: "agent.tool_use", ToolName: "edit", ToolInput: `{"file_path":"/mnt/session/outputs/report.md"}`},
		{ID: "b1", Type: "agent.tool_use", ToolName: "bash", ToolInput: `{"command":"echo hi > /mnt/session/outputs/bash.md"}`},
		{ID: "t1", Type: "agent.tool_result", Raw: []byte(`{"content":[{"type":"image","source":{"type":"file","file_id":"file-img"}}]}`)},
	}
	answer := "见 /mnt/session/outputs/bash.md 和 /mnt/session/outputs/report.md"

	got := artifactsFromEvents(events, j, answer, true, true)
	require.Equal(t, []Artifact{
		// 答案引用的按答案顺序排在最前（bash.md、report.md），Files 产物随后；
		// write/edit 中已被答案引用的不会在末尾重复。
		{Kind: "document", Path: "/mnt/session/outputs/bash.md", FileName: "bash.md"},
		{Kind: "document", Path: "/mnt/session/outputs/report.md", FileName: "report.md"},
		{Kind: "image", FileID: "file-img"},
	}, got, "all mode: answer paths first, then file refs; failed writes and non-output paths are skipped")

	eventsWithUnreferenced := append(append([]Event{}, events...),
		Event{ID: "w4", Type: "agent.tool_use", ToolName: "write", ToolInput: `{"file_path":"/mnt/session/outputs/notes.md"}`})
	require.Equal(t, []Artifact{
		{Kind: "document", Path: "/mnt/session/outputs/bash.md", FileName: "bash.md"},
		{Kind: "document", Path: "/mnt/session/outputs/report.md", FileName: "report.md"},
		{Kind: "image", FileID: "file-img"},
		{Kind: "document", Path: "/mnt/session/outputs/notes.md", FileName: "notes.md"},
	}, artifactsFromEvents(eventsWithUnreferenced, j, answer, true, true), "all mode appends write/edit files absent from the answer")

	require.Equal(t, []Artifact{
		{Kind: "document", Path: "/mnt/session/outputs/bash.md", FileName: "bash.md"},
		{Kind: "document", Path: "/mnt/session/outputs/report.md", FileName: "report.md"},
		{Kind: "image", FileID: "file-img"},
	}, artifactsFromEvents(eventsWithUnreferenced, j, answer, true, false), "explicit mode drops unreferenced write/edit files")

	require.Equal(t, []Artifact{{Kind: "image", FileID: "file-img"}}, artifactsFromEvents(events, j, answer, false, false), "self-hosted without a resolver only delivers Files references")
	require.Equal(t, []Artifact{{Kind: "image", FileID: "file-img"}}, artifactsFromEvents(events, j, answer, false, true), "includeAll cannot override the missing resolver")
}

func TestOutputFileDeliveryRequiresSessionAndSafePath(t *testing.T) {
	source := DeliverySource{Kind: SourceTurn, ID: "turn"}
	valid := NewDelivery(source, 1, "artifact:path:/mnt/session/outputs/a.md", "binding", ReplyTarget{ChatID: "chat"}, OutputFileBody(pathArtifact("/mnt/session/outputs/a.md")))
	valid.OutputSessionID = "sesn-1"
	require.True(t, valid.ValidNew(source))

	missingSession := valid
	missingSession.OutputSessionID = ""
	require.False(t, missingSession.ValidNew(source))

	escaped := valid
	escaped.Body = OutputFileBody(Artifact{Kind: "document", Path: "/mnt/session/uploads/a.md"})
	require.False(t, escaped.ValidNew(source))
}

func TestOutputFileBodyRoundTrip(t *testing.T) {
	d := NewDelivery(DeliverySource{Kind: SourceTurn, ID: "turn"}, 1, "k", "binding", ReplyTarget{ChatID: "chat"}, OutputFileBody(pathArtifact("/mnt/session/outputs/clip.mp4")))
	d.OutputSessionID = "sesn-1"
	raw, err := EncodeStored(d)
	require.NoError(t, err)
	var got Delivery
	require.NoError(t, DecodeStored(raw, &got))
	require.Equal(t, BodyOutputFile, got.Body.Type)
	require.Equal(t, Artifact{Kind: "video", Path: "/mnt/session/outputs/clip.mp4", FileName: "clip.mp4"}, got.Body.Artifact)
	require.Equal(t, "sesn-1", got.OutputSessionID)
	require.True(t, SupportsDeliveryBody(got.Body))
}
