// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type richReplyFixture struct {
	MAClient
	Provider
	downloads []string
}

func (f *richReplyFixture) DownloadSessionFile(_ context.Context, session, id string, _ int64) (FileContent, error) {
	f.downloads = append(f.downloads, session+"/"+id)
	return FileContent{Body: io.NopCloser(strings.NewReader("fixture")), Size: 7}, nil
}
func (*richReplyFixture) ResolveSessionFileURL(context.Context, string, string) (string, error) {
	return "", nil
}
func (*richReplyFixture) UploadInlineMedia(_ context.Context, a Artifact, _ FileContent) (RichTextPart, error) {
	if a.FileID == "file-bad" {
		return RichTextPart{}, errors.New("upload failed")
	}
	return RichTextPart{Type: a.Kind, ResourceKey: "uploaded_" + a.FileID}, nil
}
func (*richReplyFixture) DeliverRichText(context.Context, Delivery) (string, error) {
	return "message", nil
}

func TestPrepareRichReplyReplacesMediaInPlaceAndKeepsFailedReference(t *testing.T) {
	f := &richReplyFixture{}
	r := &Runtime{options: RuntimeOptions{Artifacts: ArtifactLimits{MaxCount: 5, MaxBytes: 1024}}}
	j := &job{Turn: Turn{Session: TurnSession{SessionID: "sesn-demo"}}, binding: Binding{DeliverArtifacts: true, StreamFinalAnswer: true}, connection: Connection{MAClient: f, Provider: f}}
	files := []SessionOutputFile{
		{FileID: "file-one", Path: "/mnt/session/outputs/a.png"},
		{FileID: "file-video", Path: "/mnt/session/outputs/v.mp4"},
		{FileID: "file-bad", Path: "/mnt/session/outputs/b.png"},
	}
	answer := "图片 `a.png`\n视频 [预览](v.mp4)\n坏图 ![图](b.png)\n再看 `a.png`\n结束"
	j.answerReferences = matchOutputReferences(answer, files)
	var artifacts []Artifact
	for _, ref := range j.answerReferences {
		a := ref.Artifact
		a.SessionID = "sesn-demo"
		artifacts = append(artifacts, a)
	}
	_, remaining := r.prepareRichReply(t.Context(), j, answer, artifacts)
	require.NotEmpty(t, j.richAnswer)
	require.Equal(t, []string{"sesn-demo/file-one", "sesn-demo/file-video", "sesn-demo/file-bad"}, f.downloads)
	require.Len(t, remaining, 1)
	require.Equal(t, "file-bad", remaining[0].FileID)
	var types, keys []string
	var text strings.Builder
	for _, part := range j.richAnswer {
		types = append(types, part.Type)
		keys = append(keys, part.ResourceKey)
		text.WriteString(part.Text)
	}
	require.Equal(t, []string{"text", "image", "text", "video", "text", "image", "text"}, types)
	require.Equal(t, []string{"", "uploaded_file-one", "", "uploaded_file-video", "", "uploaded_file-one", ""}, keys)
	require.Contains(t, text.String(), "坏图 ![图](b.png)")
	require.Contains(t, text.String(), "结束")
	body := RichTextBody(j.richAnswer)
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	var restored DeliveryBody
	require.NoError(t, json.Unmarshal(raw, &restored))
	require.Equal(t, body, restored)
	d := NewDelivery(DeliverySource{Kind: SourceTurn, ID: "turn"}, 0, "answer", "binding", ReplyTarget{}, body)
	require.True(t, d.ValidNew(d.Source))
	require.True(t, SupportsDeliveryBody(restored))
}

func TestPrepareRichReplyDoesNotRemoveMediaWhenPayloadTooLarge(t *testing.T) {
	f := &richReplyFixture{}
	r := &Runtime{options: RuntimeOptions{Artifacts: ArtifactLimits{MaxCount: 5, MaxBytes: 1024}}}
	j := &job{binding: Binding{DeliverArtifacts: true}, connection: Connection{MAClient: f, Provider: f}}
	answer := strings.Repeat("文字", 10000) + "`a.png`"
	j.answerReferences = matchOutputReferences(answer, []SessionOutputFile{{FileID: "file-one", Path: "/mnt/session/outputs/a.png"}})
	a := j.answerReferences[0].Artifact
	a.SessionID = "sesn-demo"
	_, remaining := r.prepareRichReply(t.Context(), j, answer, []Artifact{a})
	require.Empty(t, j.richAnswer)
	require.Len(t, remaining, 1)
}
