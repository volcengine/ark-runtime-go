// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type inlineImageFixture struct {
	Provider
	MAClient
	uploads, downloads, closes int
	failDownload, failUpload   bool
}

func (f *inlineImageFixture) DownloadFile(_ context.Context, _ string, _ int64) (FileContent, error) {
	f.downloads++
	if f.failDownload {
		return FileContent{}, errors.New("download failed")
	}
	return FileContent{Body: &inlineImageBody{Reader: strings.NewReader("png"), closed: &f.closes}, Size: 3}, nil
}

func (f *inlineImageFixture) UploadInlineImage(_ context.Context, content FileContent) (string, error) {
	f.uploads++
	if f.failUpload {
		return "", errors.New("upload failed")
	}
	_, err := io.ReadAll(content.Body)
	return "img_fixture", err
}

type inlineImageBody struct {
	io.Reader
	closed *int
}

func (b *inlineImageBody) Close() error { *b.closed++; return nil }

func TestPrepareAnswerImagesPreservesTextAndDeduplicates(t *testing.T) {
	f := &inlineImageFixture{}
	r := &Runtime{options: RuntimeOptions{Artifacts: ArtifactLimits{MaxCount: 5, MaxBytes: 1024}}}
	j := &job{binding: Binding{StreamFinalAnswer: true, DeliverArtifacts: true}, connection: Connection{Provider: f, MAClient: f}}
	answer := "风格一\n![预览](file-one)\n风格二\n![再次预览](file-one)\n请回复 1 或 2。"
	artifact := Artifact{Kind: "image", FileID: "file-one"}
	text, remaining := r.prepareAnswerImages(t.Context(), j, answer, []Artifact{artifact})
	require.Equal(t, "风格一\n![预览](img_fixture)\n风格二\n![再次预览](img_fixture)\n请回复 1 或 2。", text)
	require.Empty(t, remaining, "内嵌成功不再重复发独立图片")
	require.Equal(t, 1, f.downloads)
	require.Equal(t, 1, f.uploads)
	require.Equal(t, 1, f.closes)
	require.Equal(t, "风格一\n`![预览](file-one)`\n风格二\n`![再次预览](file-one)`\n请回复 1 或 2。", j.answerImageFallback)
}

func TestPrepareAnswerImagesMatchesPathAliasForPublicSessionFile(t *testing.T) {
	f := &inlineImageFixture{}
	r := &Runtime{options: RuntimeOptions{Artifacts: ArtifactLimits{MaxCount: 5, MaxBytes: 1024}}}
	j := &job{binding: Binding{StreamFinalAnswer: true, DeliverArtifacts: true}, connection: Connection{Provider: f, MAClient: f}}
	a := Artifact{Kind: "image", FileID: "file-one", Path: "/mnt/session/outputs/a.png"}
	text, remaining := r.prepareAnswerImages(t.Context(), j, "![路径](/mnt/session/outputs/a.png)\n![ID](file-one)", []Artifact{a})
	require.Equal(t, "![路径](img_fixture)\n![ID](img_fixture)", text)
	require.Empty(t, remaining)
	require.Equal(t, 1, f.uploads)
}

func TestPrepareAnswerImagesFallsBackToCompletePlainTextWhenCodeExceedsBudget(t *testing.T) {
	f := &inlineImageFixture{}
	r := &Runtime{}
	j := &job{binding: Binding{StreamFinalAnswer: true, DeliverArtifacts: true}, connection: Connection{Provider: f, MAClient: f}}
	answer := strings.Repeat("a", 23983) + "![a](file-one)"
	require.True(t, CanStreamText(answer))
	text, remaining := r.prepareAnswerImages(t.Context(), j, answer, []Artifact{{Kind: "image", FileID: "file-one"}})
	require.Equal(t, answer, text)
	require.Len(t, remaining, 1)
	require.True(t, j.disableAnswerStream)
	require.Zero(t, f.uploads)
}

func TestPrepareAnswerImagesIncludesSessionNoticeInBudget(t *testing.T) {
	f := &inlineImageFixture{}
	r := &Runtime{}
	j := &job{Turn: Turn{Session: TurnSession{Renewed: true}}, binding: Binding{StreamFinalAnswer: true, DeliverArtifacts: true}, connection: Connection{Provider: f, MAClient: f}}
	answer := strings.Repeat("a", 23970) + "![a](file-one)"
	require.True(t, CanStreamText(answer))
	text, remaining := r.prepareAnswerImages(t.Context(), j, answer, []Artifact{{Kind: "image", FileID: "file-one"}})
	require.Equal(t, answer, text)
	require.Len(t, remaining, 1)
	require.True(t, j.disableAnswerStream)
	require.Zero(t, f.uploads, "无法使用卡片时不能丢弃图片附件")
}

func TestPrepareAnswerImagesFallsBackPerReference(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		failDownload, failUpload bool
		artifacts                bool
	}{
		{"download", true, false, true},
		{"upload", false, true, true},
		{"disabled", false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &inlineImageFixture{failDownload: tc.failDownload, failUpload: tc.failUpload}
			r := &Runtime{options: RuntimeOptions{Artifacts: ArtifactLimits{MaxCount: 5, MaxBytes: 1024}}}
			j := &job{binding: Binding{StreamFinalAnswer: true, DeliverArtifacts: tc.artifacts}, connection: Connection{Provider: f, MAClient: f}}
			artifact := Artifact{Kind: "image", FileID: "file-one"}
			text, remaining := r.prepareAnswerImages(t.Context(), j, "前![图](file-one)后", []Artifact{artifact})
			require.Equal(t, "前`![图](file-one)`后", text)
			require.Equal(t, []Artifact{artifact}, remaining)
			if !tc.artifacts {
				require.Zero(t, f.downloads)
			}
			require.Equal(t, f.uploads, f.closes)
		})
	}
}

func TestPrepareAnswerImagesDoesNotFetchArbitraryDestinationsOrCode(t *testing.T) {
	f := &inlineImageFixture{}
	r := &Runtime{}
	j := &job{binding: Binding{StreamFinalAnswer: true, DeliverArtifacts: true}, connection: Connection{Provider: f, MAClient: f}}
	answer := "![外链](https://private.test/a.png)\n`![示例](file-one)`\n```md\n![示例](file-one)\n```"
	text, remaining := r.prepareAnswerImages(t.Context(), j, answer, []Artifact{{Kind: "image", FileID: "file-one"}})
	require.Equal(t, "````![外链](https://private.test/a.png)````\n`![示例](file-one)`\n```md\n![示例](file-one)\n```", text)
	require.Len(t, remaining, 1)
	require.Zero(t, f.downloads)
}
