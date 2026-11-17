// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMatchSessionOutputReferences(t *testing.T) {
	files := []SessionOutputFile{
		{FileID: "file-one", Path: "/mnt/session/outputs/cats/cat1.jpg", FileName: "cat1.jpg", MimeType: "image/jpeg"},
		{FileID: "file-two", Path: "/mnt/session/outputs/cats/cat2.jpg", FileName: "cat2.jpg", MimeType: "image/jpeg"},
		{FileID: "file-three", Path: "/mnt/session/outputs/cats/cat3.jpg", FileName: "cat3.jpg", MimeType: "image/jpeg"},
		{FileID: "file-old", Path: "/mnt/session/outputs/old/cat1.jpg", FileName: "cat1.jpg", MimeType: "image/jpeg"},
	}
	for _, tc := range []struct {
		name, answer string
		want         []string
	}{
		{"directory and filenames", "目录：`/mnt/session/outputs/cats/`\n\n- `cat1.jpg`\n- `cat2.jpg`\n- `cat3.jpg`", []string{"file-one", "file-two", "file-three"}},
		{"unique bare filename", "图片 cat2.jpg 在这里", []string{"file-two"}},
		{"ambiguous bare filename", "`cat1.jpg`", nil},
		{"full paths override names", "`/mnt/session/outputs/old/cat1.jpg`\n`cat2.jpg`", []string{"file-old", "file-two"}},
		{"wrong full path cannot match basename", "`/mnt/session/outputs/missing/cat2.jpg`", nil},
		{"URL is not a filename reference", "https://example.com/cat2.jpg", nil},
		{"code example ignored", "```sh\ncat /mnt/session/outputs/cats/cat2.jpg\n```\n展示 `cat3.jpg`", []string{"file-three"}},
		{"markdown media and order", "![二](/mnt/session/outputs/cats/cat2.jpg)\n[三](cat3.jpg)", []string{"file-two", "file-three"}},
		{"escaped path", "![图](</mnt/session/outputs/cats/cat2.jpg>)", []string{"file-two"}},
		{"longer filename ignored", "notcat2.jpg cat2.jpg.bak", nil},
		{"filename label and path on same line", "`cat2.jpg` → `/mnt/session/outputs/cats/cat2.jpg`", []string{"file-two"}},
		{"explicit directory does not fall back elsewhere", "目录 `/mnt/session/outputs/old/`\n- `cat2.jpg`", nil},
		{"unknown explicit directory does not fall back elsewhere", "目录 `/mnt/session/outputs/missing/`\n- `cat2.jpg`", nil},
		{"root directory is explicit", "目录 `/mnt/session/outputs/`\n- `cat2.jpg`", nil},
		{"directory without trailing slash", "目录 `/mnt/session/outputs/missing`\n- `cat2.jpg`", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			refs := matchOutputReferences(tc.answer, files)
			var ids []string
			for _, ref := range refs {
				ids = append(ids, ref.Artifact.FileID)
			}
			require.Equal(t, tc.want, ids)
			for _, ref := range refs {
				require.NotEmpty(t, tc.answer[ref.Start:ref.End])
			}
		})
	}
}

func TestMatchSessionOutputReferencesDirectoryWithSpaces(t *testing.T) {
	files := []SessionOutputFile{{FileID: "file-one", Path: "/mnt/session/outputs/my cats/a.png"}, {FileID: "file-two", Path: "/mnt/session/outputs/other/a.png"}}
	refs := matchOutputReferences("目录 `/mnt/session/outputs/my cats/`\n- `a.png`", files)
	require.Len(t, refs, 1)
	require.Equal(t, "file-one", refs[0].Artifact.FileID)
}

func TestAnswerOutputPathsIgnoresFencedExamples(t *testing.T) {
	require.Empty(t, answerOutputPaths("示例：\n```sh\ncat /mnt/session/outputs/secret.png\n```"))
}

func TestMatchSessionOutputReferencesKeepsWholeMarkdownAndInlineCode(t *testing.T) {
	files := []SessionOutputFile{{FileID: "file-one", Path: "/mnt/session/outputs/猫 (1).png", FileName: "猫 (1).png"}}
	answer := "前![小猫](</mnt/session/outputs/猫 (1).png>)中`猫 (1).png`后"
	refs := matchOutputReferences(answer, files)
	require.Len(t, refs, 2)
	require.Equal(t, "![小猫](</mnt/session/outputs/猫 (1).png>)", answer[refs[0].Start:refs[0].End])
	require.Equal(t, "`猫 (1).png`", answer[refs[1].Start:refs[1].End])
}
