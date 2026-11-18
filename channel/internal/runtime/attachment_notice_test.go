// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestInputBlockNoticeText(t *testing.T) {
	for _, name := range []string{"config.json", "预算表.xlsx", "report.pdf"} {
		t.Run(name, func(t *testing.T) {
			block := core.InputBlock{Type: core.InputBlockNotice, FileID: "file-1", Title: name}
			path := "/mnt/session/uploads/" + name
			mountedText := "[用户发送了文件「" + name + "」]（沙箱路径：" + path + "，可用工具直接读取处理）"
			unmountedText := "[用户发送了文件「" + name + "」，当前版本暂不支持直接读取该格式]"
			for _, tc := range []struct {
				name string
				path string
				want string
			}{
				{name: "mounted", path: path, want: mountedText},
				{name: "unmounted", want: unmountedText},
			} {
				t.Run(tc.name, func(t *testing.T) {
					require.Equal(t, tc.want, block.NoticeText(tc.path))
					input := core.UserInput{Text: "介绍一下这个文档", Blocks: []core.InputBlock{block}}
					if tc.path != "" {
						input.Mounts = []core.MountedFile{{FileID: block.FileID, ActualPath: tc.path}}
					}
					require.Equal(t, tc.want, input.AttachmentText(block))
					require.Equal(t, input.Text+tc.want, input.ExpectedText())
				})
			}
		})
	}
}
