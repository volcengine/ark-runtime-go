// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUserAuthPromptUsesHyperlink(t *testing.T) {
	const authorizeURL = "https://accounts.feishu.cn/open-apis/authen/v1/authorize?client_id=cli_test&scope=offline_access+docs%3Adocument.content%3Aread&state=opaque-state&code_challenge=opaque-challenge"
	for _, tc := range []struct {
		name     string
		userCode string
	}{
		{name: "first authorization"},
		{name: "device authorization", userCode: "ABCD-EFGH"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &Runtime{}
			j := &job{Turn: Turn{ID: "turn", BindingID: "binding"}}
			prompt := r.userAuthPrompt(j, UserAuthRecord{Attempt: 1, Batch: 2}, AuthFlow{URL: authorizeURL, UserCode: tc.userCode})
			require.Equal(t, BodyText, prompt.Body.Type)
			require.Contains(t, prompt.Body.Text, "[点击授权]("+authorizeURL+")")
			require.NotContains(t, prompt.Body.Text, "\nhttps://")
			require.Equal(t, 1, strings.Count(prompt.Body.Text, authorizeURL))
			if tc.userCode != "" {
				require.Contains(t, prompt.Body.Text, "授权码：ABCD-EFGH")
			}
		})
	}
}
