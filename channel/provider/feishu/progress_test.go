// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestProgressCardCreateThenPatch(t *testing.T) {
	var created, patched []map[string]any
	p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal") {
			tokenResponse(w)
			return
		}
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/open-apis/im/v1/messages/om-source/reply":
			created = append(created, body)
			_, _ = io.WriteString(w, `{"code":0,"data":{"message_id":"om-card"}}`)
		case r.Method == http.MethodPatch && r.URL.Path == "/open-apis/im/v1/messages/om-card":
			patched = append(patched, body)
			_, _ = io.WriteString(w, `{"code":0}`)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
	})
	update := core.ProgressUpdate{TurnID: "turn:1", ReplyTarget: core.ReplyTarget{Mode: "reply", MessageID: "om-source"}, State: core.ProgressRunning, Steps: []core.ProgressStep{{EventID: "e1", Kind: "tool", Name: "bash"}}}
	handle, err := p.ReportProgress(context.Background(), "", update)
	require.NoError(t, err)
	require.Equal(t, "om-card", handle)
	require.Len(t, created, 1)
	require.Equal(t, "interactive", created[0]["msg_type"])
	require.Equal(t, progressUUID("turn:1"), created[0]["uuid"], "card creation is idempotent per turn")
	require.Contains(t, created[0]["content"], "bash")
	require.Contains(t, created[0]["content"], "处理中")
	update.State = core.ProgressDone
	update.Steps[0].Done = true
	handle, err = p.ReportProgress(context.Background(), handle, update)
	require.NoError(t, err)
	require.Equal(t, "om-card", handle)
	require.Len(t, patched, 1)
	require.Contains(t, patched[0]["content"], "已完成")
	require.Contains(t, patched[0]["content"], "✅")
}

func TestProgressCardSanitizesAndBoundsContent(t *testing.T) {
	steps := make([]core.ProgressStep, 0, 30)
	for i := 0; i < 30; i++ {
		steps = append(steps, core.ProgressStep{Name: "tool"})
	}
	steps = append(steps, core.ProgressStep{Name: "[click](https://evil)" + strings.Repeat("x", 200) + "\n**bold**"})
	content, err := progressCard(core.ProgressUpdate{TurnID: "t", State: core.ProgressStopped, Steps: steps, Summary: "用户已停止"})
	require.NoError(t, err)
	var card struct {
		Header struct {
			Title struct {
				Content string `json:"content"`
			} `json:"title"`
		} `json:"header"`
		Elements []struct {
			Text struct {
				Tag     string `json:"tag"`
				Content string `json:"content"`
			} `json:"text"`
		} `json:"elements"`
	}
	require.NoError(t, json.Unmarshal([]byte(content), &card))
	require.Equal(t, "已停止", card.Header.Title.Content)
	body := card.Elements[0].Text.Content
	require.Contains(t, body, "之前还有 11 步")
	require.Equal(t, "plain_text", card.Elements[0].Text.Tag, "工具名与摘要必须按纯文本渲染")
	require.Contains(t, body, "[click]", "纯文本保留工具名而不解释 markdown")
	require.NotContains(t, body, strings.Repeat("x", 100), "long names are truncated")
	require.Contains(t, body, "用户已停止")
	require.Contains(t, body, "⏹", "unfinished steps show as stopped after a terminal state")
}

func TestProgressCardFailureIsClassified(t *testing.T) {
	p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal") {
			tokenResponse(w)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"code":230020,"msg":"rate limited"}`)
	})
	_, err := p.ReportProgress(context.Background(), "om-card", core.ProgressUpdate{TurnID: "t", State: core.ProgressRunning})
	var failure core.DeliveryFailure
	require.ErrorAs(t, err, &failure)
	require.False(t, failure.Uncertain())
}

func TestStatusOnlyTerminalCardDoesNotClaimStillThinking(t *testing.T) {
	for _, tc := range []struct {
		state core.ProgressState
		title string
	}{
		{core.ProgressDone, "已完成"}, {core.ProgressFailed, "未完成"}, {core.ProgressStopped, "已停止"},
	} {
		t.Run(string(tc.state), func(t *testing.T) {
			content, err := progressCard(core.ProgressUpdate{TurnID: "turn", State: tc.state})
			require.NoError(t, err)
			require.Contains(t, content, tc.title)
			require.NotContains(t, content, "正在思考", "终态状态卡片正文也应与标题保持一致")
		})
	}
}

func TestProgressCardTreatsToolTextAsLiteral(t *testing.T) {
	text := "# ![image](url) | ~strike~ - list\r<at id=all>\nnext\x00"
	content, err := progressCard(core.ProgressUpdate{TurnID: "turn", State: core.ProgressDone,
		Steps: []core.ProgressStep{{Name: text, Done: true}}, Summary: text})
	require.NoError(t, err)
	var card struct {
		Elements []struct {
			Text struct {
				Tag     string `json:"tag"`
				Content string `json:"content"`
			} `json:"text"`
		} `json:"elements"`
	}
	require.NoError(t, json.Unmarshal([]byte(content), &card))
	require.Len(t, card.Elements, 1)
	require.Equal(t, "plain_text", card.Elements[0].Text.Tag)
	require.Contains(t, card.Elements[0].Text.Content, "# ![image](url) | ~strike~ - list <at id=all> next ")
	require.NotContains(t, card.Elements[0].Text.Content, "\r")
	require.NotContains(t, card.Elements[0].Text.Content, "\x00")
}
