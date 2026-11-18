// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

type toolDetailProgressProvider struct {
	nativeProgressProvider
	mu      sync.Mutex
	updates []core.ProgressUpdate
}

func (p *toolDetailProgressProvider) ReportDurableProgress(ctx context.Context, cp core.ProgressCheckpoint, update core.ProgressUpdate) (core.ProgressCheckpoint, error) {
	p.mu.Lock()
	p.updates = append(p.updates, update)
	p.mu.Unlock()
	return p.nativeProgressProvider.ReportDurableProgress(ctx, cp, update)
}

func TestCOTToolResultFlushesWhenHistoryAlreadyTerminal(t *testing.T) {
	p := &toolDetailProgressProvider{}
	b := runtimeBinding()
	b.ProgressProtocol, b.ProgressMode = "native_cot", "tools"
	opts := testRuntimeOptions()
	opts.ProgressInterval = time.Minute
	h := newHarnessWith(t, &conformance.MAServer{}, p, b, opts)
	h.enqueue(runtimeMessage())
	h.run()
	require.Eventually(t, func() bool { n, _ := p.counts(); return n == 1 }, time.Second, time.Millisecond)
	events := []map[string]any{
		{"id": "tool", "type": "agent.tool_use", "name": "bash", "input": map[string]string{"command": "cat file.md"}},
		{"id": "tool-result", "type": "agent.tool_result", "tool_use_id": "tool", "content": []map[string]string{{"type": "text", "text": "file contents"}}},
	}
	h.fixture.AppendAfterInput(t, append(events, conformance.AnswerEvents("final answer")...)...)
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 4*time.Second)
	p.mu.Lock()
	defer p.mu.Unlock()
	var steps []core.ProgressStep
	for _, update := range p.updates {
		if update.State == core.ProgressRunning && len(update.Steps) > 0 {
			steps = update.Steps
		}
	}
	require.Len(t, steps, 1, "终态扫描应强制补齐工具结果，不受进度节流限制")
	require.Contains(t, steps[0].Arguments, "cat file.md")
	require.Equal(t, "file contents", steps[0].Output)
	require.True(t, steps[0].Done)
	for _, delivery := range state.export.Deliveries {
		if delivery.Body.Type == core.BodyProgress {
			encoded, err := json.Marshal(delivery)
			require.NoError(t, err)
			require.NotContains(t, string(encoded), "cat file.md", "明细只在运行期间发送，不进入闭合投递正文")
			require.NotContains(t, string(encoded), "file contents")
		}
	}
}
