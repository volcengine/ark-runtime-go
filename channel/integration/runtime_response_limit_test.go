// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestRuntimeLargeMediaHistoryCompletesWithoutInterrupt(t *testing.T) {
	fixture, provider := &conformance.MAServer{PageSize: 1}, &runtimeProvider{}
	store, err := newEmbeddedStore(t)
	require.NoError(t, err)
	options := testRuntimeOptions()
	options.TurnTimeout = 15 * time.Second
	options.LeaseTTL = time.Minute
	options.MessageDeadline = time.Minute
	binding := runtimeBinding()
	binding.MaxTurnDuration = options.TurnTimeout
	runtime, err := core.NewRuntime(store, []core.Connection{{Binding: binding, MAClient: newMAForHarness(t, fixture), Provider: provider}}, options)
	require.NoError(t, err)
	mustEnqueue(t, runtime, "binding", runtimeMessage())
	runUntilCleanup(t, runtime)
	var events []map[string]any
	for i := range 3 {
		events = append(events, map[string]any{"id": fmt.Sprint("image-", i), "type": "agent.tool_result", "content": []any{
			map[string]any{"type": "image", "source": map[string]string{"type": "base64", "media_type": "image/png", "data": strings.Repeat("A", 6<<20)}},
		}})
	}
	events = append(events,
		map[string]any{"id": "answer", "type": "agent.message", "content": []map[string]string{{"type": "text", "text": "完成"}}},
		map[string]any{"id": "idle", "type": "session.status_idle", "stop_reason": map[string]string{"type": "end_turn"}},
	)
	fixture.AppendAfterInput(t, events...)
	state := waitLatest(t, store, core.TurnDone, core.DeliveryDone, 15*time.Second)
	require.Equal(t, "完成", state.export.Deliveries[0].Body.Text)
	fixture.Lock()
	defer fixture.Unlock()
	require.Equal(t, 1, fixture.SendCount)
	require.Zero(t, fixture.InterruptCount)
}
