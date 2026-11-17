// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package conformance

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func unsupportedMessageContract(t *testing.T, open opener) {
	for _, active := range []bool{false, true} {
		name := "empty conversation"
		if active {
			name = "active turn"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			s, _ := open(t, StoreConfig{})
			var running *core.Turn
			if active {
				running = ingest(t, s, P2PMessage("running")).Turn
			}
			m := P2PMessage("unsupported")
			m.Text = ""
			raw, err := json.Marshal(m)
			require.NoError(t, err)
			var fields map[string]any
			require.NoError(t, json.Unmarshal(raw, &fields))
			fields["unsupported"] = true
			raw, err = json.Marshal(fields)
			require.NoError(t, err)
			require.NoError(t, json.Unmarshal(raw, &m))
			result := ingest(t, s, m)
			require.Nil(t, result.Turn, "未支持消息不能执行模型或发起个人授权")
			require.Nil(t, result.Stopped, "未支持消息不能停止当前任务")
			require.NotNil(t, result.Control)
			require.Equal(t, core.ControlKind("unsupported_message"), result.Control.Kind)
			require.Empty(t, result.Control.TargetTurnID)
			require.NotNil(t, result.Reply, "ACK 前必须已有持久回复")
			require.Contains(t, result.Reply.Body.Text, "暂不支持")
			require.Equal(t, "reply", result.Reply.ReplyTarget.Mode)
			require.Equal(t, m.MessageID, result.Reply.ReplyTarget.MessageID)
			again := ingest(t, s, m)
			require.True(t, again.Duplicate)
			require.Equal(t, result.Control.ID, again.Control.ID)
			m.ProviderEventID = "another-provider-event"
			require.True(t, ingest(t, s, m).Duplicate)
			delivery, err := s.LeaseDelivery(ctx, "sender", time.Minute)
			require.NoError(t, err)
			require.NotNil(t, delivery)
			require.Equal(t, result.Reply.ID, delivery.ID)
			require.NoError(t, s.CompleteDelivery(ctx, delivery.Lease, core.DeliveryResult{Status: core.DeliveryDone}))
			next, err := s.LeaseDelivery(ctx, "sender", time.Minute)
			require.NoError(t, err)
			require.Nil(t, next, "重推不能重复提示")
			turn, err := s.LeaseTurn(ctx, Claim("worker"))
			require.NoError(t, err)
			if active {
				require.NotNil(t, turn)
				require.Equal(t, running.ID, turn.Turn.ID)
				require.True(t, turn.Turn.StopRequestedAt.IsZero())
			} else {
				require.Nil(t, turn)
			}
		})
	}
}
