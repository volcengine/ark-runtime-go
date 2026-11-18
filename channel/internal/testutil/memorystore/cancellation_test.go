// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package memory_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	memory "github.com/volcengine/ark-runtime-go/channel/internal/testutil/memorystore"
)

func TestCanceledReadsDoNotBlockSubsequentWrites(t *testing.T) {
	s, err := memory.New(memory.Options{PayloadCodec: memoryPayloadCodec{}, PayloadNamespace: "cancel", RequirePayloadEncryption: true})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	input, err := s.Ingest(t.Context(), conformance.SampleVersion(), conformance.SampleMessage("retained"))
	require.NoError(t, err)
	for i := range 1000 {
		// 在 SQL 查询和密文引用读取的不同位置取消，验证连接替换后没有遗留读锁。
		ctx, cancel := context.WithTimeout(t.Context(), time.Duration(i%50+1)*time.Microsecond)
		_, err = s.GetTurn(ctx, input.Turn.ID)
		cancel()
		if err != nil {
			require.True(t, errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded), "unexpected read error: %v", err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	leased, err := s.LeaseTurn(ctx, conformance.Claim("after-cancel"))
	require.NoError(t, err)
	require.NotNil(t, leased)
	require.Equal(t, input.Turn.ID, leased.Turn.ID)
	require.Equal(t, core.TurnRunning, leased.Turn.Status)
	require.Equal(t, "text retained", leased.Message.Message.Text)
}
