// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package conformance

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// RunIngestBudget 验证执行容量与持久拒绝在同一受理事务内完成。
func RunIngestBudget(t *testing.T, factory StoreFactory) {
	t.Run("ChannelCapacityRejectDedupeAndBudget", func(t *testing.T) {
		ctx := context.Background()
		clock := NewClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		s := factory(t, StoreConfig{Clock: clock.Now, MaxChannelOpenTurns: 1, MaxRejectedMessages: 2, RejectNoticeInterval: time.Minute})
		t.Cleanup(func() { require.NoError(t, s.Close()) })
		v := SampleVersion()
		first, err := s.Ingest(ctx, v, SampleMessage("first"))
		require.NoError(t, err)
		require.NotNil(t, first.Turn)
		rejected, err := s.Ingest(ctx, v, SampleMessage("reject"))
		require.NoError(t, err)
		require.True(t, rejected.Rejected)
		require.Nil(t, rejected.Turn)
		require.NotNil(t, rejected.Reply)
		duplicate, err := s.Ingest(ctx, v, SampleMessage("reject"))
		require.NoError(t, err)
		require.True(t, duplicate.Duplicate)
		require.True(t, duplicate.Rejected)
		require.Equal(t, rejected.Reply.ID, duplicate.Reply.ID)
		second, err := s.Ingest(ctx, v, SampleMessage("reject2"))
		require.NoError(t, err)
		require.True(t, second.Rejected)
		require.Equal(t, rejected.Reply.ID, second.Reply.ID)
		_, err = s.Ingest(ctx, v, SampleMessage("exhausted"))
		require.ErrorIs(t, err, core.ErrQueueFull)
		e, err := s.Export(ctx)
		require.NoError(t, err)
		require.Len(t, e.Turns, 1)
		require.Len(t, e.Messages, 3)
		require.Len(t, e.Controls, 1)
		require.Len(t, e.Deliveries, 1)
		removed, err := s.Prune(ctx, clock.Now().Add(time.Second))
		require.NoError(t, err)
		require.Zero(t, removed.Messages, "pending notice protects every dedupe reference")
		d, err := s.LeaseDelivery(ctx, "delivery", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, d)
		require.NoError(t, s.CompleteDelivery(ctx, d.Lease, core.DeliveryResult{Status: core.DeliveryDone}))
		_, err = s.Prune(ctx, clock.Now().Add(time.Second))
		require.NoError(t, err)
		next, err := s.Ingest(ctx, v, SampleMessage("capacity-released"))
		require.NoError(t, err)
		require.True(t, next.Rejected)
	})
	t.Run("ConcurrentSameRejectedMessage", func(t *testing.T) {
		s := factory(t, StoreConfig{MaxChannelOpenTurns: 1, MaxRejectedMessages: 64})
		t.Cleanup(func() { require.NoError(t, s.Close()) })
		_, err := s.Ingest(t.Context(), SampleVersion(), SampleMessage("open"))
		require.NoError(t, err)
		var wg sync.WaitGroup
		results := make(chan core.IngestResult, 8)
		errs := make(chan error, 8)
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				r, e := s.Ingest(t.Context(), SampleVersion(), SampleMessage("rejected"))
				results <- r
				errs <- e
			}()
		}
		wg.Wait()
		close(results)
		close(errs)
		for e := range errs {
			require.NoError(t, e)
		}
		id := ""
		for r := range results {
			require.True(t, r.Rejected)
			require.NotNil(t, r.Reply)
			if id == "" {
				id = r.Reply.ID
			}
			require.Equal(t, id, r.Reply.ID)
		}
		e, err := s.Export(t.Context())
		require.NoError(t, err)
		require.Len(t, e.Turns, 1)
		require.Len(t, e.Messages, 2)
		require.Len(t, e.Deliveries, 1)
	})
}

// IngestBudgetScope 验证容量身份跨 epoch 保持，而不同账号/Channel 独立。
func IngestBudgetScope(t *testing.T, factory StoreFactory) {
	t.Run("ChannelScopeAcrossEpochAndConfig", func(t *testing.T) {
		s := factory(t, StoreConfig{MaxChannelOpenTurns: 1, MaxRejectedMessages: 8})
		t.Cleanup(func() { require.NoError(t, s.Close()) })
		b := SampleBinding()
		b.Owner = "owner"
		b.Project = "project"
		b.ChannelID = "channel"
		b.BindingEpoch = 1
		b.ConfigVersion = 1
		m := SampleMessage("first")
		first, err := s.Ingest(t.Context(), core.NewBindingVersion(b), m)
		require.NoError(t, err)
		require.NotNil(t, first.Turn)
		b.AgentVersion = "updated" // 同一 ConfigVersion 的配置内容变化也不能重置消息去重
		again, err := s.Ingest(t.Context(), core.NewBindingVersion(b), m)
		require.NoError(t, err)
		require.True(t, again.Duplicate)
		require.False(t, again.Rejected)
		require.Equal(t, first.Turn.ID, again.Turn.ID)
		b.ConfigVersion = 2
		again, err = s.Ingest(t.Context(), core.NewBindingVersion(b), m)
		require.NoError(t, err)
		require.True(t, again.Duplicate)
		require.Equal(t, first.Turn.ID, again.Turn.ID)
		b.BindingEpoch = 2
		b.ID = "binding-epoch2"
		m = SampleMessage("epoch")
		m.BindingID = b.ID
		r, err := s.Ingest(t.Context(), core.NewBindingVersion(b), m)
		require.NoError(t, err)
		require.True(t, r.Rejected, "new epoch must not bypass Channel capacity")
		b.ChannelID = "other-channel"
		b.ID = "binding-other"
		m = SampleMessage("other")
		m.BindingID = b.ID
		r, err = s.Ingest(t.Context(), core.NewBindingVersion(b), m)
		require.NoError(t, err)
		require.False(t, r.Rejected)
		require.NotNil(t, r.Turn)
		b.Owner = "other-owner"
		b.ChannelID = "channel"
		b.ID = "binding-owner"
		m = SampleMessage("owner")
		m.BindingID = b.ID
		r, err = s.Ingest(t.Context(), core.NewBindingVersion(b), m)
		require.NoError(t, err)
		require.False(t, r.Rejected)
		require.NotNil(t, r.Turn)
	})
	t.Run("GlobalCapacityAndRejectedAliases", func(t *testing.T) {
		s := factory(t, StoreConfig{MaxOpenTurns: 1, MaxRejectedMessages: 2})
		t.Cleanup(func() { require.NoError(t, s.Close()) })
		_, err := s.Ingest(t.Context(), SampleVersion(), SampleMessage("open"))
		require.NoError(t, err)
		m := SampleMessage("reject")
		m.ChatID = "other-chat"
		r, err := s.Ingest(t.Context(), SampleVersion(), m)
		require.NoError(t, err)
		require.True(t, r.Rejected)
		m.ProviderEventID = "alias"
		r2, err := s.Ingest(t.Context(), SampleVersion(), m)
		require.NoError(t, err)
		require.True(t, r2.Duplicate)
		require.True(t, r2.Rejected)
		require.Equal(t, r.Reply.ID, r2.Reply.ID)
		m.ProviderEventID = "alias-over-budget"
		_, err = s.Ingest(t.Context(), SampleVersion(), m)
		require.ErrorIs(t, err, core.ErrQueueFull)
		m.ProviderEventID = "alias"
		r2, err = s.Ingest(t.Context(), SampleVersion(), m)
		require.NoError(t, err)
		require.Equal(t, r.Reply.ID, r2.Reply.ID)
		e, err := s.Export(t.Context())
		require.NoError(t, err)
		require.Len(t, e.Messages, 2)
		require.Len(t, e.Turns, 1)
		require.Len(t, e.Deliveries, 1)
	})
	t.Run("BudgetRollbackNewNoticeAndFailedNoticeRetention", func(t *testing.T) {
		clock := NewClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		s := factory(t, StoreConfig{Clock: clock.Now, MaxOpenTurns: 1, MaxRejectedMessages: 1})
		t.Cleanup(func() { require.NoError(t, s.Close()) })
		_, err := s.Ingest(t.Context(), SampleVersion(), SampleMessage("open"))
		require.NoError(t, err)
		_, err = s.Ingest(t.Context(), SampleVersion(), SampleMessage("reject"))
		require.NoError(t, err)
		clock.Advance(2 * time.Minute)
		_, err = s.Ingest(t.Context(), SampleVersion(), SampleMessage("full"))
		require.ErrorIs(t, err, core.ErrQueueFull)
		e, err := s.Export(t.Context())
		require.NoError(t, err)
		require.Len(t, e.Messages, 2)
		require.Len(t, e.Controls, 1)
		require.Len(t, e.Deliveries, 1)
		d, err := s.LeaseDelivery(t.Context(), "deliver", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, d)
		require.NoError(t, s.CompleteDelivery(t.Context(), d.Lease, core.DeliveryResult{Status: core.DeliveryFailed, ErrorKind: "failed"}))
		p, err := s.Prune(t.Context(), clock.Now())
		require.NoError(t, err)
		require.Zero(t, p.Messages, "undelivered diagnostic must retain its source and dedupe facts")
	})
}

// IngestBudgetConcurrentChannels 验证独立 Channel 并发无法突破全局执行/拒绝预算。
func IngestBudgetConcurrentChannels(t *testing.T, factory StoreFactory) {
	t.Run("ConcurrentIndependentChannelsRespectGlobalCapacity", func(t *testing.T) {
		s := factory(t, StoreConfig{MaxOpenTurns: 3, MaxRejectedMessages: 32})
		t.Cleanup(func() { require.NoError(t, s.Close()) })
		var wg sync.WaitGroup
		errs := make(chan error, 12)
		for i := 0; i < 12; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				id := fmt.Sprintf("channel-%d", i)
				b := SampleBinding()
				b.ID = id
				b.ChannelID = id
				b.Owner = id
				m := SampleMessage(id)
				m.BindingID = id
				_, err := s.Ingest(t.Context(), core.NewBindingVersion(b), m)
				errs <- err
			}(i)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
		e, err := s.Export(t.Context())
		require.NoError(t, err)
		require.Len(t, e.Turns, 3)
		require.Len(t, e.Messages, 12)
		require.Len(t, e.Deliveries, 9)
	})
	t.Run("GlobalCapacityReleasedOnlyOnSettlement", func(t *testing.T) {
		s := factory(t, StoreConfig{MaxOpenTurns: 1})
		t.Cleanup(func() { require.NoError(t, s.Close()) })
		_, err := s.Ingest(t.Context(), SampleVersion(), SampleMessage("first"))
		require.NoError(t, err)
		_, err = s.Ingest(t.Context(), SampleVersion(), SampleMessage("full"))
		require.ErrorIs(t, err, core.ErrQueueFull)
		lease, err := s.LeaseTurn(t.Context(), Claim("worker"))
		require.NoError(t, err)
		require.NotNil(t, lease)
		require.NoError(t, s.SettleTurn(t.Context(), lease.Turn.Lease, core.TurnOutcome{Status: core.TurnFailed, ErrorKind: "test"}, nil))
		next, err := s.Ingest(t.Context(), SampleVersion(), SampleMessage("next"))
		require.NoError(t, err)
		require.NotNil(t, next.Turn)
	})
}

// RunIngestBudgetRestart 验证持久拒绝在重启后保持原有诊断与预算。
func RunIngestBudgetRestart(t *testing.T, open func() TestStore) {
	s := open()
	_, err := s.Ingest(t.Context(), SampleVersion(), SampleMessage("open"))
	require.NoError(t, err)
	r, err := s.Ingest(t.Context(), SampleVersion(), SampleMessage("reject"))
	require.NoError(t, err)
	require.True(t, r.Rejected)
	id := r.Reply.ID
	require.NoError(t, s.Close())
	s = open()
	defer func() { require.NoError(t, s.Close()) }()
	r, err = s.Ingest(t.Context(), SampleVersion(), SampleMessage("reject"))
	require.NoError(t, err)
	require.True(t, r.Duplicate)
	require.True(t, r.Rejected)
	require.Equal(t, id, r.Reply.ID)
	_, err = s.Ingest(t.Context(), SampleVersion(), SampleMessage("full"))
	require.ErrorIs(t, err, core.ErrQueueFull)
	d, err := s.LeaseDelivery(t.Context(), "restart", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, d)
	require.Equal(t, id, d.ID)
	e, err := s.Export(t.Context())
	require.NoError(t, err)
	require.Len(t, e.Turns, 1)
	require.Len(t, e.Messages, 2)
}

// IngestBudgetConcurrentRejects 验证同 Channel 容量与跨 Channel 拒绝槽位均无并发超调。
func IngestBudgetConcurrentRejects(t *testing.T, factory StoreFactory) {
	t.Run("ConcurrentSingleChannelCapacity", func(t *testing.T) {
		s := factory(t, StoreConfig{MaxChannelOpenTurns: 1, MaxRejectedMessages: 16})
		t.Cleanup(func() { require.NoError(t, s.Close()) })
		var wg sync.WaitGroup
		errs := make(chan error, 12)
		for i := 0; i < 12; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, err := s.Ingest(t.Context(), SampleVersion(), SampleMessage(fmt.Sprint(i)))
				errs <- err
			}(i)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
		e, err := s.Export(t.Context())
		require.NoError(t, err)
		require.Len(t, e.Turns, 1)
		require.Len(t, e.Messages, 12)
		require.Len(t, e.Deliveries, 1)
	})
	t.Run("ConcurrentRejectedBudgetCannotOverrun", func(t *testing.T) {
		s := factory(t, StoreConfig{MaxOpenTurns: 1, MaxRejectedMessages: 3})
		t.Cleanup(func() { require.NoError(t, s.Close()) })
		_, err := s.Ingest(t.Context(), SampleVersion(), SampleMessage("open"))
		require.NoError(t, err)
		var wg sync.WaitGroup
		errs := make(chan error, 12)
		for i := 0; i < 12; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				id := fmt.Sprintf("reject-%d", i)
				b := SampleBinding()
				b.ID = id
				b.Owner = id
				b.ChannelID = id
				m := SampleMessage(id)
				m.BindingID = id
				_, err := s.Ingest(t.Context(), core.NewBindingVersion(b), m)
				errs <- err
			}(i)
		}
		wg.Wait()
		close(errs)
		accepted := 0
		for err := range errs {
			if err == nil {
				accepted++
			} else {
				require.ErrorIs(t, err, core.ErrQueueFull)
			}
		}
		require.Equal(t, 3, accepted)
		e, err := s.Export(t.Context())
		require.NoError(t, err)
		require.Len(t, e.Turns, 1)
		require.Len(t, e.Messages, 4)
		require.Len(t, e.Deliveries, 3)
	})
}
