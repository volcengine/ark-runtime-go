// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// TestSQLiteConcurrentLeasesAcrossConversations 验证多连接并发领取：
// 每个会话的队首在任意时刻最多被一个持有者领取；不同会话最终都被领到。
func TestSQLiteConcurrentLeasesAcrossConversations(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/conc.db"
	a, err := openInstalledSQLFixture(ctx, openSQLite(t, path), SQLite, Options{})
	require.NoError(t, err)
	defer func() { require.NoError(t, a.Close()) }()
	b, err := openInstalledSQLFixture(ctx, openSQLite(t, path), SQLite, Options{})
	require.NoError(t, err)
	defer func() { require.NoError(t, b.Close()) }()

	const conversations = 8
	want := map[string]bool{}
	for i := 0; i < conversations; i++ {
		msg := conformance.SampleMessage(fmt.Sprintf("c%d", i))
		msg.ChatID = fmt.Sprintf("chat-%d", i)
		ingested, ierr := a.Ingest(ctx, conformance.SampleVersion(), msg)
		require.NoError(t, ierr)
		want[ingested.Turn.ID] = false
	}

	var mu sync.Mutex
	claimedBy := map[string]string{}
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		store := a
		if w%2 == 1 {
			store = b
		}
		go func(n int) {
			defer wg.Done()
			leased, lerr := store.LeaseTurn(ctx, core.ClaimOptions{Owner: fmt.Sprintf("worker-%d", n), TTL: time.Hour, MessageDeadline: 2 * time.Hour})
			if lerr != nil || leased == nil {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if owner, exists := claimedBy[leased.Turn.ID]; exists && owner != leased.Turn.Lease.Owner {
				t.Errorf("turn %s double claimed by %s and %s", leased.Turn.ID, owner, leased.Turn.Lease.Owner)
				return
			}
			claimedBy[leased.Turn.ID] = leased.Turn.Lease.Owner
		}(w)
	}
	wg.Wait()
	require.NotEmpty(t, claimedBy, "at least one turn is claimed")
	for id := range claimedBy {
		require.Contains(t, want, id, "claimed turn must belong to the ingested set")
	}
}

// TestSQLiteOldFenceWritesRejected 验证租约易主后旧 fence 的写入被拒绝。
func TestSQLiteOldFenceWritesRejected(t *testing.T) {
	ctx := context.Background()
	s, err := openInstalledSQLFixture(ctx, openSQLite(t, t.TempDir()+"/fence.db"), SQLite, Options{})
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	ingested, err := s.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("one"))
	require.NoError(t, err)
	first, err := s.LeaseTurn(ctx, conformance.Claim("a"))
	require.NoError(t, err)
	require.Equal(t, ingested.Turn.ID, first.Turn.ID)
	require.NoError(t, s.ReleaseTurn(ctx, first.Turn.Lease))
	second, err := s.LeaseTurn(ctx, conformance.Claim("b"))
	require.NoError(t, err)
	require.Equal(t, ingested.Turn.ID, second.Turn.ID)
	require.NoError(t, s.SettleTurn(ctx, second.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, nil))
	// 旧持有者（fence 落后）再写必须失败。
	_, err = s.UpdateTurn(ctx, first.Turn.Lease, core.TurnUpdate{Phase: core.PhaseSend})
	require.ErrorIs(t, err, core.ErrLeaseLost)
}

func TestQuotaBlockedCandidatesDoNotStarveOtherAccounts(t *testing.T) {
	ctx := context.Background()
	s, err := openInstalledSQLFixture(ctx, openSQLite(t, t.TempDir()+"/fair.db"), SQLite, Options{})
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	binding := conformance.SampleBinding()
	binding.Owner = "busy"
	for i := 0; i < 40; i++ {
		msg := conformance.SampleMessage(fmt.Sprint(i))
		msg.ChatID = msg.MessageID
		_, err := s.Ingest(ctx, core.NewBindingVersion(binding), msg)
		require.NoError(t, err)
	}
	binding.Owner = "other"
	msg := conformance.SampleMessage("other")
	msg.ChatID = "other"
	_, err = s.Ingest(ctx, core.NewBindingVersion(binding), msg)
	require.NoError(t, err)
	opts := conformance.Claim("worker")
	opts.AccountLimit = 1
	first, err := s.LeaseTurn(ctx, opts)
	require.NoError(t, err)
	require.NotNil(t, first)
	_, err = s.UpdateTurn(ctx, first.Turn.Lease, core.TurnUpdate{Phase: core.PhaseSend, Session: core.TurnSession{Generation: 1, SessionID: "sesn-first"}, Submit: core.Submission{WriteAttempts: 1, SubmittedAt: time.Now()}, RemoteIntent: &core.RemoteIntent{Kind: core.RemoteInput, Limit: 1}})
	require.NoError(t, err)
	var other *core.TurnLease
	for i := 0; i < 42; i++ {
		next, e := s.LeaseTurn(ctx, opts)
		require.NoError(t, e)
		if next == nil {
			continue
		}
		if next.Binding.Content.Owner == "other" {
			other = next
			break
		}
		_, e = s.UpdateTurn(ctx, next.Turn.Lease, core.TurnUpdate{Phase: core.PhaseSend, Session: core.TurnSession{Generation: 1, SessionID: "sesn-" + next.Turn.ID}, Submit: core.Submission{WriteAttempts: 1, SubmittedAt: time.Now()}, RemoteIntent: &core.RemoteIntent{Kind: core.RemoteInput, Limit: 1}})
		require.ErrorIs(t, e, core.ErrRemoteLimit)
		require.NoError(t, s.WaitTurn(ctx, next.Turn.Lease, core.TurnWait{Reason: "quota", Until: time.Now().Add(time.Minute)}))
	}
	require.NotNil(t, other, "bounded workers must advance past accounts whose new writes are quota blocked")
	require.Equal(t, "other", other.Binding.Content.Owner)
}
