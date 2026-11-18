// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

//go:build mysqlintegration

package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

var mysqlTestCounter atomic.Uint64

func mysqlTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("CHANNEL_TEST_MYSQL_DSN")
	require.NotEmpty(t, dsn, "mysqlintegration requires an isolated CHANNEL_TEST_MYSQL_DSN")
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, db.PingContext(ctx))
	return db
}

func TestMySQLStoreConformance(t *testing.T) {
	conformance.RunStore(t, func(t *testing.T, c conformance.StoreConfig) conformance.TestStore {
		prefix := fmt.Sprintf("it%d_%d_", time.Now().UnixNano(), mysqlTestCounter.Add(1))
		s, err := openInstalledSQLFixture(context.Background(), mysqlTestDB(t), MySQL, Options{TablePrefix: prefix, Clock: c.Clock, MaxOpenTurns: c.MaxOpenTurns, MaxMessageBytes: c.MaxMessageBytes, QueueTTL: c.QueueTTL})
		require.NoError(t, err)
		return s
	})
}

func TestMySQLBotVaultConformance(t *testing.T) {
	conformance.RunBotVaultStore(t, func(t *testing.T, c conformance.StoreConfig) conformance.TestStore {
		prefix := fmt.Sprintf("bv%d_%d_", time.Now().UnixNano(), mysqlTestCounter.Add(1))
		s, err := openInstalledSQLFixture(context.Background(), mysqlTestDB(t), MySQL, Options{TablePrefix: prefix, Clock: c.Clock})
		require.NoError(t, err)
		return s
	})
}

func TestMySQLConcurrentClaims(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		t.Run(fmt.Sprintf("cas_%v", fallback), func(t *testing.T) {
			ctx := context.Background()
			prefix := fmt.Sprintf("cc%d_", time.Now().UnixNano())
			dialect := MySQL
			if fallback {
				dialect.SkipLockedSuffix = ""
			}
			a, err := openInstalledSQLFixture(ctx, mysqlTestDB(t), dialect, Options{TablePrefix: prefix})
			require.NoError(t, err)
			defer func() { require.NoError(t, a.Close()) }()
			b, err := openInstalledSQLFixture(ctx, mysqlTestDB(t), dialect, Options{TablePrefix: prefix})
			require.NoError(t, err)
			defer func() { require.NoError(t, b.Close()) }()
			const count = 12
			for i := 0; i < count; i++ {
				msg := conformance.SampleMessage(fmt.Sprint(i))
				msg.ChatID = msg.MessageID
				_, err = a.Ingest(ctx, conformance.SampleVersion(), msg)
				require.NoError(t, err)
			}
			var wg sync.WaitGroup
			var mu sync.Mutex
			seen := map[string]bool{}
			for i := 0; i < 24; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					st := a
					if i%2 == 1 {
						st = b
					}
					row, e := st.LeaseTurn(ctx, conformance.Claim(fmt.Sprint(i)))
					mu.Lock()
					defer mu.Unlock()
					require.NoError(t, e)
					if row != nil {
						require.False(t, seen[row.Turn.ID], "duplicate lease")
						seen[row.Turn.ID] = true
					}
				}(i)
			}
			wg.Wait()
			// 有限候选竞争可以让一次领取返回空；再次扫描必须收敛，不能丢会话。
			for i := 0; i < count; i++ {
				row, e := a.LeaseTurn(ctx, conformance.Claim("drain"))
				require.NoError(t, e)
				if row == nil {
					break
				}
				require.False(t, seen[row.Turn.ID])
				seen[row.Turn.ID] = true
			}
			require.Len(t, seen, count)
		})
	}
}

func TestMySQLLockedAccountDoesNotBlockOtherAccount(t *testing.T) {
	ctx := context.Background()
	prefix := fmt.Sprintf("qa%d_", time.Now().UnixNano())
	db := mysqlTestDB(t)
	defer func() { require.NoError(t, db.Close()) }()
	s, err := openInstalledSQLFixture(ctx, mysqlTestDB(t), MySQL, Options{TablePrefix: prefix})
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	opts := conformance.Claim("worker")
	opts.AccountLimit = 1
	for _, owner := range []string{"a", "b"} {
		binding := conformance.SampleBinding()
		binding.Owner = owner
		msg := conformance.SampleMessage(owner)
		msg.ChatID = owner
		_, err := s.Ingest(ctx, core.NewBindingVersion(binding), msg)
		require.NoError(t, err)
	}
	first, err := s.LeaseTurn(ctx, opts)
	require.NoError(t, err)
	require.NotNil(t, first)
	require.NoError(t, s.ReleaseTurn(ctx, first.Turn.Lease))
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, tx.Rollback()) }()
	_, err = tx.ExecContext(ctx, "UPDATE "+prefix+"channel_account_quotas SET open_count=open_count WHERE owner='a'")
	require.NoError(t, err)
	bounded, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	second, err := s.LeaseTurn(bounded, opts)
	require.NoError(t, err)
	require.NotNil(t, second)
	require.Equal(t, "a", second.Binding.Content.Owner, "observer claim does not take remote account lock")
}

func TestMySQLConcurrentAccountQuota(t *testing.T) {
	ctx := context.Background()
	prefix := fmt.Sprintf("cq%d_", time.Now().UnixNano())
	db := mysqlTestDB(t)
	defer func() { require.NoError(t, db.Close()) }()
	s, err := openInstalledSQLFixture(ctx, mysqlTestDB(t), MySQL, Options{TablePrefix: prefix})
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	for i := 0; i < 8; i++ {
		binding := conformance.SampleBinding()
		binding.Owner = "account"
		msg := conformance.SampleMessage(fmt.Sprint(i))
		msg.ChatID = msg.MessageID
		_, err = s.Ingest(ctx, core.NewBindingVersion(binding), msg)
		require.NoError(t, err)
	}
	var wg sync.WaitGroup
	var claims atomic.Int64
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			opts := conformance.Claim(fmt.Sprint(i))
			opts.AccountLimit = 2
			row, e := s.LeaseTurn(ctx, opts)
			if e != nil {
				t.Error(e)
				return
			}
			if row != nil {
				_, e = s.UpdateTurn(ctx, row.Turn.Lease, core.TurnUpdate{Phase: core.PhaseSend, Session: core.TurnSession{Generation: 1, SessionID: "sesn-" + row.Turn.ID}, Submit: core.Submission{WriteAttempts: 1, SubmittedAt: time.Now()}, RemoteIntent: &core.RemoteIntent{Kind: core.RemoteInput, Limit: 2}})
				if e == nil {
					claims.Add(1)
				} else if !errors.Is(e, core.ErrRemoteLimit) {
					t.Error(e)
				}
			}
		}(i)
	}
	wg.Wait()
	require.Equal(t, int64(2), claims.Load())
	var count int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+prefix+"channel_remote_operations WHERE owner='account' AND resolved=0").Scan(&count))
	require.Equal(t, 2, count)
}

func TestMySQLLeaseUsesDatabaseClock(t *testing.T) {
	ctx := context.Background()
	db := mysqlTestDB(t)
	db.SetMaxOpenConns(1)
	prefix := fmt.Sprintf("tm%d_", time.Now().UnixNano())
	s, err := openInstalledSQLFixture(ctx, db, MySQL, Options{TablePrefix: prefix})
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	_, err = db.ExecContext(ctx, "SET timestamp=2000000000")
	require.NoError(t, err)
	_, err = s.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("clock"))
	require.NoError(t, err)
	opts := conformance.Claim("worker")
	row, err := s.LeaseTurn(ctx, opts)
	require.NoError(t, err)
	require.NotNil(t, row)
	require.WithinDuration(t, time.Unix(2000000000, 0).Add(opts.TTL), row.Turn.Lease.ExpiresAt, time.Second)
}

func TestMySQLConversationLockDoesNotBlockCandidates(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		t.Run(fmt.Sprint(fallback), func(t *testing.T) {
			ctx := context.Background()
			prefix := fmt.Sprintf("cl%d_", time.Now().UnixNano())
			db := mysqlTestDB(t)
			defer func() { require.NoError(t, db.Close()) }()
			dialect := MySQL
			if fallback {
				dialect.SkipLockedSuffix = ""
			}
			s, err := openInstalledSQLFixture(ctx, mysqlTestDB(t), dialect, Options{TablePrefix: prefix})
			require.NoError(t, err)
			defer func() { require.NoError(t, s.Close()) }()
			var first core.RouteKey
			for _, id := range []string{"a", "b"} {
				msg := conformance.SampleMessage(id)
				msg.ChatID = id
				r, e := s.Ingest(ctx, conformance.SampleVersion(), msg)
				require.NoError(t, e)
				if id == "a" {
					first = r.Turn.ConversationID
				}
			}
			tx, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer func() { require.NoError(t, tx.Rollback()) }()
			_, err = tx.ExecContext(ctx, "UPDATE "+prefix+"channel_conversations SET revision=revision WHERE id=?", string(first))
			require.NoError(t, err)
			bounded, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			row, err := s.LeaseTurn(bounded, conformance.Claim("worker"))
			require.NoError(t, err)
			require.NotNil(t, row)
			if !fallback {
				require.NotEqual(t, first, row.Turn.ConversationID)
			}
		})
	}
}
