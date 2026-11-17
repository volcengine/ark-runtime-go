// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

//go:build mysqlintegration

package sqlstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// claimWindowConnector 仅拦截测试事务的current-read边界，其余均交给真实MySQL driver。
type claimWindowConnector struct {
	driver.Connector
	beforeQuery func(string)
	beforeExec  func(string)
}

func (c claimWindowConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return claimWindowConn{Conn: conn, beforeQuery: c.beforeQuery, beforeExec: c.beforeExec}, nil
}

type claimWindowConn struct {
	driver.Conn
	beforeQuery func(string)
	beforeExec  func(string)
}

func (c claimWindowConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
}
func (c claimWindowConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.beforeQuery != nil {
		c.beforeQuery(query)
	}
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}
func (c claimWindowConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.beforeExec != nil {
		c.beforeExec(query)
	}
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}

// readsClaimWindowTurn 识别候选已选出后的实体读取，不依赖新增 metadata 投影列或空格布局。
func readsClaimWindowTurn(query, table string) bool {
	normalized := strings.ToLower(strings.Join(strings.Fields(query), " "))
	projection, predicate, found := strings.Cut(normalized, " from "+strings.ToLower(table)+" where ")
	if !found || !strings.HasPrefix(projection, "select ") {
		return false
	}
	readsData := false
	for _, column := range strings.Split(strings.TrimPrefix(projection, "select "), ",") {
		if strings.TrimSpace(column) == "data" {
			readsData = true
			break
		}
	}
	if !readsData {
		return false
	}
	for _, clause := range strings.Split(predicate, " and ") {
		compact := strings.ReplaceAll(clause, " ", "")
		if compact == "id=?" || compact == "id=?forupdate" {
			return true
		}
	}
	return false
}

func TestMySQLCASRechecksFreshTurnAfterCandidateSelection(t *testing.T) {
	for _, tc := range []struct {
		name    string
		expired bool
		wait    bool
		stop    bool
		release bool
		claim   bool
	}{
		{name: "live lease"},
		{name: "future wait", wait: true},
		{name: "stop without live lease", wait: true, stop: true, claim: true},
		{name: "expired tail with live lease", expired: true},
		{name: "expired tail released", expired: true, release: true, claim: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			clock := conformance.NewClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
			prefix := fmt.Sprintf("cw%d_", time.Now().UnixNano())
			dialect := MySQL
			dialect.SkipLockedSuffix = ""
			options := Options{TablePrefix: prefix, Clock: clock.Now}
			if tc.expired {
				options.QueueTTL = time.Second
			}
			other, err := openInstalledSQLFixture(ctx, mysqlTestDB(t), dialect, options)
			require.NoError(t, err)
			defer func() { require.NoError(t, other.Close()) }()
			if tc.expired {
				// 让队首保持有效且已开始执行，受测消息必须是真正的非队首过期tail。
				_, err = other.Ingest(ctx, conformance.SampleVersion(), conformance.P2PMessage("head"))
				require.NoError(t, err)
				head, headErr := other.LeaseTurn(ctx, conformance.Claim("head"))
				require.NoError(t, headErr)
				require.NotNil(t, head)
				_, headErr = other.UpdateTurn(ctx, head.Turn.Lease, core.TurnUpdate{Phase: core.PhaseCreate, Session: core.TurnSession{Generation: 1, CreationKey: "head"}})
				require.NoError(t, headErr)
			}
			accepted, err := other.Ingest(ctx, conformance.SampleVersion(), conformance.P2PMessage("window"))
			require.NoError(t, err)
			if tc.expired {
				clock.Advance(2 * time.Second)
			}
			config, err := mysql.ParseDSN(os.Getenv("CHANNEL_TEST_MYSQL_DSN"))
			require.NoError(t, err)
			connector, err := mysql.NewConnector(config)
			require.NoError(t, err)
			var once sync.Once
			var winner *core.TurnLease
			intercepted := sql.OpenDB(claimWindowConnector{Connector: connector, beforeQuery: func(query string) {
				if !readsClaimWindowTurn(query, prefix+"channel_turns") {
					return
				}
				once.Do(func() {
					winner, err = other.LeaseTurn(ctx, conformance.Claim("winner"))
					require.NoError(t, err)
					require.NotNil(t, winner)
					require.Equal(t, accepted.Turn.ID, winner.Turn.ID)
					if tc.expired {
						require.True(t, winner.Turn.Schedule.StartedAt.IsZero())
						require.True(t, winner.Turn.Schedule.DeadlineAt.IsZero())
					}
					if tc.wait {
						require.NoError(t, other.WaitTurn(ctx, winner.Turn.Lease, core.TurnWait{Reason: "retry", Until: clock.Now().Add(time.Hour)}))
					}
					if tc.stop {
						_, stopErr := other.Ingest(ctx, conformance.SampleVersion(), conformance.StopMessage("stop"))
						require.NoError(t, stopErr)
					}
					if tc.release {
						require.NoError(t, other.ReleaseTurn(ctx, winner.Turn.Lease))
					}
				})
			}})
			first, err := openInstalledSQLFixture(ctx, intercepted, dialect, options)
			require.NoError(t, err)
			defer func() { require.NoError(t, first.Close()) }()
			result, err := first.LeaseTurn(ctx, conformance.Claim("stale candidate"))
			require.NoError(t, err)
			require.NotNil(t, winner, "hook must run after candidate selection")
			if tc.claim {
				require.NotNil(t, result)
				require.Greater(t, result.Turn.Lease.Fence, winner.Turn.Lease.Fence)
				if tc.expired {
					require.True(t, result.Turn.Schedule.StartedAt.IsZero())
					require.True(t, result.Turn.Schedule.DeadlineAt.IsZero())
				}
			} else {
				require.Nil(t, result, "a stale candidate must not overwrite the fresh lease or retry schedule")
				persisted, getErr := other.GetTurn(ctx, accepted.Turn.ID)
				require.NoError(t, getErr)
				require.Equal(t, winner.Turn.Lease.Fence, persisted.Lease.Fence)
			}
		})
	}
}

func TestMySQLDeliveryRenewCannotResurrectCompletedHead(t *testing.T) {
	ctx := context.Background()
	prefix := fmt.Sprintf("dw%d_", time.Now().UnixNano())
	options := Options{TablePrefix: prefix}
	other, err := openInstalledSQLFixture(ctx, mysqlTestDB(t), MySQL, options)
	require.NoError(t, err)
	defer func() { require.NoError(t, other.Close()) }()
	accepted, err := other.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("delivery"))
	require.NoError(t, err)
	turn, err := other.LeaseTurn(ctx, conformance.Claim("turn"))
	require.NoError(t, err)
	require.NotNil(t, turn)
	source := core.DeliverySource{Kind: core.SourceTurn, ID: accepted.Turn.ID}
	firstDelivery := core.NewDelivery(source, 0, "first", "binding", core.ReplyTarget{Mode: "reply", ChatID: "chat"}, core.TextBody("first"))
	nextDelivery := core.NewDelivery(source, 1, "second", "binding", core.ReplyTarget{Mode: "reply", ChatID: "chat"}, core.TextBody("second"))
	require.NoError(t, other.SettleTurn(ctx, turn.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, []core.Delivery{firstDelivery, nextDelivery}))
	leased, err := other.LeaseDelivery(ctx, "delivery", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, leased)
	config, err := mysql.ParseDSN(os.Getenv("CHANNEL_TEST_MYSQL_DSN"))
	require.NoError(t, err)
	connector, err := mysql.NewConnector(config)
	require.NoError(t, err)
	completed := make(chan error, 1)
	var once sync.Once
	intercepted := sql.OpenDB(claimWindowConnector{Connector: connector, beforeExec: func(query string) {
		if !strings.HasPrefix(query, "UPDATE "+prefix+"channel_deliveries SET status =") {
			return
		}
		once.Do(func() {
			go func() {
				completed <- other.CompleteDelivery(ctx, leased.Lease, core.DeliveryResult{Status: core.DeliveryDone, RemoteMessageID: "completed"})
			}()
			// 无行锁时Complete可以先提交；有行锁时它必须等待Renew提交后继续。
			select {
			case completionErr := <-completed:
				completed <- completionErr
			case <-time.After(100 * time.Millisecond):
			}
		})
	}})
	first, err := openInstalledSQLFixture(ctx, intercepted, MySQL, options)
	require.NoError(t, err)
	defer func() { require.NoError(t, first.Close()) }()
	_, err = first.RenewDelivery(ctx, leased.Lease, time.Minute)
	require.NoError(t, err)
	select {
	case completionErr := <-completed:
		require.NoError(t, completionErr)
	case <-time.After(time.Second):
		t.Fatal("completion never obtained the delivery row after renewal")
	}
	deliveries, err := other.ListDeliveries(ctx, core.DeliveryQuery{})
	require.NoError(t, err)
	for _, delivery := range deliveries {
		if delivery.ID == leased.ID {
			require.Equal(t, core.DeliveryDone, delivery.Status, "renewal must not resurrect a completed delivery")
			require.Equal(t, "completed", delivery.RemoteMessageID)
		}
	}
	next, err := other.LeaseDelivery(ctx, "next", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, next)
	require.Equal(t, nextDelivery.ID, next.ID)
}

func TestMySQLTurnRenewDoesNotInvalidateSameFenceMutation(t *testing.T) {
	for _, action := range []string{"renew", "update", "wait", "settle", "stop"} {
		t.Run(action, func(t *testing.T) {
			ctx := context.Background()
			prefix := fmt.Sprintf("rw%d_", time.Now().UnixNano())
			options := Options{TablePrefix: prefix}
			other, err := openInstalledSQLFixture(ctx, mysqlTestDB(t), MySQL, options)
			require.NoError(t, err)
			defer func() { require.NoError(t, other.Close()) }()
			_, err = other.Ingest(ctx, conformance.SampleVersion(), conformance.P2PMessage("renew"))
			require.NoError(t, err)
			leased, err := other.LeaseTurn(ctx, conformance.Claim("holder"))
			require.NoError(t, err)
			require.NotNil(t, leased)
			config, err := mysql.ParseDSN(os.Getenv("CHANNEL_TEST_MYSQL_DSN"))
			require.NoError(t, err)
			connector, err := mysql.NewConnector(config)
			require.NoError(t, err)
			renewed := make(chan error, 1)
			var once sync.Once
			intercepted := sql.OpenDB(claimWindowConnector{Connector: connector, beforeExec: func(query string) {
				if !strings.HasPrefix(query, "UPDATE "+prefix+"channel_turns SET ") {
					return
				}
				once.Do(func() {
					go func() { _, renewErr := other.RenewTurn(ctx, leased.Turn.Lease, time.Minute); renewed <- renewErr }()
					select {
					case renewErr := <-renewed:
						renewed <- renewErr
					case <-time.After(100 * time.Millisecond):
					}
				})
			}})
			first, err := openInstalledSQLFixture(ctx, intercepted, MySQL, options)
			require.NoError(t, err)
			defer func() { require.NoError(t, first.Close()) }()
			switch action {
			case "renew":
				_, err = first.RenewTurn(ctx, leased.Turn.Lease, time.Minute)
			case "update":
				_, err = first.UpdateTurn(ctx, leased.Turn.Lease, core.TurnUpdate{Phase: core.PhaseCreate, Session: core.TurnSession{Generation: 1, CreationKey: "frozen"}})
			case "wait":
				err = first.WaitTurn(ctx, leased.Turn.Lease, core.TurnWait{Reason: "retry", Until: time.Now().Add(time.Minute)})
			case "settle":
				err = first.SettleTurn(ctx, leased.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, nil)
			case "stop":
				_, err = first.Ingest(ctx, conformance.SampleVersion(), conformance.StopMessage("stop"))
			}
			require.NoError(t, err, "a concurrent renewal cannot invalidate the same live fence")
			select {
			case renewErr := <-renewed:
				if action == "wait" || action == "settle" {
					require.ErrorIs(t, renewErr, core.ErrLeaseLost)
				} else {
					require.NoError(t, renewErr)
				}
			case <-time.After(time.Second):
				t.Fatal("renewal remained blocked after mutation commit")
			}
			persisted, err := other.GetTurn(ctx, leased.Turn.ID)
			require.NoError(t, err)
			require.Equal(t, leased.Turn.Lease.Fence, persisted.Lease.Fence)
			switch action {
			case "update":
				require.Equal(t, core.PhaseCreate, persisted.Phase)
			case "wait":
				require.Equal(t, core.TurnWaiting, persisted.Status)
			case "settle":
				require.Equal(t, core.TurnDone, persisted.Status)
			case "stop":
				require.False(t, persisted.StopRequestedAt.IsZero(), "accepted stop must be persisted")
			}
		})
	}
}
