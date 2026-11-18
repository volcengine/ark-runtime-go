// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"modernc.org/sqlite"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// pruneReadBarrier 只在真实 SQLite 已释放候选 SELECT 后暂停，第二连接仍执行真实 RetryDelivery。
type pruneReadBarrier struct {
	source      string
	captured    chan struct{}
	resume      chan struct{}
	once        sync.Once
	releaseOnce sync.Once
}

func (b *pruneReadBarrier) release() { b.releaseOnce.Do(func() { close(b.resume) }) }
func (b *pruneReadBarrier) candidate(query string) bool {
	if b.source == "turn" {
		return strings.Contains(query, "FROM channel_turns WHERE id>") && strings.Contains(query, "settled_at>0")
	}
	return strings.Contains(query, "FROM channel_controls WHERE id>") && strings.Contains(query, "source_kind='control'")
}

type pruneBarrierConnector struct {
	dsn     string
	barrier *pruneReadBarrier
}

func (c pruneBarrierConnector) Driver() driver.Driver { return &sqlite.Driver{} }
func (c pruneBarrierConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.Driver().Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return pruneBarrierConn{Conn: conn, barrier: c.barrier}, nil
}

type pruneBarrierConn struct {
	driver.Conn
	barrier *pruneReadBarrier
}

func (c pruneBarrierConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
	if err != nil || !c.barrier.candidate(query) {
		return rows, err
	}
	return &pruneBarrierRows{Rows: rows, barrier: c.barrier, ctx: ctx}, nil
}
func (c pruneBarrierConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}

type pruneBarrierRows struct {
	driver.Rows
	barrier *pruneReadBarrier
	ctx     context.Context
}

func (r *pruneBarrierRows) Close() error {
	err := r.Rows.Close()
	r.barrier.once.Do(func() {
		close(r.barrier.captured)
		select {
		case <-r.barrier.resume:
		case <-r.ctx.Done():
		}
	})
	return err
}

func TestEncryptedPruneRetriedDeliverySurvivesCandidateRace(t *testing.T) {
	for _, source := range []string{"turn", "control"} {
		t.Run(source, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			path := filepath.Join(t.TempDir(), "prune-race.db")
			setupDB := openSQLite(t, path)
			t.Cleanup(func() { require.NoError(t, setupDB.Close()) })
			options := Options{PayloadCodec: newPayloadTestCodec(t), PayloadNamespace: "prune-race", RequirePayloadEncryption: true}
			require.NoError(t, Install(ctx, setupDB, SQLite, options))
			setup, err := open(ctx, setupDB, SQLite, options)
			require.NoError(t, err)
			var delivery core.Delivery
			if source == "turn" {
				delivery = workKindSettle(t, setup, "retry-turn", 1)[0]
			} else {
				accepted, err := setup.Ingest(ctx, conformance.SampleVersion(), conformance.StopMessage("retry-control"))
				require.NoError(t, err)
				require.NotNil(t, accepted.Control)
				require.NotNil(t, accepted.Reply)
				delivery = *accepted.Reply
			}
			claimed, err := setup.LeaseDelivery(ctx, "setup", time.Minute)
			require.NoError(t, err)
			require.NotNil(t, claimed)
			require.Equal(t, delivery.ID, claimed.ID)
			require.NoError(t, setup.CompleteDelivery(ctx, claimed.Lease, core.DeliveryResult{Status: core.DeliveryFailed, ErrorKind: "permanent"}))
			barrier := &pruneReadBarrier{source: source, captured: make(chan struct{}), resume: make(chan struct{})}
			defer barrier.release()
			prunedDB := sql.OpenDB(pruneBarrierConnector{dsn: fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_txlock=immediate", path), barrier: barrier})
			prunedDB.SetMaxOpenConns(1)
			t.Cleanup(func() { require.NoError(t, prunedDB.Close()) })
			pruner, err := open(ctx, prunedDB, SQLite, options)
			require.NoError(t, err)
			retryDB := openSQLite(t, path)
			retryDB.SetMaxOpenConns(1)
			t.Cleanup(func() { require.NoError(t, retryDB.Close()) })
			retrier, err := open(ctx, retryDB, SQLite, options)
			require.NoError(t, err)
			type pruneResult struct {
				removed core.PruneResult
				err     error
			}
			result := make(chan pruneResult, 1)
			go func() {
				removed, err := pruner.Prune(ctx, time.Now().Add(time.Hour))
				result <- pruneResult{removed, err}
			}()
			select {
			case <-barrier.captured:
			case early := <-result:
				t.Fatalf("prune returned before candidate barrier: %+v", early)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			retried, err := retrier.RetryDelivery(ctx, delivery.ID)
			require.NoError(t, err)
			require.NotNil(t, retried)
			require.Equal(t, core.DeliveryPending, retried.Status)
			var before string
			require.NoError(t, retryDB.QueryRowContext(ctx, "SELECT data FROM channel_deliveries WHERE id=?", delivery.ID).Scan(&before))
			barrier.release()
			completed := <-result
			require.NoError(t, completed.err)
			require.Zero(t, completed.removed.Turns+completed.removed.Controls+completed.removed.Deliveries, "concurrent retry makes the source ineligible for pruning")
			var after, status string
			require.NoError(t, retryDB.QueryRowContext(ctx, "SELECT data,status FROM channel_deliveries WHERE id=?", delivery.ID).Scan(&after, &status))
			require.Equal(t, "pending", status)
			require.Equal(t, before, after)
		})
	}
}

func TestPrunePreservesUnsupportedTerminalTurnProtocol(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		for _, mutation := range []string{"snapshot", "phase"} {
			t.Run(fmt.Sprintf("encrypted_%t/%s", encrypted, mutation), func(t *testing.T) {
				ctx := context.Background()
				db := lifecycleDB(t)
				options := Options{}
				if encrypted {
					options.PayloadCodec = newPayloadTestCodec(t)
					options.PayloadNamespace = "prune-protocol"
					options.RequirePayloadEncryption = true
				}
				require.NoError(t, Install(ctx, db, SQLite, options))
				store, err := open(ctx, db, SQLite, options)
				require.NoError(t, err)
				workKindSettle(t, store, "unsupported-terminal", 0)
				turns, err := store.ListTurns(ctx, core.TurnQuery{})
				require.NoError(t, err)
				require.Len(t, turns, 1)
				var raw string
				require.NoError(t, db.QueryRow("SELECT data FROM channel_turns WHERE id=?", turns[0].ID).Scan(&raw))
				var object map[string]any
				require.NoError(t, json.Unmarshal([]byte(raw), &object))
				if mutation == "phase" {
					object["phase"] = "future_phase"
				} else {
					object["snapshot"] = map[string]any{"schema_version": 99}
				}
				future, err := json.Marshal(object)
				require.NoError(t, err)
				_, err = db.Exec("UPDATE channel_turns SET data=? WHERE id=?", string(future), turns[0].ID)
				require.NoError(t, err)
				removed, err := store.Prune(ctx, time.Now().Add(time.Hour))
				require.NoError(t, err)
				require.Zero(t, removed.Turns)
				require.NoError(t, db.QueryRow("SELECT data FROM channel_turns WHERE id=?", turns[0].ID).Scan(&raw))
				require.Equal(t, string(future), raw)
			})
		}
	}
}
