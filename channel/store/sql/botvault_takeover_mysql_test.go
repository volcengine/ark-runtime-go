// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

//go:build mysqlintegration

package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestMySQLBotVaultExpiredTakeoverHasSingleFenceOwner(t *testing.T) {
	ctx := t.Context()
	clock := conformance.NewClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	prefix := fmt.Sprintf("bt%d_", time.Now().UnixNano())
	db := mysqlTestDB(t)
	first, err := openInstalledSQLFixture(ctx, db, MySQL, Options{TablePrefix: prefix, Clock: clock.Now})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, first.Close()) })
	second, err := openInstalledSQLFixture(ctx, mysqlTestDB(t), MySQL, Options{TablePrefix: prefix, Clock: clock.Now})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, second.Close()) })
	original, err := first.ReserveBotVault(ctx, "principal", "vault-name", "original", time.Second)
	require.NoError(t, err)
	clock.Advance(2 * time.Second)
	blocker := mysqlTestDB(t)
	defer func() { require.NoError(t, blocker.Close()) }()
	tx, err := blocker.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var row string
	require.NoError(t, tx.QueryRowContext(ctx, "SELECT data FROM "+prefix+"channel_bot_vault_reservations WHERE principal=? AND name=? FOR UPDATE", "principal", "vault-name").Scan(&row))
	type outcome struct {
		owner  string
		record core.BotVaultReservation
		err    error
	}
	results := make(chan outcome, 2)
	start := make(chan struct{})
	for i, s := range []*store{first, second} {
		go func(owner string, store *store) {
			<-start
			r, e := store.ReserveBotVault(ctx, "principal", "vault-name", owner, time.Minute)
			results <- outcome{owner: owner, record: r, err: e}
		}(fmt.Sprintf("owner-%d", i), s)
	}
	close(start)
	observer := mysqlTestDB(t)
	defer func() { require.NoError(t, observer.Close()) }()
	require.Eventually(t, func() bool {
		var waiters int
		e := observer.QueryRowContext(ctx, "SELECT COUNT(DISTINCT l.THREAD_ID) FROM performance_schema.data_lock_waits w JOIN performance_schema.data_locks l ON l.ENGINE_LOCK_ID=w.REQUESTING_ENGINE_LOCK_ID WHERE l.OBJECT_SCHEMA=DATABASE() AND l.OBJECT_NAME=?", prefix+"channel_bot_vault_reservations").Scan(&waiters)
		require.NoError(t, e)
		return waiters == 2
	}, 5*time.Second, 5*time.Millisecond, "both concurrent attempts must reach the held reservation row before releasing it")
	require.NoError(t, tx.Commit())
	winners, losers := 0, 0
	var winner outcome
	for range 2 {
		r := <-results
		if r.err == nil {
			winners++
			winner = r
			require.Equal(t, original.Fence+1, r.record.Fence)
			require.Equal(t, r.owner, r.record.Owner)
		} else {
			require.True(t, errors.Is(r.err, core.ErrVaultNameConflict), r.err)
			losers++
		}
	}
	require.Equal(t, 1, winners, "an expired fence cannot grant the same generation to two owners")
	require.Equal(t, 1, losers)
	current, err := first.GetBotVaultReservation(ctx, "principal", "vault-name")
	require.NoError(t, err)
	require.NotNil(t, current)
	require.Equal(t, winner.owner, current.Owner)
	require.Equal(t, winner.record.Fence, current.Fence)
	_, err = first.MarkBotVaultPosted(context.Background(), "principal", "vault-name", winner.owner, winner.record.Fence)
	require.NoError(t, err)
}

func TestMySQLBotVaultConcurrentInitialReservationReturnsDomainConflict(t *testing.T) {
	ctx := t.Context()
	prefix := fmt.Sprintf("bi%d_", time.Now().UnixNano())
	db := mysqlTestDB(t)
	s, err := openInstalledSQLFixture(ctx, db, MySQL, Options{TablePrefix: prefix})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	gate := prefix + "reservation_test_gate"
	_, err = db.ExecContext(ctx, "CREATE TABLE "+gate+" (id INT NOT NULL PRIMARY KEY,counter_value INT NOT NULL)")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "INSERT INTO "+gate+" VALUES(1,0)")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "CREATE TRIGGER "+prefix+"reservation_insert_gate BEFORE INSERT ON "+prefix+"channel_bot_vault_reservations FOR EACH ROW UPDATE "+gate+" SET counter_value=counter_value+1 WHERE id=1")
	require.NoError(t, err)
	blocker := mysqlTestDB(t)
	defer func() { require.NoError(t, blocker.Close()) }()
	tx, err := blocker.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(ctx, "UPDATE "+gate+" SET counter_value=counter_value+1 WHERE id=1")
	require.NoError(t, err)
	results := make(chan error, 2)
	start := make(chan struct{})
	for i := range 2 {
		go func(owner string) {
			<-start
			_, e := s.ReserveBotVault(ctx, "new-principal", "new-name", owner, time.Minute)
			results <- e
		}(fmt.Sprintf("owner-%d", i))
	}
	close(start)
	observer := mysqlTestDB(t)
	defer func() { require.NoError(t, observer.Close()) }()
	require.Eventually(t, func() bool {
		var waiters int
		e := observer.QueryRowContext(ctx, "SELECT COUNT(DISTINCT l.THREAD_ID) FROM performance_schema.data_lock_waits w JOIN performance_schema.data_locks l ON l.ENGINE_LOCK_ID=w.REQUESTING_ENGINE_LOCK_ID WHERE l.OBJECT_SCHEMA=DATABASE() AND l.OBJECT_NAME=?", gate).Scan(&waiters)
		require.NoError(t, e)
		return waiters == 2
	}, 5*time.Second, 5*time.Millisecond, "both calls must observe the missing reservation before either INSERT completes")
	require.NoError(t, tx.Commit())
	successes, conflicts := 0, 0
	for range 2 {
		e := <-results
		if e == nil {
			successes++
		} else {
			require.ErrorIs(t, e, core.ErrVaultNameConflict, "a unique-key loser is a normal reservation conflict")
			conflicts++
		}
	}
	require.Equal(t, 1, successes)
	require.Equal(t, 1, conflicts)
}
