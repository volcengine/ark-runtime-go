// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

// 本文件只覆盖 bbolt 实现特有的行为（结构校验、独占锁、只读打开、文件权限、索引一致性）；
// 通用 store 契约见 conformance_test.go。

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestStoreRestartAndLock(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store.db")
	s, err := openInstalledFixture(path, Options{LockTimeout: 10 * time.Millisecond})
	require.NoError(t, err)
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	_, err = openInstalledFixture(path, Options{LockTimeout: 10 * time.Millisecond})
	require.Error(t, err)
	first, err := s.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("one"))
	require.NoError(t, err)
	require.NoError(t, s.Close())
	s, err = openInstalledFixture(path, Options{LockTimeout: 10 * time.Millisecond})
	require.NoError(t, err)
	again, err := s.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("one"))
	require.NoError(t, err)
	require.True(t, again.Duplicate)
	require.Equal(t, first.Turn.ID, again.Turn.ID)
	require.NoError(t, s.Close())

}

func TestStoreRejectsIncompleteStructureWithoutChangingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	// meta 与未知 bucket 不能代替当前要求的完整结构。
	db, err := bbolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucket([]byte("meta"))
		if err != nil {
			return err
		}
		_, err = tx.CreateBucket([]byte("tasks"))
		return err
	}))
	require.NoError(t, db.Close())
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	_, err = openInstalledFixture(path, Options{})
	require.ErrorIs(t, err, errSchema)
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.True(t, bytes.Equal(before, after))
}

func TestStoreRejectsUnknownContent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	db, err := bbolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bbolt.Tx) error { _, err := tx.CreateBucket([]byte("other")); return err }))
	require.NoError(t, db.Close())
	_, err = openInstalledFixture(path, Options{})
	require.ErrorIs(t, err, errSchema)
}

func TestStoreReadOnlyStatusDoesNotMutateDatabase(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store.db")
	s, err := openInstalledFixture(path, Options{})
	require.NoError(t, err)
	_, err = s.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("one"))
	require.NoError(t, err)
	require.NoError(t, s.Close())
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	readonly, err := openInstalledFixture(path, Options{ReadOnly: true, LockTimeout: time.Second})
	require.NoError(t, err)
	stats, err := readonly.Stats(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, stats.QueuedTurns)
	_, err = readonly.Ingest(ctx, conformance.SampleVersion(), conformance.SampleMessage("two"))
	require.Error(t, err)
	require.NoError(t, readonly.Close())
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.True(t, bytes.Equal(before, after))
	missing := filepath.Join(t.TempDir(), "missing.db")
	_, err = openInstalledFixture(missing, Options{ReadOnly: true, LockTimeout: 10 * time.Millisecond})
	require.Error(t, err)
	_, err = os.Stat(missing)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestStoreDefaultLockDeadline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	writer, err := openInstalledFixture(path, Options{})
	require.NoError(t, err)
	result := make(chan error, 1)
	go func() {
		other, openErr := openInstalledFixture(path, Options{})
		if other != nil {
			_ = other.Close()
		}
		result <- openErr
	}()
	select {
	case openErr := <-result:
		require.Error(t, openErr)
	case <-time.After(2 * time.Second):
		require.NoError(t, writer.Close())
		<-result
		t.Fatal("default lock timeout did not return within two seconds")
	}
	require.NoError(t, writer.Close())
	_, err = openInstalledFixture(path, Options{LockTimeout: -time.Second})
	require.ErrorIs(t, err, errInvalid)
}

// TestIndexesStayBoundedByOpenWork 验证就绪与未结算索引只含进行中的数据：大量历史结算后
// 索引为空，清理后主 bucket 与去重索引也一并清空。
func TestIndexesStayBoundedByOpenWork(t *testing.T) {
	ctx := context.Background()
	clock := conformance.NewClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	s, err := openInstalledFixture(filepath.Join(t.TempDir(), "store.db"), Options{Clock: clock.Now, NoSync: true})
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	for i := 0; i < 30; i++ {
		message := conformance.SampleMessage(fmt.Sprint(i))
		message.ChatID = fmt.Sprint("chat-", i%3)
		_, err := s.Ingest(ctx, conformance.SampleVersion(), message)
		require.NoError(t, err)
		_, err = s.Ingest(ctx, conformance.SampleVersion(), core.Message{BindingID: message.BindingID, ProviderEventID: "alias-" + fmt.Sprint(i), MessageID: message.MessageID, TenantKey: message.TenantKey, ChatID: message.ChatID, ChatType: message.ChatType, SenderID: message.SenderID, Text: message.Text})
		require.NoError(t, err)
	}
	for {
		leased, err := s.LeaseTurn(ctx, conformance.Claim("w"))
		require.NoError(t, err)
		if leased == nil {
			break
		}
		require.NoError(t, s.SettleTurn(ctx, leased.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, nil))
	}
	count := func(name []byte) int {
		n := 0
		require.NoError(t, s.db.View(func(tx *bbolt.Tx) error { n = countKeys(tx.Bucket(name)); return nil }))
		return n
	}
	for _, name := range [][]byte{bucketTurnReady, bucketOpenTurns, bucketTurnsByConv} {
		require.Zero(t, count(name), string(name))
	}
	require.Equal(t, 30, count(bucketSettledTurns))
	clock.Advance(time.Hour)
	removed, err := s.Prune(ctx, clock.Now())
	require.NoError(t, err)
	require.Equal(t, 30, removed.Turns)
	for _, name := range [][]byte{bucketTurns, bucketMessages, bucketMsgByProviderID, bucketMsgByEventID, bucketEventsByMsg, bucketSettledTurns, bucketTurnsByVersion} {
		require.Zero(t, count(name), string(name))
	}
	require.Equal(t, 1, removed.BindingVersions, "an unreferenced version older than the cutoff is removed")
	require.Zero(t, count(bucketVersions))
	require.Equal(t, 3, count(bucketConversations), "conversations are long-lived")
}
