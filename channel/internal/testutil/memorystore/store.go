// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Package memory 提供仅用于测试的内存 Store：语义与持久实现一致（通过同一套 conformance），
// 但进程退出即丢失全部状态，不得用于生产。
package memory

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite" // 内存 SQLite 驱动

	"github.com/volcengine/ark-runtime-go/channel/internal/handles"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/codec"
	sqlstore "github.com/volcengine/ark-runtime-go/channel/store/sql"
)

var counter atomic.Uint64

// Options 指定时钟与容量。
type Options struct {
	PayloadCodec             codec.Codec
	PayloadNamespace         string
	RequirePayloadEncryption bool
	// MaxChannelOpenTurns 限制同一 Channel 跨 epoch 的未结算轮次，0 关闭。
	MaxChannelOpenTurns int
	// MaxRejectedMessages 是拒绝去重记录总预算，0 关闭持久拒绝。
	MaxRejectedMessages int
	// RejectNoticeInterval 限制同会话拒绝通知频率，0 使用一分钟。
	RejectNoticeInterval time.Duration

	Clock           func() time.Time
	MaxOpenTurns    int
	MaxMessageBytes int
	QueueTTL        time.Duration
}

type backend interface {
	core.Store
	core.Exporter
	core.UserAuthStore
	core.TextStreamStore
	core.ProgressDeliveryAttemptStore
	UserAuthEncryptionReady() bool
	ExpandUserAuth(context.Context) error
}

// Store 组合SQL事务实现并拥有内存数据库的完整生命周期。
type Store struct {
	backend
	keeperDB  *sql.DB
	keeper    *sql.Conn
	closeOnce sync.Once
	closeErr  error
}

// Close 幂等关闭此内存Store拥有的资源。
func (s *Store) Close() error {
	s.closeOnce.Do(func() { s.closeErr = errors.Join(s.backend.Close(), s.keeper.Close(), s.keeperDB.Close()) })
	return s.closeErr
}

// New 返回一个独立的内存 Store。实现复用 store/sql 的事务语义，保证测试与生产行为一致。
func New(options Options) (*Store, error) {
	name := fmt.Sprintf("file:channel-memory-%d?mode=memory&cache=shared&_txlock=immediate&_pragma=busy_timeout(5000)", counter.Add(1))
	db, err := sql.Open("sqlite", name)
	if err != nil {
		return nil, err
	}
	return openMemory(db, name, options)
}

// openMemory 接管工作数据库句柄；初始化失败时同样关闭它。
func openMemory(db *sql.DB, name string, options Options) (*Store, error) {
	// 单工作连接只控制事务串行；取消/ErrBadConn会丢弃它，不能由它承担数据库保活。
	db.SetMaxOpenConns(1)
	keeperDB, err := sql.Open("sqlite", name)
	if err != nil {
		return nil, errors.Join(err, db.Close())
	}
	keeperDB.SetMaxOpenConns(1)
	// 固定保活连接不执行任何工作查询，也不接收Runtime的取消上下文。
	keeper, err := keeperDB.Conn(context.Background())
	if err != nil {
		return nil, errors.Join(err, keeperDB.Close(), db.Close())
	}
	storeOptions := sqlstore.Options{PayloadCodec: options.PayloadCodec, PayloadNamespace: options.PayloadNamespace, RequirePayloadEncryption: options.RequirePayloadEncryption, MaxChannelOpenTurns: options.MaxChannelOpenTurns, MaxRejectedMessages: options.MaxRejectedMessages, RejectNoticeInterval: options.RejectNoticeInterval, Clock: options.Clock, MaxOpenTurns: options.MaxOpenTurns, MaxMessageBytes: options.MaxMessageBytes, QueueTTL: options.QueueTTL}
	if err := sqlstore.Install(context.Background(), db, sqlstore.SQLite, storeOptions); err != nil {
		return nil, errors.Join(err, keeper.Close(), keeperDB.Close(), db.Close())
	}
	store, err := sqlstore.Open(context.Background(), db, sqlstore.SQLite, storeOptions)
	if err != nil {
		return nil, errors.Join(err, keeper.Close(), keeperDB.Close(), db.Close())
	}
	return &Store{backend: handles.StoreValue(store).(backend), keeperDB: keeperDB, keeper: keeper}, nil
}
