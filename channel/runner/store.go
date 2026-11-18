// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package runner

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql" // mysql 驱动
	bbolterrors "go.etcd.io/bbolt/errors"
	_ "modernc.org/sqlite" // sqlite 驱动

	"github.com/volcengine/ark-runtime-go/channel/internal/handles"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage"
	"github.com/volcengine/ark-runtime-go/channel/secret"
	"github.com/volcengine/ark-runtime-go/channel/store/embedded"
	sqlstore "github.com/volcengine/ark-runtime-go/channel/store/sql"
)

type storeMode int

const (
	// modeRun 是 Runner 自身的写入方；只打开已经显式安装的数据库。
	modeRun storeMode = iota
	// modeAdmin 是离线运维写入方；数据库必须已存在，embedded 下与 Runner 互斥。
	modeAdmin
	// modeReadOnly 只读打开已存在的数据库。
	modeReadOnly
	// modeExpand 仅用于显式安装可选授权结构；不在普通 Open 中执行 DDL。
	modeExpand
)

// openStore 按配置打开 Store；错误信息不含 DSN 或路径之外的敏感内容。
func openStore(ctx context.Context, config Config, resolver secret.Resolver, mode storeMode) (core.Store, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if resolver == nil {
		resolver = secret.EnvFileResolver{}
	}
	crypt, err := userAuthCodec(config, resolver)
	if err != nil {
		return nil, err
	}
	config.payloadCodec = crypt
	switch config.Store.Driver {
	case "", "embedded":
		if err := requireDatabaseFile(config.DatabasePath); err != nil {
			return nil, err
		}
		options := embedded.Options{PayloadCodec: config.payloadCodec, PayloadNamespace: config.Store.PayloadNamespace, RequirePayloadEncryption: config.payloadCodec != nil, LockTimeout: 500 * time.Millisecond, MaxOpenTurns: config.Store.MaxOpenTurns, MaxMessageBytes: config.Store.MaxMessageBytes}
		if mode != modeRun {
			options.LockTimeout = 200 * time.Millisecond
		}
		options.ReadOnly = mode == modeReadOnly
		handle, err := embedded.Open(config.DatabasePath, options)
		if err != nil {
			if errors.Is(err, storage.ErrEmbeddedSchema) {
				return nil, errors.New("database schema unsupported")
			}
			if errors.Is(err, bbolterrors.ErrTimeout) {
				if mode == modeReadOnly {
					return nil, errors.New("database writer active; stop runner before status")
				}
				return nil, errors.New("database writer active; stop runner first")
			}
			return nil, errors.New("database unavailable")
		}
		store := handles.StoreValue(handle)
		if err := checkUserAuthStorageMode(ctx, config, store, mode); err != nil {
			_ = store.Close()
			return nil, err
		}
		return store, nil
	case "sqlite":
		if err := requireDatabaseFile(config.DatabasePath); err != nil {
			return nil, err
		}
		dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)", url.PathEscape(config.DatabasePath))
		if mode == modeReadOnly {
			// 只读检查不能试图切换journal mode，旧库也必须先准确返回ErrSchema。
			dsn += "&mode=ro"
		} else {
			// mode=rw 防止 stat 后文件消失时连接重新创建空库；WAL 只在显式安装时设置。
			dsn += "&mode=rw&_txlock=immediate"
		}
		return openSQL(ctx, dsn, sqlstore.SQLite, config, mode)
	case "mysql":
		dsn, err := resolver.Resolve(config.Store.DSNRef)
		if err != nil || dsn == "" {
			return nil, errors.New("store dsn unavailable")
		}
		return openSQL(ctx, dsn, sqlstore.MySQL, config, mode)
	}
	return nil, errors.New("store driver unsupported")
}

func requireDatabaseFile(path string) error {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return errors.New("database does not exist; run channel-runner install first")
	}
	if err != nil || !info.Mode().IsRegular() {
		return errors.New("database unavailable")
	}
	return nil
}

func openSQL(ctx context.Context, dsn string, dialect sqlstore.Dialect, config Config, mode storeMode) (core.Store, error) {
	db, err := sql.Open(dialect.Name, dsn)
	if err != nil {
		return nil, errors.New("database unavailable")
	}
	db.SetMaxOpenConns(16)
	db.SetConnMaxLifetime(30 * time.Minute)
	openCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	handle, err := sqlstore.Open(openCtx, db, dialect, sqlstore.Options{PayloadCodec: config.payloadCodec, PayloadNamespace: config.Store.PayloadNamespace, RequirePayloadEncryption: config.payloadCodec != nil, TablePrefix: config.Store.TablePrefix, MaxOpenTurns: config.Store.MaxOpenTurns, MaxMessageBytes: config.Store.MaxMessageBytes})
	if err != nil {
		_ = db.Close()
		if errors.Is(err, storage.ErrSQLSchema) {
			return nil, fmt.Errorf("database schema unsupported: %w", storage.ErrSQLSchema)
		}
		return nil, errors.New("database unavailable")
	}
	store := handles.StoreValue(handle)
	if err := checkUserAuthStorageMode(ctx, config, store, mode); err != nil {
		_ = store.Close()
		return nil, err
	}
	return store, nil
}

func checkUserAuthStorageMode(ctx context.Context, config Config, store core.Store, mode storeMode) error {
	if mode == modeExpand {
		return nil
	}
	return checkUserAuthStorage(ctx, config, store)
}
