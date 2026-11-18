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
	"path/filepath"
	"time"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/secret"
	"github.com/volcengine/ark-runtime-go/channel/store/embedded"
	sqlstore "github.com/volcengine/ark-runtime-go/channel/store/sql"
)

// Install 显式安装全新本地文件或 SQL namespace，不启动渠道或访问 MA/飞书。
// 已有本地文件不能被覆盖；已有 SQL 数据必须使用独立维护入口。
func Install(ctx context.Context, config Config) error {
	return InstallStorageWith(ctx, config, nil)
}

// InstallStorageWith 允许注入 MySQL DSN 的解析器；只解析安装所需的存储凭证。
func InstallStorageWith(ctx context.Context, config Config, resolver secret.Resolver) error {
	if ctx == nil {
		return errors.New("install context required")
	}
	if err := config.Validate(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	crypt, err := userAuthCodec(config, resolver)
	if err != nil {
		return err
	}
	config.payloadCodec = crypt
	if config.Store.Driver == "mysql" {
		if resolver == nil {
			resolver = secret.EnvFileResolver{}
		}
		dsn, err := resolver.Resolve(config.Store.DSNRef)
		if err != nil || dsn == "" {
			return errors.New("store dsn unavailable")
		}
		return installSQLStorage(ctx, dsn, sqlstore.MySQL, config)
	}
	if _, err := os.Lstat(config.DatabasePath); err == nil {
		return errors.New("database already exists; installation was not started")
	} else if !errors.Is(err, os.ErrNotExist) {
		return errors.New("database unavailable")
	}
	if err := os.MkdirAll(filepath.Dir(config.DatabasePath), 0700); err != nil {
		return errors.New("database directory unavailable")
	}
	if config.Store.Driver == "embedded" || config.Store.Driver == "" {
		return installStorageError(ctx, embedded.Install(config.DatabasePath, embedded.Options{PayloadCodec: config.payloadCodec, PayloadNamespace: config.Store.PayloadNamespace, RequirePayloadEncryption: config.payloadCodec != nil, LockTimeout: 500 * time.Millisecond, MaxOpenTurns: config.Store.MaxOpenTurns, MaxMessageBytes: config.Store.MaxMessageBytes}))
	}
	// 先以排他方式取得新路径，避免 SQLite 默认连接隐式覆盖已有文件。
	file, err := os.OpenFile(config.DatabasePath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return errors.New("database installation could not create a new file")
	}
	if err = file.Close(); err != nil {
		return errors.New("database installation file close failed")
	}
	dsn := fmt.Sprintf("file:%s?mode=rw&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_txlock=immediate", url.PathEscape(config.DatabasePath))
	return installSQLStorage(ctx, dsn, sqlstore.SQLite, config)
}

func installSQLStorage(ctx context.Context, dsn string, dialect sqlstore.Dialect, config Config) (result error) {
	db, err := sql.Open(dialect.Name, dsn)
	if err != nil {
		return errors.New("database unavailable")
	}
	defer func() {
		if err := db.Close(); err != nil {
			result = errors.Join(result, errors.New("database install cleanup failed; inspect actual state before retrying"))
		}
	}()
	installCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	err = sqlstore.Install(installCtx, db, dialect, sqlstore.Options{PayloadCodec: config.payloadCodec, PayloadNamespace: config.Store.PayloadNamespace, RequirePayloadEncryption: config.payloadCodec != nil, TablePrefix: config.Store.TablePrefix, MaxOpenTurns: config.Store.MaxOpenTurns, MaxMessageBytes: config.Store.MaxMessageBytes})
	return installStorageError(ctx, err)
}

func installStorageError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, core.ErrConflict) {
		return errors.New("database already exists; installation was not started")
	}
	return errors.New("database install failed; inspect actual state before retrying")
}

// ExpandUserAuthStorage 显式安装既有加密库的可选 User 授权结构；不迁移明文历史库。
func ExpandUserAuthStorage(ctx context.Context, config Config) error {
	return ExpandUserAuthStorageWith(ctx, config, nil)
}

// ExpandUserAuthStorageWith 允许注入密钥解析器，普通 run/status 永不调用此入口。
func ExpandUserAuthStorageWith(ctx context.Context, config Config, resolver secret.Resolver) error {
	if ctx == nil {
		return errors.New("expand context required")
	}
	if err := config.Validate(); err != nil {
		return err
	}
	if !config.UserAuth.enabled() {
		return errors.New("expand-user-auth requires user_auth configuration")
	}
	store, err := openStore(ctx, config, resolver, modeExpand)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	expander, ok := store.(interface{ ExpandUserAuth(context.Context) error })
	if !ok {
		return errors.New("user_auth storage expansion unavailable")
	}
	if err := expander.ExpandUserAuth(ctx); err != nil {
		return errors.New("user_auth storage expansion failed; inspect actual state before retrying")
	}
	return nil
}
