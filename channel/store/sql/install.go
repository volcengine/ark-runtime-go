// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/payload"
)

// Install 为自托管的空 namespace 显式建库。
// MySQL ddl 非事务化，失败后保留实际结构；可重试仅含已知空表的部分安装。
func Install(ctx context.Context, db *sql.DB, dialect Dialect, options Options) error {
	s, err := configuredStore(db, dialect, options)
	if err != nil {
		return err
	}
	if err = s.checkEmptyInstallation(ctx, options.TablePrefix); err != nil {
		return err
	}
	for _, statement := range append(ddl(dialect, options.TablePrefix), userAuthDDL(dialect, options.TablePrefix)...) {
		if _, err = db.ExecContext(ctx, statement); err != nil && !isAlreadyExists(err) {
			return err
		}
	}
	_, err = initializeInstalled(ctx, db, dialect, options)
	return err
}

// initializeInstalled 在人工完成建表后显式初始化空库的载荷策略；始终不执行 ddl。
// 已就绪策略幂等返回；非空明文库不允许切换为密文策略。
func initializeInstalled(ctx context.Context, db *sql.DB, dialect Dialect, options Options) (storageReport, error) {
	s, err := configuredStore(db, dialect, options)
	if err != nil {
		return storageReport{}, err
	}
	report, err := inspect(ctx, s.db, s.dialect, options)
	if err != nil {
		return report, err
	}
	var structure []string
	for _, missing := range report.Missing {
		if missing != "payload_policy" {
			structure = append(structure, missing)
		}
	}
	if len(structure) > 0 {
		return report, fmt.Errorf("%w: missing %s", errSchema, strings.Join(structure, ", "))
	}
	if report.PayloadMode != "" && !report.NamespaceMatches {
		return report, openReportError(report, options)
	}
	err = s.runTxOnce(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var current string
		readErr := tx.QueryRowContext(ctx, "SELECT v FROM "+s.t.meta+" WHERE k='payload_mode'"+s.dialect.RowLockSuffix).Scan(&current)
		if readErr != nil && !errors.Is(readErr, sql.ErrNoRows) {
			return readErr
		}
		expected := ""
		if options.PayloadCodec != nil || options.RequirePayloadEncryption {
			expected = encryptedMode(options.PayloadNamespace)
		}
		policyReady := readErr == nil && current == expected
		if policyReady {
			return nil
		}
		if current != "" && !policyReady {
			return payload.ErrPolicyMismatch
		}
		for _, dependency := range requirements(ddl(s.dialect, options.TablePrefix)) {
			table := dependency.table
			if table == s.t.meta {
				continue
			}
			var count int
			if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
				return err
			}
			if count > 0 {
				return payload.ErrPolicyMismatch
			}
		}
		if errors.Is(readErr, sql.ErrNoRows) {
			if _, err := tx.ExecContext(ctx, "INSERT INTO "+s.t.meta+" (k,v) VALUES ('payload_mode',?)", expected); err != nil {
				return err
			}
		} else if _, err := tx.ExecContext(ctx, "UPDATE "+s.t.meta+" SET v=? WHERE k='payload_mode' AND v=''", expected); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return report, err
	}
	return inspect(ctx, s.db, s.dialect, options)
}

func (s *store) checkEmptyInstallation(ctx context.Context, prefix string) error {
	q := "SELECT name FROM sqlite_master WHERE type='table' AND substr(name,1,?)=?"
	if s.dialect.Name == mysqlName {
		q = "SELECT table_name FROM information_schema.tables WHERE table_schema=DATABASE() AND LEFT(table_name,?)=?"
	}
	namespace := prefix + "channel_"
	rows, err := s.db.QueryContext(ctx, q, len(namespace), namespace)
	if err != nil {
		return err
	}
	var tables []string
	for rows.Next() {
		var table string
		if err = rows.Scan(&table); err != nil {
			break
		}
		tables = append(tables, table)
	}
	if err == nil {
		err = rows.Err()
	}
	_ = rows.Close()
	if err != nil {
		return err
	}
	known := map[string]storageRequirement{}
	for _, r := range requirements(ddl(s.dialect, prefix)) {
		known[r.table] = r
	}
	for _, table := range tables {
		r, ok := known[table]
		if !ok {
			return errSchema
		}
		actual, err := actualColumns(ctx, s.db, s.dialect, table)
		if err != nil {
			return err
		}
		for _, column := range r.columns {
			if !actual[column] {
				return errSchema
			}
		}
		var count int
		if err = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
			return err
		}
		if count > 0 {
			return fmt.Errorf("%w: installation requires empty namespace", core.ErrConflict)
		}
	}
	return nil
}
