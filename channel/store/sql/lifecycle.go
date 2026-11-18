// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package sqlstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/payload"
)

// storageReport 描述实际结构与载荷策略。
type storageReport struct {
	Ready            bool
	PayloadMode      string
	NamespaceMatches bool
	Missing          []string
}

func configuredStore(db *sql.DB, dialect Dialect, options Options) (*store, error) {
	if (options.PayloadCodec != nil || options.RequirePayloadEncryption) && options.PayloadNamespace == "" || db == nil || (dialect.Name != sqliteName && dialect.Name != mysqlName) || !validPrefix(options.TablePrefix) || options.MaxChannelOpenTurns < 0 || options.MaxRejectedMessages < 0 || options.RejectNoticeInterval < 0 || options.MaxOpenTurns < 0 || options.MaxMessageBytes < 0 || options.QueueTTL < 0 {
		return nil, core.ErrInvalid
	}
	s := &store{payloadCodec: options.PayloadCodec, payloadNamespace: options.PayloadNamespace, requirePayloadEncryption: options.RequirePayloadEncryption, databaseClock: options.Clock == nil && dialect.Name == mysqlName, db: db, dialect: dialect, clock: options.Clock, maxChannelOpenTurns: options.MaxChannelOpenTurns, maxRejectedMessages: options.MaxRejectedMessages, rejectNoticeInterval: options.RejectNoticeInterval, maxOpenTurns: options.MaxOpenTurns, maxMessageBytes: options.MaxMessageBytes, queueTTL: options.QueueTTL, t: tableNames(options.TablePrefix)}
	if s.clock == nil {
		s.clock = time.Now
	}
	if s.rejectNoticeInterval == 0 {
		s.rejectNoticeInterval = time.Minute
	}
	if s.maxOpenTurns == 0 {
		s.maxOpenTurns = 1000
	}
	if s.maxMessageBytes == 0 {
		s.maxMessageBytes = 32768
	}
	return s, nil
}

func encryptedMode(namespace string) string {
	sum := sha256.Sum256([]byte(namespace))
	return "enc-v1:" + hex.EncodeToString(sum[:])[:56]
}

// inspect 只执行 SELECT，按实际必需列及索引检查存储，不解释全局版本数字。
// 明文库允许 payload_mode 缺失；加密库必须已显式完成策略初始化。
func inspect(ctx context.Context, db *sql.DB, dialect Dialect, options Options) (storageReport, error) {
	report := storageReport{}
	s, err := configuredStore(db, dialect, options)
	if err != nil {
		return report, err
	}
	report.Missing, err = inspectRequirements(ctx, db, dialect, ddl(dialect, options.TablePrefix))
	if err != nil {
		var missing *mysql.MySQLError
		if errors.As(err, &missing) && missing.Number == 1146 {
			return report, fmt.Errorf("%w: %w", errSchema, err)
		}
		return report, err
	}
	metaExists, err := storageTableExists(ctx, db, dialect, s.t.meta)
	if err != nil {
		return report, err
	}
	if metaExists {
		columns, columnErr := actualColumns(ctx, db, dialect, s.t.meta)
		if columnErr != nil {
			return report, columnErr
		}
		metaExists = columns["k"] && columns["v"]
	}
	if metaExists {
		err = db.QueryRowContext(ctx, "SELECT v FROM "+s.t.meta+" WHERE k='payload_mode'").Scan(&report.PayloadMode)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return report, err
		}
	}
	report.NamespaceMatches = options.PayloadCodec == nil && !options.RequirePayloadEncryption && report.PayloadMode == "" || (options.PayloadCodec != nil || options.RequirePayloadEncryption) && report.PayloadMode == encryptedMode(options.PayloadNamespace)
	if !report.NamespaceMatches {
		report.Missing = append(report.Missing, "payload_policy")
	}
	report.Ready = len(report.Missing) == 0
	return report, nil
}

func openReportError(report storageReport, options Options) error {
	if !report.NamespaceMatches {
		if report.PayloadMode == "" || options.PayloadCodec == nil && !options.RequirePayloadEncryption {
			return payload.ErrPolicyMismatch
		}
		return payload.ErrInvalid
	}
	return fmt.Errorf("%w: missing %s", errSchema, strings.Join(report.Missing, ", "))
}

type schemaQuery interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type storageIndex struct {
	columns []string
	unique  bool
}
type storageRequirement struct {
	table   string
	columns []string
	indexes []storageIndex
}

func splitDefinitions(s string) []string {
	var out []string
	depth, start := 0, 0
	for i, r := range s {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				out = append(out, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	return append(out, strings.TrimSpace(s[start:]))
}
func indexColumns(s string) []string {
	a, b := strings.Index(s, "("), strings.LastIndex(s, ")")
	if a < 0 || b < a {
		return nil
	}
	var out []string
	for _, c := range strings.Split(s[a+1:b], ",") {
		out = append(out, strings.TrimSpace(c))
	}
	return out
}
func requirements(statements []string) []storageRequirement {
	var out []storageRequirement
	byName := map[string]int{}
	for _, s := range statements {
		if strings.HasPrefix(s, "CREATE TABLE IF NOT EXISTS ") {
			a := strings.Index(s, " (")
			name := strings.TrimPrefix(s[:a], "CREATE TABLE IF NOT EXISTS ")
			r := storageRequirement{table: name}
			for _, def := range splitDefinitions(s[a+2 : len(s)-1]) {
				words := strings.Fields(def)
				switch words[0] {
				case "PRIMARY", "UNIQUE":
					r.indexes = append(r.indexes, storageIndex{columns: indexColumns(def), unique: true})
				default:
					r.columns = append(r.columns, words[0])
					if strings.Contains(def, "PRIMARY KEY") || strings.Contains(def, " UNIQUE") {
						r.indexes = append(r.indexes, storageIndex{columns: []string{words[0]}, unique: true})
					}
				}
			}
			byName[name] = len(out)
			out = append(out, r)
		} else if strings.HasPrefix(s, "CREATE INDEX ") || strings.HasPrefix(s, "CREATE UNIQUE INDEX ") {
			words := strings.Fields(s)
			on := 0
			for i, w := range words {
				if w == "ON" {
					on = i
					break
				}
			}
			if idx, ok := byName[words[on+1]]; ok {
				out[idx].indexes = append(out[idx].indexes, storageIndex{columns: indexColumns(s), unique: strings.HasPrefix(s, "CREATE UNIQUE")})
			}
		}
	}
	return out
}
func storageTableExists(ctx context.Context, db schemaQuery, d Dialect, table string) (bool, error) {
	var count int
	q := "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?"
	if d.Name == mysqlName {
		q = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name=?"
	}
	err := db.QueryRowContext(ctx, q, table).Scan(&count)
	return count > 0, err
}
func actualColumns(ctx context.Context, db schemaQuery, d Dialect, table string) (map[string]bool, error) {
	q := "SELECT name FROM pragma_table_info(?)"
	if d.Name == mysqlName {
		q = "SELECT column_name FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=?"
	}
	rows, err := db.QueryContext(ctx, q, table)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[string]bool{}
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			return nil, err
		}
		out[name] = true
	}
	return out, rows.Err()
}
func actualIndexes(ctx context.Context, db schemaQuery, d Dialect, table string) ([]storageIndex, error) {
	q := "SELECT l.name,l.\"unique\",i.seqno,i.name FROM pragma_index_list(?) l JOIN pragma_index_info(l.name) i ORDER BY l.name,i.seqno"
	if d.Name == mysqlName {
		q = "SELECT index_name,1-non_unique,seq_in_index,column_name FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name=? ORDER BY index_name,seq_in_index"
	}
	rows, err := db.QueryContext(ctx, q, table)
	if err != nil {
		return nil, err
	}
	var out []storageIndex
	last := ""
	for rows.Next() {
		var name, column string
		var unique, seq int
		if err = rows.Scan(&name, &unique, &seq, &column); err != nil {
			break
		}
		if name != last {
			out = append(out, storageIndex{unique: unique == 1})
			last = name
		}
		out[len(out)-1].columns = append(out[len(out)-1].columns, column)
	}
	if err == nil {
		err = rows.Err()
	}
	_ = rows.Close()
	if err != nil {
		return nil, err
	}
	// SQLite INTEGER PRIMARY KEY 是 rowid 的别名，没有独立索引。
	if d.Name == sqliteName {
		rows, err = db.QueryContext(ctx, "SELECT name FROM pragma_table_info(?) WHERE pk>0 ORDER BY pk", table)
		if err != nil {
			return nil, err
		}
		idx := storageIndex{unique: true}
		for rows.Next() {
			var name string
			if err = rows.Scan(&name); err != nil {
				break
			}
			idx.columns = append(idx.columns, name)
		}
		if err == nil {
			err = rows.Err()
		}
		_ = rows.Close()
		if err != nil {
			return nil, err
		}
		if len(idx.columns) > 0 {
			out = append(out, idx)
		}
	}
	return out, nil
}
func inspectRequirements(ctx context.Context, db schemaQuery, d Dialect, statements []string) ([]string, error) {
	var missing []string
	for _, r := range requirements(statements) {
		exists, err := storageTableExists(ctx, db, d, r.table)
		if err != nil {
			return nil, err
		}
		if !exists {
			missing = append(missing, "table:"+r.table)
			continue
		}
		columns, err := actualColumns(ctx, db, d, r.table)
		if err != nil {
			return nil, err
		}
		for _, name := range r.columns {
			if !columns[name] {
				missing = append(missing, "column:"+r.table+"."+name)
			}
		}
		indexes, err := actualIndexes(ctx, db, d, r.table)
		if err != nil {
			return nil, err
		}
		for _, want := range r.indexes {
			found := false
			for _, got := range indexes {
				if want.unique && !got.unique {
					continue
				}
				if len(got.columns) < len(want.columns) {
					continue
				}
				if strings.Join(got.columns[:len(want.columns)], ",") == strings.Join(want.columns, ",") && (!want.unique || len(got.columns) == len(want.columns)) {
					found = true
					break
				}
			}
			if !found {
				missing = append(missing, "index:"+r.table+"("+strings.Join(want.columns, ",")+")")
			}
		}
	}
	sort.Strings(missing)
	return missing, nil
}
