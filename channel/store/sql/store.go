// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Package sqlstore 是基于 database/sql 的 core.Store 实现，供 Hosted 多实例部署使用。
//
// 写事务不再竞争单一 meta 行锁：
//   - Ingest / 领取 / 结算按会话（conversations 行）加锁，不同会话完全并行；
//   - 投递按来源内的 delivery 行加锁，不同来源并行；
//   - 租约写全部带 revision/状态 CAS 条件，旧 fence 与丢失的行锁都写不进；
//   - meta 表保存载荷策略，不参与任何业务互斥。
//
// MySQL 使用 SELECT ... FOR UPDATE [SKIP LOCKED]；SQLite 依赖 DSN 的 _txlock=immediate
// 串行化写事务，行锁后缀为空。
package sqlstore

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/volcengine/ark-runtime-go/channel"
	"github.com/volcengine/ark-runtime-go/channel/internal/handles"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/codec"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/payload"
)

// errSchema 表示库中实际结构不满足当前实现要求。
var errSchema = storage.ErrSQLSchema

// Dialect 描述 SQL 方言差异。
type Dialect struct {
	Name string
	// TextType 是存放 JSON 记录的列类型。
	TextType string
	// RowLockSuffix 追加在单行锁定查询后（MySQL 用 FOR UPDATE，SQLite 为空）。
	RowLockSuffix string
	// SkipLockedSuffix 追加在批量候选查询后；不支持 SKIP LOCKED 的方言留空。
	SkipLockedSuffix string
}

const (
	sqliteName = "sqlite"
	mysqlName  = "mysql"
)

var (
	// SQLite 方言；写事务由 DSN 的 _txlock=immediate 串行化，不发 FOR UPDATE。
	SQLite = Dialect{Name: sqliteName, TextType: "TEXT"}
	// MySQL 方言；写事务按会话/来源行锁串行化，候选查询用 SKIP LOCKED 跳过被锁行。
	MySQL = Dialect{Name: mysqlName, TextType: "LONGTEXT", RowLockSuffix: " FOR UPDATE", SkipLockedSuffix: " SKIP LOCKED"}
)

// Options 指定时钟与容量限制。
type Options struct {
	// PayloadCodec 在 store 事务外保护敏感字段；nil 显式保留自托管明文兼容。
	PayloadCodec             codec.Codec
	PayloadNamespace         string
	RequirePayloadEncryption bool

	// MaxChannelOpenTurns 限制同一 Channel 跨 epoch 的未结算轮次，0 关闭。
	MaxChannelOpenTurns int
	// MaxRejectedMessages 是拒绝去重记录总预算，0 关闭持久拒绝。
	MaxRejectedMessages int
	// RejectNoticeInterval 限制同会话拒绝通知频率，0 使用一分钟。
	RejectNoticeInterval time.Duration

	Clock func() time.Time
	// MaxOpenTurns 是未结算 Turn 的容量上限，默认 1000。
	MaxOpenTurns int
	// MaxMessageBytes 是单条消息正文的字节上限，默认 32768。
	MaxMessageBytes int
	// TablePrefix 允许多个 Channel 部署共用一个库；只接受字母、数字和下划线。
	TablePrefix string
	// QueueTTL 是排队时限：Turn 入队时按 received_at + QueueTTL 写入 queue_deadline；
	// 0 表示不限制。到期队尾也可独立领取，由 Runtime 以 queue_deadline 诊断结算。
	QueueTTL time.Duration
}

// store 是 SQL 实现的 core.Store。
type store struct {
	payloadCodec             codec.Codec
	payloadReady             bool
	payloadNamespace         string
	requirePayloadEncryption bool

	db                   *sql.DB
	dialect              Dialect
	clock                func() time.Time
	maxChannelOpenTurns  int
	maxRejectedMessages  int
	rejectNoticeInterval time.Duration
	maxOpenTurns         int
	maxMessageBytes      int
	queueTTL             time.Duration
	databaseClock        bool
	turnScanOffset       atomic.Uint64
	turnStageOffsets     [3]atomic.Uint64
	deliveryScanOffset   atomic.Uint64
	remoteScanOffset     atomic.Uint64
	t                    tables
}

type tables struct {
	remoteOperations string
	meta             string
	versions         string
	conversations    string
	messages         string
	eventIDs         string
	turns            string
	controls         string
	interactions     string
	deliveries       string
	sessionSnapshots string
	sessionHistory   string
	accountQuotas    string
	botVaults        string
	ingestLocks      string
	ingestSlots      string
	rejected         string
}

var (
	_ core.Store    = (*store)(nil)
	_ core.Exporter = (*store)(nil)
)

func validPrefix(prefix string) bool {
	for _, r := range prefix {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '_' {
			return false
		}
	}
	return len(prefix) <= 31
}

func tableNames(prefix string) tables {
	return tables{
		ingestLocks: prefix + "channel_ingest_locks", ingestSlots: prefix + "channel_ingest_slots", rejected: prefix + "channel_rejected_messages",
		meta: prefix + "channel_meta", versions: prefix + "channel_binding_versions", conversations: prefix + "channel_conversations",
		messages: prefix + "channel_messages", eventIDs: prefix + "channel_event_ids", turns: prefix + "channel_turns",
		controls: prefix + "channel_controls", interactions: prefix + "channel_interactions", deliveries: prefix + "channel_deliveries",
		sessionSnapshots: prefix + "channel_session_snapshots", sessionHistory: prefix + "channel_session_history",
		accountQuotas:    prefix + "channel_account_quotas",
		remoteOperations: prefix + "channel_remote_operations",
		botVaults:        prefix + "channel_bot_vault_reservations",
	}
}

// ddl 返回当前 schema 的完整建表语句，便于审阅或交给迁移工具执行。
// 时间列统一存 UTC Unix 纳秒，0 表示未设置。
func ddl(dialect Dialect, prefix string) []string {
	t := tableNames(prefix)
	text := dialect.TextType
	// turns 的 enqueue_order 提供跨会话全局入队顺序：
	// MySQL 用 AUTO_INCREMENT 列（自增锁只在 INSERT 瞬间持有，不参与会话互斥）；
	// SQLite 写事务由 _txlock=immediate 串行，运行时在事务内取 MAX+1，列只需普通 BIGINT。
	enqueueDecl := "enqueue_order BIGINT NOT NULL DEFAULT 0"
	enqueueUnique := ""
	if dialect.Name == mysqlName {
		enqueueDecl = "enqueue_order BIGINT NOT NULL AUTO_INCREMENT UNIQUE"
	}
	turnsDDL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id VARCHAR(128) NOT NULL PRIMARY KEY, %s, seq BIGINT NOT NULL, conversation_id VARCHAR(128) NOT NULL, message_id VARCHAR(128) NOT NULL, binding_id VARCHAR(128) NOT NULL, binding_version VARCHAR(80) NOT NULL, status VARCHAR(16) NOT NULL, phase VARCHAR(16) NOT NULL, queue_head SMALLINT NOT NULL, eligible_at BIGINT NOT NULL, queue_deadline BIGINT NOT NULL DEFAULT 0, stop_requested_at BIGINT NOT NULL, created_at BIGINT NOT NULL, settled_at BIGINT NOT NULL, revision BIGINT NOT NULL, data %s NOT NULL%s)", t.turns, enqueueDecl, text, enqueueUnique)
	_ = enqueueUnique
	return append([]string{
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (k VARCHAR(64) NOT NULL PRIMARY KEY, v VARCHAR(64) NOT NULL)", t.meta),
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (binding_id VARCHAR(128) NOT NULL, version VARCHAR(80) NOT NULL, created_at BIGINT NOT NULL, data %s NOT NULL, PRIMARY KEY (binding_id, version))", t.versions, text),
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id VARCHAR(128) NOT NULL PRIMARY KEY, binding_id VARCHAR(128) NOT NULL, binding_version VARCHAR(80) NOT NULL, generation BIGINT NOT NULL, revision BIGINT NOT NULL, updated_at BIGINT NOT NULL, next_seq BIGINT NOT NULL DEFAULT 1, next_delivery_seq BIGINT NOT NULL DEFAULT 1, data %s NOT NULL)", t.conversations, text),
		fmt.Sprintf("CREATE INDEX %s_binding ON %s (binding_id)", t.conversations, t.conversations),
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id VARCHAR(128) NOT NULL PRIMARY KEY, seq BIGINT NOT NULL, binding_id VARCHAR(128) NOT NULL, conversation_id VARCHAR(128) NOT NULL, provider_message_id VARCHAR(255) NOT NULL, kind VARCHAR(16) NOT NULL, content_hash VARCHAR(64) NOT NULL, received_at BIGINT NOT NULL, data %s NOT NULL)", t.messages, text),
		fmt.Sprintf("CREATE UNIQUE INDEX %s_provider_message ON %s (binding_id, provider_message_id)", t.messages, t.messages),
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (binding_id VARCHAR(128) NOT NULL, provider_event_id VARCHAR(255) NOT NULL, message_id VARCHAR(128) NOT NULL, PRIMARY KEY (binding_id, provider_event_id))", t.eventIDs),
		fmt.Sprintf("CREATE INDEX %s_message ON %s (message_id)", t.eventIDs, t.eventIDs),
		turnsDDL,
		fmt.Sprintf("CREATE UNIQUE INDEX %s_message ON %s (message_id)", t.turns, t.turns),
		fmt.Sprintf("CREATE INDEX %s_ready ON %s (queue_head, eligible_at, enqueue_order)", t.turns, t.turns),
		fmt.Sprintf("CREATE INDEX %s_conversation ON %s (conversation_id, seq)", t.turns, t.turns),
		fmt.Sprintf("CREATE INDEX %s_settled ON %s (status, settled_at)", t.turns, t.turns),
		fmt.Sprintf("CREATE INDEX %s_queue_deadline ON %s (phase, queue_deadline, eligible_at)", t.turns, t.turns),
		fmt.Sprintf("CREATE INDEX %s_version ON %s (binding_version)", t.turns, t.turns),
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id VARCHAR(128) NOT NULL PRIMARY KEY, seq BIGINT NOT NULL, message_id VARCHAR(128) NOT NULL, conversation_id VARCHAR(128) NOT NULL, created_at BIGINT NOT NULL, data %s NOT NULL)", t.controls, text),
		fmt.Sprintf("CREATE UNIQUE INDEX %s_message ON %s (message_id)", t.controls, t.controls),
		fmt.Sprintf("CREATE INDEX %s_created ON %s (created_at)", t.controls, t.controls),
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id VARCHAR(255) NOT NULL PRIMARY KEY, turn_id VARCHAR(128) NOT NULL, status VARCHAR(16) NOT NULL, data %s NOT NULL)", t.interactions, text),
		fmt.Sprintf("CREATE INDEX %s_turn ON %s (turn_id, status)", t.interactions, t.interactions),
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id VARCHAR(128) NOT NULL PRIMARY KEY, source_kind VARCHAR(16) NOT NULL, source_id VARCHAR(128) NOT NULL, seq INTEGER NOT NULL, binding_id VARCHAR(128) NOT NULL, uuid VARCHAR(64) NOT NULL, status VARCHAR(16) NOT NULL, queue_head SMALLINT NOT NULL, eligible_at BIGINT NOT NULL, conversation_id VARCHAR(128) NOT NULL DEFAULT '', inbox_seq BIGINT NOT NULL DEFAULT 0, data %s NOT NULL)", t.deliveries, text),
		fmt.Sprintf("CREATE UNIQUE INDEX %s_uuid ON %s (uuid)", t.deliveries, t.deliveries),
		fmt.Sprintf("CREATE INDEX %s_ready ON %s (queue_head, eligible_at, inbox_seq, id)", t.deliveries, t.deliveries),
		fmt.Sprintf("CREATE INDEX %s_source ON %s (source_kind, source_id, seq)", t.deliveries, t.deliveries),
		fmt.Sprintf("CREATE INDEX %s_status ON %s (status)", t.deliveries, t.deliveries),
		// 双层快照、账号额度。
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id VARCHAR(128) NOT NULL PRIMARY KEY, route VARCHAR(128) NOT NULL, generation BIGINT NOT NULL, owner VARCHAR(255) NOT NULL, creation_key VARCHAR(128) NOT NULL, request_hash VARCHAR(80) NOT NULL, session_id VARCHAR(128) NOT NULL DEFAULT '', epoch BIGINT NOT NULL DEFAULT 0, created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL, data %s NOT NULL, %s)", t.sessionSnapshots, text, uniqueClause(dialect, "uk_snapshot_generation", "route", "generation")+", "+uniqueClause(dialect, "uk_snapshot_creation", "owner", "creation_key")),
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id VARCHAR(128) NOT NULL PRIMARY KEY, route VARCHAR(128) NOT NULL, generation BIGINT NOT NULL, snapshot_id VARCHAR(128) NOT NULL, session_id VARCHAR(128) NOT NULL DEFAULT '', created_at BIGINT NOT NULL, data %s NOT NULL, %s)", t.sessionHistory, text, uniqueClause(dialect, "uk_history_generation", "route", "generation")),
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (owner VARCHAR(255) NOT NULL PRIMARY KEY, open_count BIGINT NOT NULL DEFAULT 0, quota BIGINT NOT NULL DEFAULT 0, creation_count BIGINT NOT NULL DEFAULT 0, creation_limit BIGINT NOT NULL DEFAULT 0, execution_count BIGINT NOT NULL DEFAULT 0, execution_limit BIGINT NOT NULL DEFAULT 0, revision BIGINT NOT NULL DEFAULT 0, updated_at BIGINT NOT NULL, data %s NOT NULL)", t.accountQuotas, text),
		// per-principal Bot Vault 持久创建协调（reservation/fence），不含密钥。
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (principal VARCHAR(255) NOT NULL, name VARCHAR(128) NOT NULL, state VARCHAR(16) NOT NULL, fence_owner VARCHAR(128) NOT NULL DEFAULT '', fence BIGINT NOT NULL DEFAULT 0, lease_expires_at BIGINT NOT NULL DEFAULT 0, vault_id VARCHAR(128) NOT NULL DEFAULT '', created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL, data %s NOT NULL, PRIMARY KEY (principal, name))", t.botVaults, text),
	}, append(budgetDDL(dialect, prefix), append(remoteDDL(dialect, prefix), payloadDDL(dialect, prefix)...)...)...)
}

// uniqueClause 返回各方言可接受的唯一约束：MySQL 支持命名 UNIQUE KEY，SQLite 只支持 UNIQUE(...)。
func uniqueClause(dialect Dialect, name string, columns ...string) string {
	list := strings.Join(columns, ", ")
	if dialect.Name == mysqlName {
		return "UNIQUE KEY " + name + " (" + list + ")"
	}
	return "UNIQUE (" + list + ")"
}

// open 只读检查实际存储依赖和载荷策略，不创建表或更新载荷策略。
// db 的生命周期由 store.Close 管理；新装须显式调用 Install。
func open(ctx context.Context, db *sql.DB, dialect Dialect, options Options) (*store, error) {
	if options.RequirePayloadEncryption && options.PayloadCodec == nil {
		return nil, core.ErrInvalid
	}
	s, err := configuredStore(db, dialect, options)
	if err != nil {
		return nil, err
	}
	report, err := inspect(ctx, db, dialect, options)
	if err != nil {
		return nil, err
	}
	if !report.Ready {
		return nil, openReportError(report, options)
	}
	s.payloadReady = s.payloadCodec != nil
	return s, nil
}

func isAlreadyExists(err error) bool {
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "already exists") || strings.Contains(text, "duplicate")
}

func isDuplicateRecord(err error) bool {
	return err != nil && (isAlreadyExists(err) || strings.Contains(strings.ToLower(err.Error()), "unique constraint failed"))
}

type transactionClockKey struct{}
type transactionClock struct {
	tx  *sql.Tx
	err error
}

func (s *store) now(ctx context.Context) time.Time {
	if clock, ok := ctx.Value(transactionClockKey{}).(*transactionClock); ok {
		if clock.err != nil {
			return time.Time{}
		}
		var micros int64
		clock.err = clock.tx.QueryRowContext(ctx, "SELECT CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(6))*1000000 AS UNSIGNED)").Scan(&micros)
		return time.UnixMicro(micros).UTC()
	}
	return s.clock().UTC()
}

func nanos(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

// runTx 开启一个业务事务；调用方按需锁定会话行/投递行，不存在全局锁。
func (s *store) runTxOnce(ctx context.Context, fn func(context.Context, *sql.Tx) error) (err error) {
	defer func() {
		// 自动回滚可能先于语句或 Commit 观察到取消，保持调用者的取消/超时语义。
		if errors.Is(err, sql.ErrTxDone) && ctx.Err() != nil {
			err = ctx.Err()
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	var txOptions *sql.TxOptions
	if s.dialect.Name == mysqlName {
		txOptions = &sql.TxOptions{Isolation: sql.LevelReadCommitted}
	}
	tx, err := s.db.BeginTx(ctx, txOptions)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	clock := &transactionClock{tx: tx}
	if s.databaseClock {
		ctx = context.WithValue(ctx, transactionClockKey{}, clock)
	}
	err = fn(ctx, tx)
	if err == nil {
		if ps, _ := ctx.Value(payloadSessionKey{}).(*payload.Session); ps != nil && ps.Pending() {
			err = payload.ErrPrepare
		}
	}
	if clock.err != nil {
		return clock.err
	}
	if err != nil {
		return err
	}
	return tx.Commit()
}

// lockConversation 锁定一个会话行。skipLocked 为 true 时（领取），MySQL 直接跳过被其他
// 事务持有的会话；为 false 时（入站/结算）等待行锁，保证同会话写入严格串行。
// 会话不存在返回 core.ErrNotFound；MySQL SKIP LOCKED 未取到锁也返回 core.ErrNotFound。
func (s *store) lockConversation(ctx context.Context, tx *sql.Tx, id core.RouteKey, skipLocked bool) error {
	suffix := s.dialect.RowLockSuffix
	if skipLocked && s.dialect.SkipLockedSuffix == "" {
		suffix = ""
	}
	if skipLocked {
		suffix += s.dialect.SkipLockedSuffix
	}
	var locked string
	query := "SELECT id FROM " + s.t.conversations + " WHERE id = ?" + suffix
	err := tx.QueryRowContext(ctx, query, string(id)).Scan(&locked)
	if errors.Is(err, sql.ErrNoRows) {
		return core.ErrNotFound
	}
	return err
}

// allocConversationSeq 必须在会话行被当前事务持有后调用：原子分配一个会话内递增序号。
func (s *store) allocConversationSeq(ctx context.Context, tx *sql.Tx, id core.RouteKey) (uint64, error) {
	if _, err := tx.ExecContext(ctx, "UPDATE "+s.t.conversations+" SET next_seq = next_seq + 1 WHERE id = ?", string(id)); err != nil {
		return 0, err
	}
	var seq uint64
	if err := tx.QueryRowContext(ctx, "SELECT next_seq FROM "+s.t.conversations+" WHERE id = ?", string(id)).Scan(&seq); err != nil {
		return 0, err
	}
	return seq, nil
}

// randomID 生成按时间字典序排列的实体 ID：前缀 + 20 位纳秒时间 + 8 位随机十六进制。
// 列表接口按 ID 升序即按创建时间升序，无需全局序号。
func rawRandomID(prefix string, now time.Time) (string, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s%020d%s", prefix, uint64(now.UnixNano()), hex.EncodeToString(b[:])), nil
}

func encode(value any) (string, error) {
	data, err := core.EncodeStored(value)
	return string(data), err
}

// mustEncode 只用于入站事务中已构造好的内存对象，理论上不会序列化失败。
func mustEncode(value any) string {
	data, err := core.EncodeStored(value)
	if err != nil {
		panic(err)
	}
	return string(data)
}

type queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// storedRowMetadata 将可信 SQL 行 ID 与排序列和原始 JSON 一起读取。
type storedRowMetadata struct {
	data    string
	id      string
	order   uint64
	hasID   bool
	ordered bool
}

func recordProjection[T any](query string) (string, storedRowMetadata, error) {
	metadata := storedRowMetadata{}
	var value T
	switch any(&value).(type) {
	case *core.Turn:
		metadata.hasID = true
		metadata.ordered = true
	case *core.Delivery, *core.RemoteOperation:
		metadata.hasID = true
	default:
		return query, metadata, nil
	}
	const prefix = "SELECT data FROM "
	if !strings.HasPrefix(query, prefix) {
		return "", metadata, core.ErrInvalid
	}
	columns := "data, id"
	if metadata.ordered {
		columns += ", enqueue_order"
	}
	return "SELECT " + columns + " FROM " + strings.TrimPrefix(query, prefix), metadata, nil
}
func (m *storedRowMetadata) destinations() []any {
	dest := []any{&m.data}
	if m.hasID {
		dest = append(dest, &m.id)
	}
	if m.ordered {
		dest = append(dest, &m.order)
	}
	return dest
}
func (m storedRowMetadata) decode(value any) error {
	err := core.DecodeStored([]byte(m.data), value)
	if err != nil {
		return err
	}
	// opaque 视图不信任原文 ID；实际 row ID 保证分页仍可继续，编码仍逐字保留原文。
	switch v := value.(type) {
	case *core.Turn:
		if v.IsOpaqueStored() {
			v.ID = m.id
		}
		if m.ordered {
			v.EnqueueOrder = m.order
		}
	case *core.Delivery:
		if v.IsOpaqueStored() {
			v.ID = m.id
		}
	case *core.RemoteOperation:
		if v.IsOpaqueStored() {
			v.ID = m.id
		}
	}
	return nil
}
func loadOne[T any](ctx context.Context, q queryer, query string, args ...any) (*T, error) {
	query, metadata, err := recordProjection[T](query)
	if err != nil {
		return nil, err
	}
	err = q.QueryRowContext(ctx, query, args...).Scan(metadata.destinations()...)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var value T
	if err = metadata.decode(&value); err != nil {
		return nil, err
	}
	return &value, nil
}
func loadMany[T any](ctx context.Context, q queryer, query string, args ...any) ([]T, error) {
	query, projection, err := recordProjection[T](query)
	if err != nil {
		return nil, err
	}
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []T
	for rows.Next() {
		metadata := projection
		if err = rows.Scan(metadata.destinations()...); err != nil {
			return nil, err
		}
		var value T
		if err = metadata.decode(&value); err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

func loadIDs(ctx context.Context, q queryer, query string, args ...any) ([]string, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *store) turn(ctx context.Context, q queryer, id string) (*core.Turn, error) {
	return loadOne[core.Turn](ctx, q, "SELECT data FROM "+s.t.turns+" WHERE id = ?", id)
}

// lockedTurn 仅供已按account→Conversation顺序取得前置锁的写事务，或只写Turn的事务使用。
func (s *store) lockedTurn(ctx context.Context, tx *sql.Tx, id string) (*core.Turn, error) {
	return loadOne[core.Turn](ctx, tx, "SELECT data FROM "+s.t.turns+" WHERE id = ?"+s.dialect.RowLockSuffix, id)
}

func (s *store) putTurn(ctx context.Context, tx *sql.Tx, turn core.Turn, insert bool, head bool) error {
	if turn.IsOpaqueStored() {
		return core.ErrUnsupportedWork
	}
	data, err := s.protectedData(ctx, tx, turn, core.Binding{ID: turn.BindingID})
	if err != nil {
		return err
	}
	settled := int64(0)
	if turn.Outcome != nil {
		settled = nanos(turn.Outcome.SettledAt)
	}
	eligible := nanos(turn.EligibleAt(s.now(ctx)))
	if insert {
		if s.dialect.Name == mysqlName {
			// enqueue_order 为 AUTO_INCREMENT：用 LastInsertId 取回本次插入分配的值。
			result, execErr := tx.ExecContext(ctx, "INSERT INTO "+s.t.turns+" (id, seq, conversation_id, message_id, binding_id, binding_version, status, phase, queue_head, eligible_at, queue_deadline, stop_requested_at, created_at, settled_at, revision, data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
				turn.ID, turn.Seq, string(turn.ConversationID), turn.MessageID, turn.BindingID, turn.BindingVersion, string(turn.Status), string(turn.Phase), boolInt(head), eligible, nanos(turn.QueueDeadlineAt), nanos(turn.StopRequestedAt), nanos(turn.CreatedAt), settled, turn.Revision, data)
			if execErr != nil {
				return execErr
			}
			order, lerr := result.LastInsertId()
			if lerr != nil {
				return lerr
			}
			turn.EnqueueOrder = uint64(order)
			return nil
		}
		// SQLite：写事务已串行，MAX+1 分配全局入队顺序。
		order, err := s.maxEnqueueOrder(ctx, tx)
		if err != nil {
			return err
		}
		turn.EnqueueOrder = order
		data, derr := s.protectedData(ctx, tx, turn, core.Binding{ID: turn.BindingID})
		if derr != nil {
			return derr
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO "+s.t.turns+" (id, enqueue_order, seq, conversation_id, message_id, binding_id, binding_version, status, phase, queue_head, eligible_at, queue_deadline, stop_requested_at, created_at, settled_at, revision, data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			turn.ID, order, turn.Seq, string(turn.ConversationID), turn.MessageID, turn.BindingID, turn.BindingVersion, string(turn.Status), string(turn.Phase), boolInt(head), eligible, nanos(turn.QueueDeadlineAt), nanos(turn.StopRequestedAt), nanos(turn.CreatedAt), settled, turn.Revision, data)
		return err
	}
	result, err := tx.ExecContext(ctx, "UPDATE "+s.t.turns+" SET status = ?, phase = ?, eligible_at = ?, stop_requested_at = ?, settled_at = ?, revision = ?, data = ? WHERE id = ? AND revision = ?",
		string(turn.Status), string(turn.Phase), eligible, nanos(turn.StopRequestedAt), settled, turn.Revision, data, turn.ID, turn.Revision-1)
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n == 0 {
		return core.ErrLeaseLost
	}
	if !turn.Status.Open() {
		_, err = tx.ExecContext(ctx, "DELETE FROM "+s.t.ingestSlots+" WHERE message_id=?", turn.MessageID)
		if err != nil {
			return err
		}
	}
	return nil
}

// maxEnqueueOrder 返回下一全局入队序号。MySQL 也可用它读取 AUTO_INCREMENT 当前最大值
// （同一连接/事务内 LAST_INSERT_ID 只对单行；MAX 更稳妥）。
func (s *store) maxEnqueueOrder(ctx context.Context, tx *sql.Tx) (uint64, error) {
	var order uint64
	if err := tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(enqueue_order), 0) FROM "+s.t.turns).Scan(&order); err != nil {
		return 0, err
	}
	return order + 1, nil
}

// leasedTurn 在当前事务内读取 lease 当前有效的 Turn（调用方应已持有会话行锁）。
func (s *store) leasedTurn(ctx context.Context, tx *sql.Tx, lease core.Lease) (core.Turn, error) {
	turn, err := s.lockedTurn(ctx, tx, lease.ID)
	if err != nil {
		return core.Turn{}, err
	}
	if turn == nil || !turn.Holds(lease, s.now(ctx)) {
		return core.Turn{}, core.ErrLeaseLost
	}
	return *turn, nil
}

// refreshTurnHead 让会话中 seq 最小的未结算 Turn 成为唯一队首；必须在会话行锁内调用。
func (s *store) refreshTurnHead(ctx context.Context, tx *sql.Tx, conversation core.RouteKey) error {
	if _, err := tx.ExecContext(ctx, "UPDATE "+s.t.turns+" SET queue_head = 0 WHERE conversation_id = ? AND queue_head = 1", string(conversation)); err != nil {
		return err
	}
	ids, err := loadIDs(ctx, tx, "SELECT id FROM "+s.t.turns+" WHERE conversation_id = ? AND status IN ('queued', 'running', 'waiting') ORDER BY seq LIMIT 1", string(conversation))
	if err != nil || len(ids) == 0 {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE "+s.t.turns+" SET queue_head = 1 WHERE id = ?", ids[0])
	return err
}

func (s *store) delivery(ctx context.Context, q queryer, id string) (*core.Delivery, error) {
	return loadOne[core.Delivery](ctx, q, "SELECT data FROM "+s.t.deliveries+" WHERE id = ?", id)
}

// lockedDelivery 在续租/完成/重试事务中锁定当前行，避免旧状态覆盖终态或新fence。
func (s *store) lockedDelivery(ctx context.Context, tx *sql.Tx, id string) (*core.Delivery, error) {
	return loadOne[core.Delivery](ctx, tx, "SELECT data FROM "+s.t.deliveries+" WHERE id = ?"+s.dialect.RowLockSuffix, id)
}

func (s *store) putDelivery(ctx context.Context, tx *sql.Tx, delivery core.Delivery, insert bool) error {
	if delivery.IsOpaqueStored() {
		return core.ErrUnsupportedWork
	}
	binding := core.Binding{ID: delivery.BindingID}
	if s.payloadCodec != nil {
		var err error
		binding, err = s.payloadSourceBinding(ctx, tx, delivery)
		if err != nil {
			return err
		}
	}
	data, err := s.protectedData(ctx, tx, delivery, binding)
	if err != nil {
		return err
	}
	eligible := nanos(delivery.EligibleAt())
	if insert {
		_, err = tx.ExecContext(ctx, "INSERT INTO "+s.t.deliveries+" (id, source_kind, source_id, seq, binding_id, uuid, status, queue_head, eligible_at, conversation_id, inbox_seq, data) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)",
			delivery.ID, string(delivery.Source.Kind), delivery.Source.ID, delivery.Seq, delivery.BindingID, delivery.UUID, string(delivery.Status), eligible, string(delivery.ConversationID), delivery.InboxSeq, data)
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE "+s.t.deliveries+" SET status = ?, eligible_at = ?, data = ? WHERE id = ?", string(delivery.Status), eligible, data, delivery.ID)
	return err
}

// refreshDeliveryHead 让来源中 Seq 最小的未终态投递成为唯一队首。
// 调用方必须已锁定候选行（SELECT ... FOR UPDATE），避免与另一投递 worker 的领取竞争。
func (s *store) refreshDeliveryHead(ctx context.Context, tx *sql.Tx, source core.DeliverySource) error {
	if _, err := tx.ExecContext(ctx, "UPDATE "+s.t.deliveries+" SET queue_head = 0 WHERE source_kind = ? AND source_id = ? AND queue_head = 1", string(source.Kind), source.ID); err != nil {
		return err
	}
	ids, err := loadIDs(ctx, tx, "SELECT id FROM "+s.t.deliveries+" WHERE source_kind = ? AND source_id = ? AND status IN ('pending', 'uncertain') ORDER BY seq LIMIT 1"+s.dialect.RowLockSuffix, string(source.Kind), source.ID)
	if err != nil || len(ids) == 0 {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE "+s.t.deliveries+" SET queue_head = 1 WHERE id = ?", ids[0])
	return err
}

// allocInboxSeq 为新投递分配会话内投递序号：调用方必须已持有对应会话行锁。
// 投递序号独立于消息/控制命令序号，只用于跨来源的全局领取顺序比较。
func (s *store) allocInboxSeq(ctx context.Context, tx *sql.Tx, conversation core.RouteKey) (uint64, error) {
	if _, err := tx.ExecContext(ctx, "UPDATE "+s.t.conversations+" SET next_delivery_seq = next_delivery_seq + 1 WHERE id = ?", string(conversation)); err != nil {
		return 0, err
	}
	var seq uint64
	if err := tx.QueryRowContext(ctx, "SELECT next_delivery_seq FROM "+s.t.conversations+" WHERE id = ?", string(conversation)).Scan(&seq); err != nil {
		return 0, err
	}
	return seq, nil
}

// insertDeliveries 写入同一来源的新投递并刷新队首。
func (s *store) insertDeliveries(ctx context.Context, tx *sql.Tx, source core.DeliverySource, conversation core.RouteKey, deliveries []core.Delivery) error {
	if len(deliveries) == 0 {
		return nil
	}
	now := s.now(ctx)
	seen := map[string]bool{}
	for _, delivery := range deliveries {
		if !delivery.ValidNew(source) || seen[delivery.ID] || seen["uuid:"+delivery.UUID] {
			return core.ErrConflict
		}
		seen[delivery.ID], seen["uuid:"+delivery.UUID] = true, true
		existing, err := s.delivery(ctx, tx, delivery.ID)
		if err != nil {
			return err
		}
		var count int
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+s.t.deliveries+" WHERE uuid = ?", delivery.UUID).Scan(&count); err != nil {
			return err
		}
		if existing != nil || count > 0 {
			return core.ErrConflict
		}
		delivery.Payloads = nil
		delivery.CreatedAt = now
		delivery.Lease = core.Lease{ID: delivery.ID}
		if conversation != "" {
			seq, err := s.allocInboxSeq(ctx, tx, conversation)
			if err != nil {
				return err
			}
			// 每条只使用本次分配的序号，避免批内偏移超过已预留范围。
			delivery.ConversationID = string(conversation)
			delivery.InboxSeq = seq
		}
		if err := s.putDelivery(ctx, tx, delivery, true); err != nil {
			return err
		}
	}
	// 与插入的投递同源；锁住旧队首（若存在）后再重算，避免与 LeaseDelivery 竞争。
	var lockedHead string
	if err := tx.QueryRowContext(ctx, "SELECT id FROM "+s.t.deliveries+" WHERE source_kind = ? AND source_id = ? AND queue_head = 1 ORDER BY seq LIMIT 1"+s.dialect.RowLockSuffix, string(source.Kind), source.ID).Scan(&lockedHead); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	return s.refreshDeliveryHead(ctx, tx, source)
}

func (s *store) conversation(ctx context.Context, q queryer, id core.RouteKey) (*core.Conversation, error) {
	return loadOne[core.Conversation](ctx, q, "SELECT data FROM "+s.t.conversations+" WHERE id = ?", string(id))
}

func (s *store) putConversation(ctx context.Context, tx *sql.Tx, conversation core.Conversation, insert bool) error {
	data, err := encode(conversation)
	if err != nil {
		return err
	}
	if insert {
		_, err = tx.ExecContext(ctx, "INSERT INTO "+s.t.conversations+" (id, binding_id, binding_version, generation, revision, updated_at, next_seq, next_delivery_seq, data) VALUES (?, ?, ?, ?, ?, ?, 1, 1, ?)",
			string(conversation.ID), conversation.BindingID, conversation.Session.BindingVersion, conversation.Session.Generation, conversation.Revision, nanos(conversation.UpdatedAt), data)
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE "+s.t.conversations+" SET binding_version = ?, generation = ?, revision = ?, updated_at = ?, data = ? WHERE id = ?",
		conversation.Session.BindingVersion, conversation.Session.Generation, conversation.Revision, nanos(conversation.UpdatedAt), data, string(conversation.ID))
	return err
}

func (s *store) interaction(ctx context.Context, q queryer, id string) (*core.Interaction, error) {
	return loadOne[core.Interaction](ctx, q, "SELECT data FROM "+s.t.interactions+" WHERE id = ?", id)
}

func (s *store) putInteraction(ctx context.Context, tx *sql.Tx, interaction core.Interaction, insert bool) error {
	var binding core.Binding
	if s.payloadCodec != nil {
		var err error
		binding, err = s.payloadSourceBinding(ctx, tx, interaction)
		if err != nil {
			return err
		}
	}
	data, err := s.protectedData(ctx, tx, interaction, binding)
	if err != nil {
		return err
	}
	if insert {
		_, err = tx.ExecContext(ctx, "INSERT INTO "+s.t.interactions+" (id, turn_id, status, data) VALUES (?, ?, ?, ?)", interaction.ID, interaction.TurnID, string(interaction.Status), data)
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE "+s.t.interactions+" SET status = ?, data = ? WHERE id = ?", string(interaction.Status), data, interaction.ID)
	return err
}

// ---------------------------------------------------------------------------
// IngestStore

// Ingest 在一个事务内完成去重、配置版本登记、会话创建、消息写入，以及 Turn 入队或控制命令生效。
// 互斥粒度是会话行：同一会话串行，不同会话并行。
func (s *store) Ingest(ctx context.Context, version core.BindingVersion, message core.Message) (core.IngestResult, error) {
	var result core.IngestResult
	if !version.Valid() || core.ValidateMessage(version.Content, message, s.maxMessageBytes) != nil {
		return result, core.ErrInvalid
	}
	err := s.runTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		result = core.IngestResult{}
		now := s.now(ctx)
		if !message.AdmitUntil.IsZero() && !now.Before(message.AdmitUntil) {
			return core.ErrAdmissionExpired
		}
		if err := s.lockIngestChannel(ctx, tx, version.Content); err != nil {
			return err
		}
		if message.RecalledMessageID != "" {
			original, err := loadOne[core.InboundMessage](ctx, tx, "SELECT data FROM "+s.t.messages+" WHERE binding_id = ? AND provider_message_id = ?", message.BindingID, message.RecalledMessageID)
			if err != nil {
				return err
			}
			if original == nil {
				result.Ignored = true
				return nil
			}
			rejected, err := s.rejectedResult(ctx, tx, *original)
			if err != nil {
				return err
			}
			if rejected != nil {
				result.Ignored = true
				return nil
			}
			message, err = core.ResolveRecall(message, *original)
			if err != nil {
				return err
			}
		}
		inbound := core.NewInboundMessage(message, now)
		inbound.BindingVersion = version.Version
		// 去重检查不依赖会话锁，唯一索引是最终防线。
		existing, err := loadOne[core.InboundMessage](ctx, tx, "SELECT data FROM "+s.t.messages+" WHERE binding_id = ? AND provider_message_id = ?", inbound.BindingID, inbound.ProviderMessageID)
		if err != nil {
			return err
		}
		aliases, err := loadIDs(ctx, tx, "SELECT message_id FROM "+s.t.eventIDs+" WHERE binding_id = ? AND provider_event_id = ?", inbound.BindingID, inbound.ProviderEventID)
		if err != nil {
			return err
		}
		if existing != nil {
			if existing.ContentHash != inbound.ContentHash || len(aliases) > 0 && aliases[0] != existing.ID {
				return core.ErrConflict
			}
			if len(aliases) == 0 {
				r, err := s.rejectedResult(ctx, tx, *existing)
				if err != nil {
					return err
				}
				if r != nil {
					if err = s.reserveRejection(ctx, tx, core.RejectedMessage{MessageID: existing.ID, ControlID: r.Control.ID, DeliveryID: r.Reply.ID, CreatedAt: nanos(now)}); err != nil {
						return err
					}
				}
				if _, err := tx.ExecContext(ctx, "INSERT INTO "+s.t.eventIDs+" (binding_id, provider_event_id, message_id) VALUES (?, ?, ?)", inbound.BindingID, inbound.ProviderEventID, existing.ID); err != nil {
					return err
				}
			}
			result, err = s.existingResult(ctx, tx, *existing)
			return err
		}
		if len(aliases) > 0 {
			return core.ErrConflict
		}
		if err := s.ensureVersion(ctx, tx, version, now); err != nil {
			return err
		}
		if err := s.ensureConversation(ctx, tx, message, now); err != nil {
			return err
		}
		// 此后所有写入按会话串行：锁定会话行（MySQL FOR UPDATE，SQLite 由 immediate 事务串行）。
		if err := s.lockConversation(ctx, tx, inbound.ConversationID, false); err != nil {
			if errors.Is(err, core.ErrNotFound) {
				return core.ErrConflict
			}
			return err
		}
		if !message.AdmitUntil.IsZero() && !s.now(ctx).Before(message.AdmitUntil) {
			return core.ErrAdmissionExpired
		}
		reject := false
		if inbound.Kind == core.MessageUser {
			var err error
			reject, err = s.channelAtCapacity(ctx, tx, version.Content)
			if err != nil {
				return err
			}
		}
		seq, err := s.allocConversationSeq(ctx, tx, inbound.ConversationID)
		if err != nil {
			return err
		}
		inbound.Seq = seq
		inbound.ID, err = s.randomID(ctx, "msg:", now)
		if err != nil {
			return err
		}
		data, err := s.protectedData(ctx, tx, inbound, version.Content)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO "+s.t.messages+" (id, seq, binding_id, conversation_id, provider_message_id, kind, content_hash, received_at, data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
			inbound.ID, seq, inbound.BindingID, string(inbound.ConversationID), inbound.ProviderMessageID, string(inbound.Kind), inbound.ContentHash, nanos(inbound.ReceivedAt), data); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO "+s.t.eventIDs+" (binding_id, provider_event_id, message_id) VALUES (?, ?, ?)", inbound.BindingID, inbound.ProviderEventID, inbound.ID); err != nil {
			return err
		}
		if s.payloadCodec != nil {
			var meta core.InboundMessage
			if err = json.Unmarshal([]byte(data), &meta); err != nil {
				return err
			}
			inbound = meta
		}
		result.Message = inbound
		if inbound.Kind == core.MessageUser {
			if !reject {
				err := s.reserveIngestSlot(ctx, tx, s.t.ingestSlots, s.maxOpenTurns, "message_id", []any{inbound.ID})
				if err != nil {
					if !errors.Is(err, core.ErrQueueFull) {
						return err
					}
					reject = true
				}
			}
			if reject {
				return s.persistRejected(ctx, tx, inbound, version, now, &result)
			}
			turn, err := s.enqueueTurn(ctx, tx, inbound, version, seq, now)
			if message.TrackReactions {
				reaction := core.NewMessageReactionDelivery(turn, message)
				if err := s.insertDeliveries(ctx, tx, reaction.Source, "", []core.Delivery{reaction}); err != nil {
					return err
				}
			}
			result.Turn = &turn
			return err
		}
		return s.applyControl(ctx, tx, inbound, version, seq, now, &result)
	})
	if err == nil && s.payloadCodec != nil {
		err = s.hydrateIngest(ctx, &result)
	}
	return result, err
}

func (s *store) existingResult(ctx context.Context, tx *sql.Tx, message core.InboundMessage) (core.IngestResult, error) {
	result := core.IngestResult{Message: message, Duplicate: true}
	r, err := s.rejectedResult(ctx, tx, message)
	if err != nil {
		return result, err
	}
	if r != nil {
		return *r, nil
	}
	if message.Kind == core.MessageUser {
		turn, err := loadOne[core.Turn](ctx, tx, "SELECT data FROM "+s.t.turns+" WHERE message_id = ?", message.ID)
		result.Turn = turn
		return result, err
	}
	control, err := loadOne[core.ControlRequest](ctx, tx, "SELECT data FROM "+s.t.controls+" WHERE message_id = ?", message.ID)
	if err != nil || control == nil {
		return result, err
	}
	result.Control = control
	if control.TargetTurnID != "" {
		result.Stopped, err = s.turn(ctx, tx, control.TargetTurnID)
	}
	return result, err
}

func (s *store) ensureVersion(ctx context.Context, tx *sql.Tx, version core.BindingVersion, now time.Time) error {
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+s.t.versions+" WHERE binding_id = ? AND version = ?", version.BindingID, version.Version).Scan(&count); err != nil || count > 0 {
		return err
	}
	version.CreatedAt = now
	data, err := encode(version)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO "+s.t.versions+" (binding_id, version, created_at, data) VALUES (?, ?, ?, ?)", version.BindingID, version.Version, nanos(now), data)
	return err
}

// ensureConversation 幂等创建会话；并发首条消息竞争唯一键时后到者复用已有行。
func (s *store) ensureConversation(ctx context.Context, tx *sql.Tx, message core.Message, now time.Time) error {
	id := core.RouteFor(message)
	existing, err := s.conversation(ctx, tx, id)
	if err != nil || existing != nil {
		return err
	}
	conversation := core.Conversation{ID: id, BindingID: message.BindingID, Scope: core.ConversationScope{TenantKey: message.TenantKey, ChatType: message.ChatType, ChatID: message.ChatID, ThreadID: message.ThreadID}, Session: core.SessionBinding{State: core.SessionNone}, UpdatedAt: now, Revision: 1}
	err = s.putConversation(ctx, tx, conversation, true)
	if err != nil && isAlreadyExists(err) {
		return nil
	}
	return err
}

func (s *store) enqueueTurn(ctx context.Context, tx *sql.Tx, message core.InboundMessage, version core.BindingVersion, seq uint64, now time.Time) (core.Turn, error) {
	id, err := s.randomID(ctx, "turn:", now)
	if err != nil {
		return core.Turn{}, err
	}
	turn := core.Turn{WorkKind: core.InboundWorkKind(version.Content, message.Message.ChatType), Snapshot: core.NewTurnSnapshot(version.Content, message.Message.AdmitUntil), ID: id, Seq: seq, ConversationID: message.ConversationID, MessageID: message.ID, BindingID: version.BindingID, BindingVersion: version.Version, InputEventID: core.InputEventIDFor(message.ContentHash), CreatedAt: now, Status: core.TurnQueued, Lease: core.Lease{ID: id}, Revision: 1}
	if s.queueTTL > 0 {
		turn.QueueDeadlineAt = message.ReceivedAt.Add(s.queueTTL)
	}
	var open int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+s.t.turns+" WHERE conversation_id = ? AND status IN ('queued', 'running', 'waiting')", string(turn.ConversationID)).Scan(&open); err != nil {
		return turn, err
	}
	if err = s.putTurn(ctx, tx, turn, true, open == 0); err != nil {
		return turn, err
	}
	stored, err := s.turn(ctx, tx, turn.ID)
	if err != nil {
		return core.Turn{}, err
	}
	if stored == nil {
		return core.Turn{}, core.ErrConflict
	}
	return *stored, nil
}

// applyControl 让控制命令在入站事务（持有会话锁）内生效：命中会话中最早的未结算 Turn 时
// 登记停止请求，否则同一事务写入“当前没有进行中的任务。”的回复。
func (s *store) applyControl(ctx context.Context, tx *sql.Tx, message core.InboundMessage, version core.BindingVersion, seq uint64, now time.Time, result *core.IngestResult) error {
	id, err := s.randomID(ctx, "ctl:", now)
	if err != nil {
		return err
	}
	control := core.ControlRequest{ID: id, Seq: seq, MessageID: message.ID, ConversationID: message.ConversationID, Kind: core.ControlStop, CreatedAt: now}
	if message.Message.Unsupported {
		control.Kind = core.ControlUnsupportedMessage
	}
	if control.Kind == core.ControlStop {
		targets, err := loadMany[core.Turn](ctx, tx, "SELECT data FROM "+s.t.turns+" WHERE conversation_id = ? AND seq < ? AND status IN ('queued', 'running', 'waiting') ORDER BY seq LIMIT 1"+s.dialect.RowLockSuffix, string(message.ConversationID), seq)
		if err != nil {
			return err
		}
		if len(targets) > 0 {
			target := targets[0].RequestStop(now)
			if message.Message.RecalledMessageID != "" {
				replyTarget := core.ReplyTargetFor(message.Message, version.Content)
				target.StopReplyTarget = &replyTarget
			}
			// 停止请求可由入站无租约写入：按 ID 直接更新，CAS 旧 revision。
			oldRevision := targets[0].Revision
			if _, err := tx.ExecContext(ctx, "UPDATE "+s.t.turns+" SET stop_requested_at = ?, eligible_at = ?, revision = ?, data = ? WHERE id = ? AND revision = ?",
				nanos(target.StopRequestedAt), nanos(target.EligibleAt(now)), target.Revision, mustEncode(target), target.ID, oldRevision); err != nil {
				return err
			}
			control.TargetTurnID = target.ID
			result.Stopped = &target
		}
	}
	data, err := encode(control)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO "+s.t.controls+" (id, seq, message_id, conversation_id, created_at, data) VALUES (?, ?, ?, ?, ?, ?)", control.ID, seq, message.ID, string(message.ConversationID), nanos(now), data); err != nil {
		return err
	}
	result.Control = &control
	if control.TargetTurnID != "" {
		return nil
	}
	if core.ReplyTargetFor(message.Message, version.Content).Mode == "none" {
		return nil
	}
	text := core.ReplyNoActiveTurn
	if control.Kind == core.ControlUnsupportedMessage {
		text = core.ReplyUnsupportedMessage
	}
	reply := core.NewControlReply(control, message, version.Content, text)
	reply.Snapshot = core.NewTurnSnapshot(version.Content, message.Message.AdmitUntil)
	if err := s.insertDeliveries(ctx, tx, reply.Source, message.ConversationID, []core.Delivery{reply}); err != nil {
		return err
	}
	stored, err := s.delivery(ctx, tx, reply.ID)
	result.Reply = stored
	return err
}

// ---------------------------------------------------------------------------
// TurnStore

// leaseCandidateLimit 是单轮领取扫描的候选会话上限，被锁住的候选会被跳过。
const leaseCandidateLimit = 32

// LeaseTurn 领取一个可执行的会话队首 Turn。候选会话之间互不阻塞：
// MySQL 用 SKIP LOCKED 跳过正被其他事务持有的会话，SQLite 由写事务整体串行。
func (s *store) LeaseTurn(ctx context.Context, options core.ClaimOptions) (*core.TurnLease, error) {
	if !options.Valid() {
		return nil, core.ErrInvalid
	}
	var leased *core.TurnLease
	cursor, phaseFilter := s.turnStageCursor(options.Stage)
	err := s.runTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		now := s.now(ctx)
		// 候选只在无锁条件下读取（MySQL 不支持带非聚合列的 FOR UPDATE 排序查询）；
		// 真正的互斥在逐会话 lockConversation 时建立，被锁住的会话直接跳过。
		// 各会话队首之间按全局入队顺序排序，同刻就绪时先入队的会话先被领取。
		offset := cursor.Load()
		conversationIDs, err := loadIDs(ctx, tx, "SELECT conversation_id FROM "+s.t.turns+" WHERE "+phaseFilter+"(queue_head = 1 OR (queue_deadline > 0 AND queue_deadline <= ? AND phase = '')) AND eligible_at <= ? AND status IN ('queued','running','waiting') GROUP BY conversation_id ORDER BY MIN(enqueue_order) LIMIT ? OFFSET ?", now.UnixNano(), now.UnixNano(), leaseCandidateLimit, offset)
		if err == nil && len(conversationIDs) == 0 && offset > 0 {
			offset = 0
			cursor.Store(0)
			conversationIDs, err = loadIDs(ctx, tx, "SELECT conversation_id FROM "+s.t.turns+" WHERE "+phaseFilter+"(queue_head = 1 OR (queue_deadline > 0 AND queue_deadline <= ? AND phase = '')) AND eligible_at <= ? AND status IN ('queued','running','waiting') GROUP BY conversation_id ORDER BY MIN(enqueue_order) LIMIT ? OFFSET ?", now.UnixNano(), now.UnixNano(), leaseCandidateLimit, offset)
		}
		if err != nil || len(conversationIDs) == 0 {
			return err
		}
		for _, conversationID := range conversationIDs {
			conv := core.RouteKey(conversationID)
			if err := s.lockConversation(ctx, tx, conv, true); err != nil {
				if errors.Is(err, core.ErrNotFound) {
					continue // 被其他事务持有时 MySQL SKIP LOCKED 返回空。
				}
				return err
			}
			ids, err := loadIDs(ctx, tx, "SELECT id FROM "+s.t.turns+" WHERE conversation_id = ? AND "+phaseFilter+"(queue_head = 1 OR (queue_deadline > 0 AND queue_deadline <= ? AND phase = '')) AND eligible_at <= ? AND status IN ('queued','running','waiting') ORDER BY seq LIMIT 1", conversationID, now.UnixNano(), now.UnixNano())
			if err != nil || len(ids) == 0 {
				if err != nil {
					return err
				}
				continue
			}
			turn, err := loadOne[core.Turn](ctx, tx, "SELECT data FROM "+s.t.turns+" WHERE id=?", ids[0])
			if err != nil || turn == nil {
				if err != nil && !storedFormatError(err) {
					return err
				}
				continue
			}
			// 已知格式也不能信任 JSON 身份，避免用一行载荷覆盖另一行。
			if turn.ID != ids[0] || !supportedTurn(*turn, options.UserAuth) {
				continue
			}
			blocked, err := s.turnHasUnsupportedPredecessor(ctx, tx, ids[0], options.UserAuth)
			if err != nil {
				return err
			}
			if blocked {
				continue
			}
			// CAS回退没有会话行锁：候选查询与读取最新revision之间，其他事务可能
			// 已领取或推迟该Turn。必须按最新行重新核验就绪时间，不能覆盖有效lease。
			if !options.Stage.Accepts(turn.Phase) || !turn.Status.Open() || now.Before(turn.EligibleAt(now)) {
				continue
			}
			claimed := turn.Claim(options.Owner, now, options.TTL, options.MessageDeadline)
			if turn.Phase == core.PhaseNone && !turn.QueueDeadlineAt.IsZero() && !now.Before(turn.QueueDeadlineAt) {
				claimed.Schedule.StartedAt = turn.Schedule.StartedAt
				claimed.Schedule.DeadlineAt = turn.Schedule.DeadlineAt
			}
			if err := s.putTurn(ctx, tx, claimed, false, false); err != nil {
				if errors.Is(err, core.ErrLeaseLost) {
					continue
				}
				return err
			}
			message, err := loadOne[core.InboundMessage](ctx, tx, "SELECT data FROM "+s.t.messages+" WHERE id = ?", claimed.MessageID)
			if err != nil {
				return err
			}
			bindingVersion, err := loadOne[core.BindingVersion](ctx, tx, "SELECT data FROM "+s.t.versions+" WHERE binding_id = ? AND version = ?", claimed.BindingID, claimed.BindingVersion)
			if err != nil {
				return err
			}
			conversation, err := s.conversation(ctx, tx, claimed.ConversationID)
			if err != nil {
				return err
			}
			if message == nil || bindingVersion == nil || conversation == nil {
				return core.ErrConflict
			}
			leased = &core.TurnLease{Turn: claimed, Message: *message, Binding: *bindingVersion, Conversation: *conversation}
			cursor.Store(0)
			return nil
		}
		cursor.Store(offset + uint64(len(conversationIDs)))
		return nil
	})
	if err == nil && leased != nil {
		err = s.hydrateTurnLease(ctx, leased)
	}
	return leased, err
}

// RenewTurn 使用当前 fence 延长租约。
func (s *store) RenewTurn(ctx context.Context, lease core.Lease, ttl time.Duration) (core.Lease, error) {
	if ttl <= 0 {
		return core.Lease{}, core.ErrInvalid
	}
	var renewed core.Lease
	err := s.runTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		turn, err := s.lockedTurn(ctx, tx, lease.ID)
		if err != nil || turn == nil {
			if err == nil {
				return core.ErrLeaseLost
			}
			return err
		}
		if !turn.Holds(lease, s.now(ctx)) {
			return core.ErrLeaseLost
		}
		turn.Lease.ExpiresAt = s.now(ctx).Add(ttl)
		turn.Revision++
		renewed = turn.Lease
		return s.putTurn(ctx, tx, *turn, false, false)
	})
	return renewed, err
}

// ReleaseTurn 不改变进度地交还租约。
func (s *store) ReleaseTurn(ctx context.Context, lease core.Lease) error {
	return s.runTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		turn, err := s.turn(ctx, tx, lease.ID)
		if err != nil || turn == nil {
			if err == nil {
				return core.ErrLeaseLost
			}
			return err
		}
		current, err := s.leasedTurn(ctx, tx, lease)
		if err != nil {
			return err
		}
		released := current.Release()
		return s.putTurn(ctx, tx, released, false, false)
	})
}

// UpdateTurn 按迁移规则推进 Turn；revision CAS 保证旧 fence 写不进。
func (s *store) UpdateTurn(ctx context.Context, lease core.Lease, update core.TurnUpdate) (core.Turn, error) {
	var updated core.Turn
	var original *core.Turn
	if s.payloadCodec != nil && (update.Rebind || update.Input.ContextFrozen || update.Input.BlocksFrozen || update.Input.MountsFrozen) {
		var err error
		original, err = s.GetTurn(ctx, lease.ID)
		if err != nil {
			return updated, err
		}
	}
	err := s.runTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		updated = core.Turn{}
		owner, quota, err := s.lockRemoteQuota(ctx, tx, lease.ID)
		if err != nil {
			return err
		}
		if err := s.checkUserIntent(ctx, tx, owner, update.RemoteIntent); err != nil {
			return err
		}
		turn, err := s.leasedTurn(ctx, tx, lease)
		if err != nil {
			return err
		}
		if original != nil {
			if err = s.restoreInput(&turn, original); err != nil {
				return err
			}
		}

		if update.RemoteIntent != nil && turn.Session.SnapshotID != "" {
			snapshot, e := loadOne[core.SessionSnapshot](ctx, tx, "SELECT data FROM "+s.t.sessionSnapshots+" WHERE id = ?", turn.Session.SnapshotID)
			if e != nil {
				return e
			}
			turn, e = turn.WithSessionIdentity(snapshot)
			if e != nil {
				return e
			}
		}
		if updated, err = turn.Apply(update); err != nil {
			return err
		}
		if err := s.applyRemoteTurn(ctx, tx, updated, update, owner, quota); err != nil {
			return err
		}
		if err := s.putTurn(ctx, tx, updated, false, false); err != nil {
			return err
		}
		if s.payloadCodec != nil {
			persisted, e := s.turn(ctx, tx, updated.ID)
			if e != nil {
				return e
			}
			if persisted == nil {
				return core.ErrConflict
			}
			updated = *persisted
		}

		return nil
	})
	if err == nil && s.payloadCodec != nil {
		err = s.hydrate(ctx, &updated)
	}
	return updated, err
}

// WaitTurn 释放租约，到点后重新可领取。
func (s *store) WaitTurn(ctx context.Context, lease core.Lease, wait core.TurnWait) error {
	return s.runTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		candidate, err := s.turn(ctx, tx, lease.ID)
		if err != nil {
			return err
		}
		if candidate == nil {
			return core.ErrLeaseLost
		}
		turn, err := s.leasedTurn(ctx, tx, lease)
		if err != nil {
			return err
		}
		waiting, err := turn.Wait(wait)
		if err != nil {
			return err
		}
		return s.putTurn(ctx, tx, waiting, false, false)
	})
}

// SettleTurn 把终态、投递、交互过期与会话队首推进在一个事务内提交。
// 锁序：account quota → 会话行 → Turn → 投递/交互，与领取的前置锁顺序一致。
func (s *store) SettleTurn(ctx context.Context, lease core.Lease, outcome core.TurnOutcome, deliveries []core.Delivery) error {
	if !outcome.Status.Terminal() {
		return core.ErrInvalid
	}
	return s.runTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return s.settleUserTurn(ctx, tx, lease, outcome, deliveries)
	})
}

func (s *store) settleUserTurn(ctx context.Context, tx *sql.Tx, lease core.Lease, outcome core.TurnOutcome, deliveries []core.Delivery) error {
	turnLease, err := s.turn(ctx, tx, lease.ID)
	if err != nil || turnLease == nil {
		if err == nil {
			return core.ErrLeaseLost
		}
		return err
	}
	if err := s.lockConversation(ctx, tx, turnLease.ConversationID, false); err != nil {
		return err
	}
	turn, err := s.leasedTurn(ctx, tx, lease)
	if err != nil {
		return err
	}
	now := s.now(ctx)
	settled, err := turn.Settle(outcome, now)
	if err != nil {
		return err
	}
	if turn.WorkKind == core.WorkKindUserAuth {
		deliveries = append([]core.Delivery(nil), deliveries...)
		for i := range deliveries {
			deliveries[i].Snapshot = turn.Snapshot
		}
	}
	for _, delivery := range deliveries {
		if delivery.BindingID != turn.BindingID {
			return core.ErrConflict
		}
	}
	deliveries = core.ApplyStopReplyTarget(turn, deliveries)
	replies, progress, err := core.SplitTurnDeliveries(turn.ID, deliveries)
	if err != nil {
		return err
	}
	if err := s.insertDeliveries(ctx, tx, core.DeliverySource{Kind: core.SourceTurn, ID: turn.ID}, turn.ConversationID, replies); err != nil {
		return err
	}
	if err := s.insertDeliveries(ctx, tx, core.DeliverySource{Kind: core.SourceProgress, ID: turn.ID}, "", progress); err != nil {
		return err
	}
	pending, err := loadIDs(ctx, tx, "SELECT id FROM "+s.t.interactions+" WHERE turn_id = ? AND status = 'pending' ORDER BY id", turn.ID)
	if err != nil {
		return err
	}
	for _, id := range pending {
		// Lock by primary key before updating status, just like card decisions.
		// Locking the turn/status secondary index first can deadlock a callback
		// that already owns the primary row and needs to update that index.
		interaction, err := loadOne[core.Interaction](ctx, tx, "SELECT data FROM "+s.t.interactions+" WHERE id=?"+s.dialect.RowLockSuffix, id)
		if err != nil {
			return err
		}
		if interaction == nil || interaction.TurnID != turn.ID || interaction.Status != core.InteractionPending {
			continue
		}
		expired, _ := interaction.Expire(now)
		if err := s.putInteraction(ctx, tx, expired, false); err != nil {
			return err
		}
	}
	if err := s.putTurn(ctx, tx, settled, false, false); err != nil {
		return err
	}
	return s.refreshTurnHead(ctx, tx, turn.ConversationID)
}

// GetTurn 读取 Turn 的最新状态。
func (s *store) GetTurn(ctx context.Context, id string) (*core.Turn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	v, err := s.turn(ctx, s.db, id)
	if err == nil && v != nil {
		err = s.hydrate(ctx, v)
	}
	return v, err
}

// ---------------------------------------------------------------------------
// ConversationStore

func (s *store) GetConversation(ctx context.Context, id core.RouteKey) (*core.Conversation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.conversation(ctx, s.db, id)
}

// BindSession 把持有租约的 Turn 所在会话绑定到已确认的 Session。
// 锁序：会话行 → Turn，与领取/结算一致。
func (s *store) BindSession(ctx context.Context, lease core.Lease, binding core.SessionBinding) (core.Conversation, error) {
	var result core.Conversation
	err := s.runTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		turnLease, err := s.turn(ctx, tx, lease.ID)
		if err != nil || turnLease == nil {
			if err == nil {
				return core.ErrLeaseLost
			}
			return err
		}
		owner, quota, err := s.lockRemoteQuota(ctx, tx, lease.ID)
		if err != nil {
			return err
		}
		if err := s.lockConversation(ctx, tx, turnLease.ConversationID, false); err != nil {
			return err
		}
		turn, err := s.leasedTurn(ctx, tx, lease)
		if err != nil {
			return err
		}
		if binding.BindingVersion != turn.BindingVersion {
			return core.ErrInvalid
		}
		conversation, err := s.conversation(ctx, tx, turn.ConversationID)
		if err != nil {
			return err
		}
		if conversation == nil {
			return core.ErrConflict
		}
		if err := conversation.Session.CheckBind(binding); err != nil {
			return err
		}
		if conversation.Session != binding {
			conversation.Session, conversation.UpdatedAt = binding, s.now(ctx)
			conversation.Revision++
			if err := s.putConversation(ctx, tx, *conversation, false); err != nil {
				return err
			}
		}
		result = *conversation
		remoteTurn := turn
		remoteTurn.Session.Generation = binding.Generation
		remoteTurn.Session.SessionID = binding.SessionID
		if err := s.applyRemoteTurn(ctx, tx, remoteTurn, core.TurnUpdate{RemoteKind: core.RemoteCreate, RemoteProof: &core.RemoteProof{Kind: core.RemoteCreated, Reference: binding.SessionID}}, owner, quota); err != nil {
			return err
		}
		if err := s.recordHistory(ctx, tx, turn.ConversationID, binding); err != nil {
			return err
		}
		return nil
	})
	return result, err
}

// ResetConversation 让会话的下一条消息新建 Session。会话不存在时返回 (nil, nil)。
func (s *store) ResetConversation(ctx context.Context, id core.RouteKey) (*core.Conversation, error) {
	var result *core.Conversation
	err := s.runTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		existing, err := s.conversation(ctx, tx, id)
		if err != nil || existing == nil {
			return err
		}
		if err := s.lockConversation(ctx, tx, id, false); err != nil {
			return err
		}
		conversation, err := s.conversation(ctx, tx, id)
		if err != nil || conversation == nil {
			return err
		}
		if next := conversation.Session.Reset(); next != conversation.Session {
			conversation.Session, conversation.UpdatedAt = next, s.now(ctx)
			conversation.Revision++
			if err := s.putConversation(ctx, tx, *conversation, false); err != nil {
				return err
			}
		}
		result = conversation
		return nil
	})
	return result, err
}

// ---------------------------------------------------------------------------
// InteractionStore

// PutInteraction 幂等登记交互。
func (s *store) PutInteraction(ctx context.Context, lease core.Lease, interaction core.Interaction) (core.Interaction, error) {
	var result core.Interaction
	if !interaction.ValidNew() {
		return result, core.ErrInvalid
	}
	var original *core.Interaction
	if s.payloadCodec != nil {
		var err error
		original, err = s.GetInteraction(ctx, interaction.ID)
		if err != nil {
			return result, err
		}
	}
	err := s.runTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		result = core.Interaction{}
		turn, err := s.leasedTurn(ctx, tx, lease)
		if err != nil {
			return err
		}
		if interaction.TurnID != turn.ID {
			return core.ErrInvalid
		}
		existing, err := loadOne[core.Interaction](ctx, tx, "SELECT data FROM "+s.t.interactions+" WHERE id=?"+s.dialect.RowLockSuffix, interaction.ID)
		if err != nil {
			return err
		}
		if existing == nil {
			interaction.Payloads = nil
			result = interaction
			return s.putInteraction(ctx, tx, interaction, true)
		}
		if s.payloadCodec != nil {
			if err = restoreInteraction(existing, original); err != nil {
				return err
			}
		}
		merged, changed, err := existing.Register(interaction)
		if err != nil {
			return err
		}
		result = merged
		if changed {
			return s.putInteraction(ctx, tx, merged, false)
		}
		return nil
	})
	if err == nil && s.payloadCodec != nil {
		stored, readErr := s.GetInteraction(ctx, interaction.ID)
		if readErr != nil {
			return core.Interaction{}, readErr
		}
		if stored == nil {
			return core.Interaction{}, core.ErrConflict
		}
		result = *stored
	}
	return result, err
}

// DecideInteraction 以 CAS 方式决定交互。
func (s *store) DecideInteraction(ctx context.Context, id string, decision core.InteractionDecision) (core.Interaction, error) {
	var result core.Interaction
	var decideErr error
	var original *core.Interaction
	if s.payloadCodec != nil {
		var err error
		original, err = s.GetInteraction(ctx, id)
		if err != nil {
			return result, err
		}
	}
	err := s.runTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		existing, err := loadOne[core.Interaction](ctx, tx, "SELECT data FROM "+s.t.interactions+" WHERE id=?"+s.dialect.RowLockSuffix, id)
		if err != nil {
			return err
		}
		if existing == nil {
			return core.ErrNotFound
		}
		if s.payloadCodec != nil {
			if err = restoreInteraction(existing, original); err != nil {
				return err
			}
		}
		decided, changed, err := existing.Decide(decision, s.now(ctx))
		result, decideErr = decided, err
		if changed {
			return s.putInteraction(ctx, tx, decided, false)
		}
		return nil
	})
	if err != nil {
		return core.Interaction{}, err
	}
	if s.payloadCodec != nil {
		stored, err := s.GetInteraction(ctx, id)
		if err != nil {
			return core.Interaction{}, err
		}
		if stored == nil {
			return core.Interaction{}, core.ErrConflict
		}
		result = *stored
	}
	return result, decideErr
}

func (s *store) GetInteraction(ctx context.Context, id string) (*core.Interaction, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result, err := s.interaction(ctx, s.db, id)
	if err == nil && result != nil {
		err = s.hydrate(ctx, result)
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// DeliveryStore

// LeaseDelivery 领取一个可投递的来源队首投递。不同来源的行锁互不阻塞。
func (s *store) LeaseDelivery(ctx context.Context, owner string, ttl time.Duration) (*core.Delivery, error) {
	if owner == "" || ttl <= 0 {
		return nil, core.ErrInvalid
	}
	if s.payloadCodec != nil {
		return s.leaseEncryptedDelivery(ctx, owner, ttl)
	}
	var claimed *core.Delivery
	err := s.runTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		now := s.now(ctx)
		offset := s.deliveryScanOffset.Load()
		query := "SELECT id FROM " + s.t.deliveries + " WHERE queue_head=1 AND status IN ('pending','uncertain') AND eligible_at<=? ORDER BY " + deliveryOrder + " LIMIT ? OFFSET ?" + s.dialect.RowLockSuffix + s.dialect.SkipLockedSuffix
		ids, err := loadIDs(ctx, tx, query, now.UnixNano(), leaseCandidateLimit, offset)
		if err == nil && len(ids) == 0 && offset > 0 {
			offset = 0
			s.deliveryScanOffset.Store(0)
			ids, err = loadIDs(ctx, tx, query, now.UnixNano(), leaseCandidateLimit, offset)
		}
		if err != nil {
			return err
		}
		for _, id := range ids {
			delivery, err := loadOne[core.Delivery](ctx, tx, "SELECT data FROM "+s.t.deliveries+" WHERE id=?", id)
			if err != nil {
				if storedFormatError(err) {
					continue
				}
				return err
			}
			if delivery == nil || !supportedDelivery(*delivery) || delivery.Status.Terminal() || now.Before(delivery.EligibleAt()) {
				continue
			}
			next := delivery.Claim(owner, now, ttl)
			if err = s.putDelivery(ctx, tx, next, false); err != nil {
				return err
			}
			claimed = &next
			s.deliveryScanOffset.Store(0)
			return nil
		}
		if len(ids) == leaseCandidateLimit {
			s.deliveryScanOffset.Store(offset + uint64(len(ids)))
		} else {
			s.deliveryScanOffset.Store(0)
		}
		return nil
	})
	if err == nil && claimed != nil {
		err = s.hydrate(ctx, claimed)
	}
	return claimed, err
}

// RenewDelivery 使用数据库时间与当前 fence 核验并延长投递租约。
func (s *store) RenewDelivery(ctx context.Context, lease core.Lease, ttl time.Duration) (core.Lease, error) {
	if ttl <= 0 {
		return core.Lease{}, core.ErrInvalid
	}
	var renewed core.Lease
	err := s.runTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		delivery, err := s.lockedDelivery(ctx, tx, lease.ID)
		if err != nil {
			return err
		}
		if delivery == nil || !delivery.Holds(lease, s.now(ctx)) {
			return core.ErrLeaseLost
		}
		delivery.Lease.ExpiresAt = s.now(ctx).Add(ttl)
		renewed = delivery.Lease
		return s.putDelivery(ctx, tx, *delivery, false)
	})
	return renewed, err
}

// CompleteDelivery 保存一次投递结果；终态时在同一事务（持有来源行锁）推进队首。
func (s *store) CompleteDelivery(ctx context.Context, lease core.Lease, result core.DeliveryResult) error {
	return s.runTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		delivery, err := s.lockedDelivery(ctx, tx, lease.ID)
		if err != nil || delivery == nil {
			if err == nil {
				return core.ErrLeaseLost
			}
			return err
		}
		if !delivery.Holds(lease, s.now(ctx)) {
			return core.ErrLeaseLost
		}
		completed, err := delivery.Complete(result)
		if err != nil {
			return err
		}
		if err := s.putDelivery(ctx, tx, completed, false); err != nil {
			return err
		}
		if completed.Status.Terminal() {
			return s.refreshDeliveryHead(ctx, tx, completed.Source)
		}
		return nil
	})
}

// RetryDelivery 把 failed 投递恢复为 pending 并重置尝试预算。
func (s *store) RetryDelivery(ctx context.Context, id string) (*core.Delivery, error) {
	var result *core.Delivery
	err := s.runTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		delivery, err := s.lockedDelivery(ctx, tx, id)
		if err != nil || delivery == nil {
			if err == nil {
				return core.ErrNotFound
			}
			return err
		}
		retried, err := delivery.Retry()
		if err != nil {
			return err
		}
		if err := s.putDelivery(ctx, tx, retried, false); err != nil {
			return err
		}
		result = &retried
		return s.refreshDeliveryHead(ctx, tx, retried.Source)
	})
	if err == nil && result != nil {
		err = s.hydrate(ctx, result)
	}
	return result, err
}

// ---------------------------------------------------------------------------
// AdminStore

func (s *store) Stats(ctx context.Context) (core.QueueStats, error) {
	var stats core.QueueStats
	if err := ctx.Err(); err != nil {
		return stats, err
	}
	rows, err := s.db.QueryContext(ctx, "SELECT status, COUNT(*) FROM "+s.t.turns+" WHERE status IN ('queued', 'running', 'waiting') GROUP BY status")
	if err != nil {
		return stats, err
	}
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			_ = rows.Close()
			return stats, err
		}
		switch core.TurnStatus(status) {
		case core.TurnQueued:
			stats.QueuedTurns = count
		case core.TurnRunning:
			stats.RunningTurns = count
		case core.TurnWaiting:
			stats.WaitingTurns = count
		}
	}
	if err := rows.Close(); err != nil {
		return stats, err
	}
	var oldest sql.NullInt64
	if err := s.db.QueryRowContext(ctx, "SELECT MIN(created_at) FROM "+s.t.turns+" WHERE status IN ('queued', 'running', 'waiting')").Scan(&oldest); err != nil {
		return stats, err
	}
	if oldest.Valid && oldest.Int64 > 0 {
		stats.OldestOpenTurnAt = time.Unix(0, oldest.Int64).UTC()
	}
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+s.t.deliveries+" WHERE status IN ('pending', 'uncertain')").Scan(&stats.OpenDeliveries); err != nil {
		return stats, err
	}
	err = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+s.t.deliveries+" WHERE status = 'failed'").Scan(&stats.FailedDeliveries)
	return stats, err
}

func (s *store) ListTurns(ctx context.Context, query core.TurnQuery) ([]core.Turn, error) {
	limit, err := core.PageLimit(query.Limit)
	if err != nil {
		return nil, err
	}
	where, args := []string{"id > ?"}, []any{query.After}
	if query.ConversationID != "" {
		where = append(where, "conversation_id = ?")
		args = append(args, string(query.ConversationID))
	}
	if query.OpenOnly {
		where = append(where, "status IN ('queued', 'running', 'waiting')")
	}
	values, err := loadMany[core.Turn](ctx, s.db, fmt.Sprintf("SELECT data FROM %s WHERE %s ORDER BY id LIMIT %d", s.t.turns, strings.Join(where, " AND "), limit), args...)
	if err == nil {
		for i := range values {
			if err = s.hydrate(ctx, &values[i]); err != nil {
				break
			}
		}
	}
	return values, err
}

func (s *store) ListDeliveries(ctx context.Context, query core.DeliveryQuery) ([]core.Delivery, error) {
	limit, err := core.PageLimit(query.Limit)
	if err != nil {
		return nil, err
	}
	where, args := []string{"id > ?"}, []any{query.After}
	if query.Status != "" {
		where = append(where, "status = ?")
		args = append(args, string(query.Status))
	}
	if query.OpenOnly {
		where = append(where, "status IN ('pending', 'uncertain')")
	}
	values, err := loadMany[core.Delivery](ctx, s.db, fmt.Sprintf("SELECT data FROM %s WHERE %s ORDER BY id LIMIT %d", s.t.deliveries, strings.Join(where, " AND "), limit), args...)
	if err == nil {
		for i := range values {
			if err = s.hydrate(ctx, &values[i]); err != nil {
				break
			}
		}
	}
	return values, err
}

func (s *store) ListConversations(ctx context.Context, query core.ConversationQuery) ([]core.Conversation, error) {
	limit, err := core.PageLimit(query.Limit)
	if err != nil {
		return nil, err
	}
	return loadMany[core.Conversation](ctx, s.db, fmt.Sprintf("SELECT data FROM %s WHERE id > ? ORDER BY id LIMIT %d", s.t.conversations, limit), query.After)
}

func (s *store) ListBindingVersions(ctx context.Context) ([]core.BindingVersion, error) {
	return loadMany[core.BindingVersion](ctx, s.db, "SELECT data FROM "+s.t.versions+" ORDER BY binding_id, version")
}

// Prune 按来源小批删除终态记录；不持长事务、不锁 meta，引用尚在的记录不删除。
func (s *store) Prune(ctx context.Context, before time.Time) (core.PruneResult, error) {
	if s.payloadCodec != nil {
		return s.pruneEncrypted(ctx, before)
	}
	var removed core.PruneResult
	cutoff := nanos(before)
	const batch = 100
	if err := s.pruneRejectedSources(ctx, cutoff, &removed); err != nil {
		return removed, err
	}
	turnAfter := ""
	for {
		var n int
		err := s.runTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
			openDelivery := "SELECT source_id FROM " + s.t.deliveries + " WHERE source_kind = ? AND status IN ('pending', 'uncertain')"
			turns, err := loadMany[core.Turn](ctx, tx, "SELECT data FROM "+s.t.turns+" WHERE id>? AND status IN ('done', 'failed', 'stopped') AND settled_at > 0 AND settled_at < ? AND id NOT IN (SELECT turn_id FROM "+s.t.remoteOperations+" WHERE resolved = 0) AND id NOT IN ("+openDelivery+") AND id NOT IN (SELECT source_id FROM "+s.t.deliveries+" WHERE source_kind=? AND status IN ('pending','uncertain')) AND id NOT IN (SELECT source_id FROM "+s.t.deliveries+" WHERE source_kind='reaction' AND status IN ('pending','uncertain')) ORDER BY id LIMIT ?", turnAfter, cutoff, string(core.SourceTurn), string(core.SourceProgress), batch)
			if err != nil {
				return err
			}
			n = len(turns)
			for _, turn := range turns {
				turnAfter = turn.ID
				if !supportedTurn(turn) {
					continue
				}
				if supported, err := s.pruneChildrenSupported(ctx, tx, turn.MessageID, turn.ID); err != nil {
					return err
				} else if !supported {
					continue
				}
				remoteBlocked, err := s.turnHasUnsupportedRemote(ctx, tx, turn.ID)
				if err != nil {
					return err
				}
				if remoteBlocked {
					continue
				}
				blocked, err := s.sourceHasUnsupportedDelivery(ctx, tx, core.DeliverySource{Kind: core.SourceTurn, ID: turn.ID}, true)
				if err != nil {
					return err
				}
				if blocked {
					continue
				}
				blocked, err = s.sourceHasUnsupportedDelivery(ctx, tx, core.DeliverySource{Kind: core.SourceProgress, ID: turn.ID}, true)
				if err != nil {
					return err
				}
				if blocked {
					continue
				}
				blocked, err = s.sourceHasUnsupportedDelivery(ctx, tx, core.DeliverySource{Kind: core.SourceReaction, ID: turn.ID}, true)
				if err != nil {
					return err
				}
				if blocked {
					continue
				}
				reactionDeleted, err := s.deleteSource(ctx, tx, core.DeliverySource{Kind: core.SourceReaction, ID: turn.ID}, "")
				if err != nil {
					return err
				}
				progressDeleted, err := s.deleteSource(ctx, tx, core.DeliverySource{Kind: core.SourceProgress, ID: turn.ID}, "")
				if err != nil {
					return err
				}
				deleted, err := s.deleteSource(ctx, tx, core.DeliverySource{Kind: core.SourceTurn, ID: turn.ID}, turn.MessageID)
				if err != nil {
					return err
				}
				removed.Deliveries += deleted + progressDeleted + reactionDeleted
				if _, err := tx.ExecContext(ctx, "DELETE FROM "+s.t.interactions+" WHERE turn_id = ?", turn.ID); err != nil {
					return err
				}
				if _, err := tx.ExecContext(ctx, "DELETE FROM "+s.t.turns+" WHERE id = ?", turn.ID); err != nil {
					return err
				}
				removed.Turns++
				removed.Messages++
			}
			return nil
		})
		if err != nil {
			return removed, err
		}
		if n < batch {
			break
		}
	}
	controlAfter := ""
	for {
		var n int
		err := s.runTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
			openDelivery := "SELECT source_id FROM " + s.t.deliveries + " WHERE source_kind = ? AND status IN ('pending', 'uncertain')"
			controls, err := loadMany[core.ControlRequest](ctx, tx, "SELECT data FROM "+s.t.controls+" WHERE id>? AND created_at < ? AND id NOT IN (SELECT source_id FROM "+s.t.rejected+") AND id NOT IN ("+openDelivery+") ORDER BY id LIMIT ?", controlAfter, cutoff, string(core.SourceControl), batch)
			if err != nil {
				return err
			}
			n = len(controls)
			for _, control := range controls {
				if control.IsOpaqueStored() {
					continue
				}
				if supported, err := s.pruneChildrenSupported(ctx, tx, control.MessageID, ""); err != nil {
					return err
				} else if !supported {
					continue
				}
				controlAfter = control.ID
				pinned, err := s.recallSourcePinned(ctx, tx, control.MessageID)
				if err != nil {
					return err
				}
				if pinned {
					continue
				}
				blocked, err := s.sourceHasUnsupportedDelivery(ctx, tx, core.DeliverySource{Kind: core.SourceControl, ID: control.ID}, true)
				if err != nil {
					return err
				}
				if blocked {
					continue
				}
				deleted, err := s.deleteSource(ctx, tx, core.DeliverySource{Kind: core.SourceControl, ID: control.ID}, control.MessageID)
				if err != nil {
					return err
				}
				removed.Deliveries += deleted
				if _, err := tx.ExecContext(ctx, "DELETE FROM "+s.t.controls+" WHERE id = ?", control.ID); err != nil {
					return err
				}
				removed.Controls++
				removed.Messages++
			}
			return nil
		})
		if err != nil {
			return removed, err
		}
		if n < batch {
			break
		}
	}
	// 加密模式的消息、投递及长期快照依赖精确历史归属；删除策略批准前保守保留配置版本。
	if s.payloadCodec != nil {
		return removed, nil
	}
	err := s.runTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, "DELETE FROM "+s.t.versions+" WHERE created_at < ? AND NOT EXISTS (SELECT 1 FROM "+s.t.turns+" t WHERE t.binding_id = "+s.t.versions+".binding_id AND t.binding_version = "+s.t.versions+".version) AND NOT EXISTS (SELECT 1 FROM "+s.t.conversations+" c WHERE c.binding_version = "+s.t.versions+".version)", cutoff)
		if err != nil {
			return err
		}
		count, _ := result.RowsAffected()
		removed.BindingVersions = int(count)
		return nil
	})
	return removed, err
}

// deleteSource 删除一个来源的全部投递及其入站消息（连同去重索引），返回删除的投递数。
func (s *store) deleteSource(ctx context.Context, tx *sql.Tx, source core.DeliverySource, messageID string) (int, error) {
	result, err := tx.ExecContext(ctx, "DELETE FROM "+s.t.deliveries+" WHERE source_kind = ? AND source_id = ?", string(source.Kind), source.ID)
	if err != nil {
		return 0, err
	}
	count, _ := result.RowsAffected()
	if _, err := tx.ExecContext(ctx, "DELETE FROM "+s.t.eventIDs+" WHERE message_id = ?", messageID); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM "+s.t.messages+" WHERE id = ?", messageID); err != nil {
		return 0, err
	}
	return int(count), nil
}

// Export 读取全部内容副本，只用于测试。
func (s *store) Export(ctx context.Context) (core.Export, error) {
	var export core.Export
	var err error
	if export.BindingVersions, err = s.ListBindingVersions(ctx); err != nil {
		return core.Export{}, err
	}
	if export.Conversations, err = loadMany[core.Conversation](ctx, s.db, "SELECT data FROM "+s.t.conversations+" ORDER BY id"); err != nil {
		return core.Export{}, err
	}
	if export.Messages, err = loadMany[core.InboundMessage](ctx, s.db, "SELECT data FROM "+s.t.messages+" ORDER BY id"); err != nil {
		return core.Export{}, err
	}
	if export.Turns, err = loadMany[core.Turn](ctx, s.db, "SELECT data FROM "+s.t.turns+" ORDER BY id"); err != nil {
		return core.Export{}, err
	}
	if export.Controls, err = loadMany[core.ControlRequest](ctx, s.db, "SELECT data FROM "+s.t.controls+" ORDER BY id"); err != nil {
		return core.Export{}, err
	}
	if export.Interactions, err = loadMany[core.Interaction](ctx, s.db, "SELECT data FROM "+s.t.interactions+" ORDER BY id"); err != nil {
		return core.Export{}, err
	}
	export.Deliveries, err = loadMany[core.Delivery](ctx, s.db, "SELECT data FROM "+s.t.deliveries+" ORDER BY id")
	if err == nil && s.payloadCodec != nil {
		for i := range export.Messages {
			if err = s.hydrate(ctx, &export.Messages[i]); err != nil {
				return core.Export{}, err
			}
		}
		for i := range export.Turns {
			if err = s.hydrate(ctx, &export.Turns[i]); err != nil {
				return core.Export{}, err
			}
		}
		for i := range export.Interactions {
			if err = s.hydrate(ctx, &export.Interactions[i]); err != nil {
				return core.Export{}, err
			}
		}
		for i := range export.Deliveries {
			if err = s.hydrate(ctx, &export.Deliveries[i]); err != nil {
				return core.Export{}, err
			}
		}
	}
	if err != nil {
		return core.Export{}, err
	}
	return export, nil
}

func (s *store) Close() error { return s.db.Close() }

// Open opens an installed database; the returned handle owns the db lifecycle.
func Open(ctx context.Context, db *sql.DB, dialect Dialect, options Options) (*channel.Store, error) {
	s, err := open(ctx, db, dialect, options)
	if err != nil {
		return nil, err
	}
	return handles.NewStore(s), nil
}
