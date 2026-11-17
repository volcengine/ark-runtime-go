// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

//go:build mysqlintegration

package sqlstore

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"debug/buildinfo"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/codec"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/payload"
)

const candidateSDKVersion = "v0.0.0"
const crossVersionNamespace = "cross-version-localtest-v1"

type crossVersionAESCodec struct{ aead cipher.AEAD }

func newCrossVersionAESCodec() (*crossVersionAESCodec, error) {
	key := sha256.Sum256([]byte("channel-cross-version-fixed-localtest-key"))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	return &crossVersionAESCodec{aead: aead}, err
}

func (c *crossVersionAESCodec) Seal(ctx context.Context, scope codec.Scope, plain []byte) (codec.Envelope, error) {
	if err := ctx.Err(); err != nil {
		return codec.Envelope{}, err
	}
	aad, err := codec.CanonicalScope(scope)
	if err != nil {
		return codec.Envelope{}, err
	}
	nonce := make([]byte, c.aead.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return codec.Envelope{}, err
	}
	return codec.Envelope{Version: 1, KeyID: "localtest-aes-gcm-v1", Metadata: nonce, Ciphertext: c.aead.Seal(nil, nonce, plain, aad)}, nil
}

func (c *crossVersionAESCodec) Open(ctx context.Context, scope codec.Scope, envelope codec.Envelope) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if envelope.Version != 1 || envelope.KeyID != "localtest-aes-gcm-v1" || len(envelope.Metadata) != c.aead.NonceSize() {
		return nil, errors.New("invalid localtest envelope")
	}
	aad, err := codec.CanonicalScope(scope)
	if err != nil {
		return nil, err
	}
	return c.aead.Open(nil, envelope.Metadata, envelope.Ciphertext, aad)
}

type legacyRequest struct {
	Command   string
	Profile   string
	Prefix    string
	ID        string
	Owner     string
	TTLMillis int
	Lease     core.Lease
}

type legacyResponse struct {
	Version  string
	Error    string
	Ingest   *core.IngestResult
	Turn     *core.TurnLease
	Lease    core.Lease
	Snapshot *core.SessionSnapshot
	Delivery *core.Delivery
}

func crossVersionEnvironment(overrides map[string]string) []string {
	out := make([]string, 0, len(os.Environ())+len(overrides))
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if _, changed := overrides[key]; !changed {
			out = append(out, entry)
		}
	}
	for key, value := range overrides {
		out = append(out, key+"="+value)
	}
	return out
}

func buildCandidateWorker(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	source, err := os.ReadFile("testdata/candidate_worker.go.txt")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "main.go"), source, 0600))
	modulePath, err := exec.Command("go", "env", "GOMOD").Output()
	require.NoError(t, err)
	root := filepath.Dir(strings.TrimSpace(string(modulePath)))
	module := fmt.Sprintf("module github.com/volcengine/ark-runtime-go/channel/testfixture\n\ngo 1.25.0\n\nrequire github.com/volcengine/ark-runtime-go/channel %s\nreplace github.com/volcengine/ark-runtime-go/channel => %q\nreplace github.com/volcengine/ark-runtime-go => %q\n", candidateSDKVersion, filepath.ToSlash(root), filepath.ToSlash(filepath.Dir(root)))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go.mod"), []byte(module), 0600))
	binary := filepath.Join(dir, "legacy-worker")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "build", "-o", binary, ".")
	command.Dir = dir
	command.Env = crossVersionEnvironment(map[string]string{"GOWORK": "off", "GOPROXY": "off", "GOSUMDB": "off", "GOTOOLCHAIN": "local", "GOFLAGS": "-mod=mod"})
	output, err := command.CombinedOutput()
	require.NoError(t, err, "必须独立构建当前候选SDK，不能用mock替代进程协议：%s", output)
	info, err := buildinfo.ReadFile(binary)
	require.NoError(t, err)
	found := false
	for _, dependency := range info.Deps {
		if dependency.Path == "github.com/volcengine/ark-runtime-go/channel" {
			require.Equal(t, candidateSDKVersion, dependency.Version)
			require.NotNil(t, dependency.Replace)
			require.Equal(t, filepath.ToSlash(root), filepath.ToSlash(dependency.Replace.Path), "独立worker必须构建同一个候选SDK")
			found = true
		}
	}
	require.True(t, found, "构建制品必须包含独立候选SDK版本")
	return binary
}

func runCandidateWorker(t *testing.T, binary, dsn, prefix string, request legacyRequest) legacyResponse {
	t.Helper()
	request.Prefix = prefix
	input, err := json.Marshal(request)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary)
	command.Env = crossVersionEnvironment(map[string]string{"CHANNEL_TEST_MYSQL_DSN": dsn})
	command.Stdin = bytes.NewReader(input)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	require.NoError(t, err, "独立候选SDK命令%s失败：%s", request.Command, stderr.String())
	var response legacyResponse
	require.NoError(t, json.Unmarshal(output, &response))
	require.Equal(t, candidateSDKVersion, response.Version)
	return response
}

func crossVersionBinding() core.Binding {
	return core.Binding{ID: "compat-binding", AppID: "compat-app", AgentID: "compat-agent", AgentVersion: "1", Owner: "compat-owner", Project: "compat-project"}
}

func crossVersionMessage(id string) core.Message {
	return core.Message{BindingID: crossVersionBinding().ID, ProviderEventID: "event-" + id, MessageID: "message-" + id, TenantKey: "compat-tenant", ChatID: "chat-" + id, ChatType: "group", SenderID: "compat-user", Text: "input-" + id}
}

func crossVersionSettle(t *testing.T, s *store, lease core.Lease) core.SessionSnapshot {
	t.Helper()
	ctx := context.Background()
	turn, err := s.GetTurn(ctx, lease.ID)
	require.NoError(t, err)
	require.NotNil(t, turn)
	key := "create-" + turn.ID
	updated, err := s.UpdateTurn(ctx, lease, core.TurnUpdate{Phase: core.PhaseCreate, Session: core.TurnSession{Generation: 1, CreationKey: key}})
	require.NoError(t, err)
	snapshot, err := s.FreezeSession(ctx, lease, core.NewSessionSnapshot(updated.ConversationID, 1, core.CreateSessionSpec{Binding: crossVersionBinding(), CreationKey: key, LookupKey: "lookup-" + turn.ID, ChatType: "group", EnvironmentType: "cloud", Extras: core.SessionExtras{Env: map[string]string{"PRIVATE_CROSS_VERSION": "fixture-spec-secret"}}, EnvironmentOverride: &core.EnvironmentOverride{Type: "cloud", Env: map[string]string{"PRIVATE_OVERRIDE": "fixture-override-secret"}}}))
	require.NoError(t, err)
	sessionID := "session-" + turn.ID
	_, err = s.BindSession(ctx, lease, core.SessionBinding{SnapshotID: snapshot.ID, State: core.SessionActive, Generation: 1, SessionID: sessionID, CreationKey: key, LookupKey: snapshot.Spec.LookupKey, BindingVersion: turn.BindingVersion})
	require.NoError(t, err)
	_, err = s.UpdateTurn(ctx, lease, core.TurnUpdate{Phase: core.PhaseSend, Session: core.TurnSession{SnapshotID: snapshot.ID, Generation: 1, CreationKey: key, SessionID: sessionID}})
	require.NoError(t, err)
	delivery := core.NewDelivery(core.DeliverySource{Kind: core.SourceTurn, ID: turn.ID}, 0, "answer", crossVersionBinding().ID, core.ReplyTarget{ChatID: "chat", MessageID: "message", Mode: "reply"}, core.TextBody("answer-"+turn.ID))
	delivery.Snapshot, delivery.SessionSnapshotID = turn.Snapshot, snapshot.ID
	require.NoError(t, s.SettleTurn(ctx, lease, core.TurnOutcome{Status: core.TurnDone}, []core.Delivery{delivery}))
	return snapshot
}

// crossVersionTakeoverMetadata 只含接管条件和原文哈希，失败诊断不输出正文或凭证。
type crossVersionTakeoverMetadata struct {
	Head                          int
	Status, JSONStatus            string
	RowRevision, JSONRevision     uint64
	IDMatches, LeaseMatches       bool
	DBNow, LocalNow               time.Time
	SQLEligible, LeaseExpires     time.Time
	EligibleByDB, EligibleByLocal time.Time
	Hash                          string
}

func crossVersionReadTakeover(ctx context.Context, db *sql.DB, table string, lease core.Lease) (crossVersionTakeoverMetadata, error) {
	var state crossVersionTakeoverMetadata
	var raw []byte
	var eligible, micros int64
	turn := strings.HasSuffix(table, "channel_turns")
	revision := "0"
	if turn {
		revision = "revision"
	}
	err := db.QueryRowContext(ctx, "SELECT queue_head,status,eligible_at,"+revision+",data,CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(6))*1000000 AS UNSIGNED) FROM "+table+" WHERE id=?", lease.ID).
		Scan(&state.Head, &state.Status, &eligible, &state.RowRevision, &raw, &micros)
	if err != nil {
		return state, err
	}
	state.DBNow, state.LocalNow = time.UnixMicro(micros), time.Now()
	state.SQLEligible = time.Unix(0, eligible)
	state.Hash = fmt.Sprintf("%x", sha256.Sum256(raw))
	var storedLease core.Lease
	if turn {
		var value core.Turn
		if err = json.Unmarshal(raw, &value); err != nil {
			return state, fmt.Errorf("turn metadata decode failed (%T)", err)
		}
		state.IDMatches = value.ID == lease.ID
		state.JSONStatus, state.JSONRevision = string(value.Status), value.Revision
		state.EligibleByDB, state.EligibleByLocal = value.EligibleAt(state.DBNow), value.EligibleAt(state.LocalNow)
		storedLease = value.Lease
	} else {
		var value core.Delivery
		if err = json.Unmarshal(raw, &value); err != nil {
			return state, fmt.Errorf("delivery metadata decode failed (%T)", err)
		}
		state.IDMatches = value.ID == lease.ID
		state.JSONStatus = string(value.Status)
		state.EligibleByDB, state.EligibleByLocal = value.EligibleAt(), value.EligibleAt()
		storedLease = value.Lease
	}
	state.LeaseMatches = storedLease.ID == lease.ID && storedLease.Owner == lease.Owner && storedLease.Fence == lease.Fence && storedLease.ExpiresAt.Equal(lease.ExpiresAt)
	state.LeaseExpires = storedLease.ExpiresAt
	return state, nil
}

// crossVersionWaitTakeover 仅观察已续租记录；条件满足后调用方仍只领取一次。
func crossVersionWaitTakeover(t *testing.T, db *sql.DB, table string, lease core.Lease) {
	t.Helper()
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		state, err := crossVersionReadTakeover(ctx, db, table, lease)
		t.Logf("接管失败时的安全 metadata：%+v; read_error=%v", state, err)
	})
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	require.EventuallyWithT(t, func(check *assert.CollectT) {
		state, err := crossVersionReadTakeover(ctx, db, table, lease)
		if !assert.NoError(check, err) {
			return
		}
		assert.Equal(check, 1, state.Head)
		assert.True(check, state.IDMatches && state.LeaseMatches, "接管前身份/fence必须仍属于被续租记录")
		assert.Equal(check, state.Status, state.JSONStatus)
		if strings.HasSuffix(table, "channel_turns") {
			assert.Equal(check, string(core.TurnRunning), state.Status)
			assert.Equal(check, state.RowRevision, state.JSONRevision)
		} else {
			// Delivery 不含 revision 列或字段；保持其 pending/uncertain 领取契约。
			assert.Contains(check, []string{string(core.DeliveryPending), string(core.DeliveryUncertain)}, state.Status)
		}
		assert.False(check, state.LeaseExpires.IsZero())
		assert.True(check, state.SQLEligible.Equal(state.EligibleByDB) && state.SQLEligible.Equal(state.EligibleByLocal), "SQL eligible_at必须与typed就绪时间一致，metadata=%+v", state)
		for _, now := range []time.Time{state.DBNow, state.LocalNow} {
			for _, readyAt := range []time.Time{state.SQLEligible, state.LeaseExpires, state.EligibleByDB, state.EligibleByLocal} {
				assert.False(check, now.Before(readyAt), "等待实际到期，metadata=%+v", state)
			}
		}
	}, 2*time.Second, 5*time.Millisecond, "只读等待接管就绪，不重试领取")
}

func crossVersionCleanup(t *testing.T, db *sql.DB, prefix string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		rows, err := db.QueryContext(ctx, "SELECT table_name FROM information_schema.tables WHERE table_schema=DATABASE() AND LEFT(table_name,?)=?", len(prefix), prefix)
		require.NoError(t, err)
		var tables []string
		for rows.Next() {
			var name string
			require.NoError(t, rows.Scan(&name))
			require.True(t, strings.HasPrefix(name, prefix))
			tables = append(tables, name)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		for _, name := range tables {
			_, err = db.ExecContext(ctx, "DROP TABLE `"+strings.ReplaceAll(name, "`", "``")+"`")
			require.NoError(t, err)
		}
	})
}

func assertCrossVersionEncryptedStorage(t *testing.T, db *sql.DB, prefix string, secrets []string) {
	t.Helper()
	ctx := context.Background()
	rows, err := db.QueryContext(ctx, "SELECT table_name FROM information_schema.columns WHERE table_schema=DATABASE() AND LEFT(table_name,?)=? AND column_name='data'", len(prefix), prefix)
	require.NoError(t, err)
	var tables []string
	for rows.Next() {
		var table string
		require.NoError(t, rows.Scan(&table))
		tables = append(tables, table)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	metadataCount := 0
	for _, table := range tables {
		require.True(t, strings.HasPrefix(table, prefix))
		values, err := db.QueryContext(ctx, "SELECT data FROM `"+strings.ReplaceAll(table, "`", "``")+"`")
		require.NoError(t, err)
		for values.Next() {
			var raw []byte
			require.NoError(t, values.Scan(&raw))
			metadataCount++
			for _, secret := range secrets {
				require.NotContains(t, string(raw), secret, "数据库metadata不得恢复fixture正文，table=%s", table)
			}
		}
		require.NoError(t, values.Err())
		require.NoError(t, values.Close())
	}
	require.Positive(t, metadataCount)
	cipher, err := newCrossVersionAESCodec()
	require.NoError(t, err)
	sealed, err := db.QueryContext(ctx, "SELECT data FROM "+prefix+"channel_sensitive_payloads")
	require.NoError(t, err)
	found := make(map[string]bool, len(secrets))
	sealedCount := 0
	for sealed.Next() {
		var raw []byte
		require.NoError(t, sealed.Scan(&raw))
		var record payload.Record
		require.NoError(t, json.Unmarshal(raw, &record))
		require.Equal(t, crossVersionNamespace, record.Scope.Namespace)
		require.Equal(t, "compat-owner", record.Scope.Owner)
		require.Equal(t, "compat-project", record.Scope.Project)
		plain, err := cipher.Open(ctx, record.Scope, record.Envelope)
		require.NoError(t, err)
		for _, secret := range secrets {
			if bytes.Contains(plain, []byte(secret)) {
				found[secret] = true
			}
		}
		clear(plain)
		wrongScope := record.Scope
		wrongScope.RecordID += "-other"
		_, err = cipher.Open(ctx, wrongScope, record.Envelope)
		require.Error(t, err, "旧/新writer密文都必须认证完整AAD身份")
		sealedCount++
	}
	require.NoError(t, sealed.Err())
	require.NoError(t, sealed.Close())
	require.GreaterOrEqual(t, sealedCount, 6)
	for _, secret := range secrets {
		require.True(t, found[secret], "正文必须可从真实加密载荷恢复，而不是被丢弃：%s", secret)
	}
}

// TestMySQLRealLegacyAndCurrentWorkers 验证兼容legacy格式的真实双版本读写与回退。
// 本地效果表只验证稳定UUID的幂等标识，不代表真实渠道已完成外部副作用验收。
func TestMySQLIndependentCandidateWorkers(t *testing.T) {
	dsn := os.Getenv("CHANNEL_TEST_MYSQL_DSN")
	require.NotEmpty(t, dsn, "必须显式提供本地隔离MySQL，不跳过双版本验证")
	config, err := mysql.ParseDSN(dsn)
	require.NoError(t, err)
	host, _, err := net.SplitHostPort(config.Addr)
	require.NoError(t, err)
	require.Equal(t, "tcp", config.Net)
	require.True(t, strings.HasSuffix(config.DBName, "_test"), "必须使用以_test结尾的隔离测试库")
	require.Contains(t, []string{"127.0.0.1", "localhost", "::1"}, host, "禁止连接环境数据库")
	legacy := buildCandidateWorker(t)
	for _, profile := range []string{"plain", "encrypted"} {
		t.Run(profile, func(t *testing.T) { runCrossVersionMatrix(t, dsn, legacy, profile) })
	}
}

func runCrossVersionMatrix(t *testing.T, dsn, legacy, profile string) {
	var random [8]byte
	_, err := rand.Read(random[:])
	require.NoError(t, err)
	prefix := "cv_" + hex.EncodeToString(random[:]) + "_"
	admin, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, admin.Close()) })
	crossVersionCleanup(t, admin, prefix)
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	options := Options{TablePrefix: prefix}
	if profile == "encrypted" {
		options.PayloadCodec, err = newCrossVersionAESCodec()
		require.NoError(t, err)
		options.PayloadNamespace = crossVersionNamespace
		options.RequirePayloadEncryption = true
	}
	ctx := context.Background()
	require.NoError(t, Install(ctx, db, MySQL, options))
	current, err := open(ctx, db, MySQL, options)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, current.Close()) })
	_, err = admin.ExecContext(ctx, "CREATE TABLE "+prefix+"test_effects (uuid VARCHAR(64) PRIMARY KEY, delivery_id VARCHAR(64) NOT NULL, writer VARCHAR(16) NOT NULL)")
	require.NoError(t, err)
	old := func(request legacyRequest) legacyResponse {
		request.Profile = profile
		return runCandidateWorker(t, legacy, dsn, prefix, request)
	}
	require.Empty(t, old(legacyRequest{Command: "open"}).Error)
	var workKindColumn int
	require.NoError(t, admin.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=? AND column_name='work_kind'", prefix+"channel_turns").Scan(&workKindColumn))
	require.Zero(t, workKindColumn)

	// worker A生产和领取，worker B续租、过期接管、冻结和结算；worker A完成投递。
	producedOld := old(legacyRequest{Command: "produce", ID: "old-to-new"}).Ingest
	require.NotNil(t, producedOld)
	oldClaim := old(legacyRequest{Command: "lease-turn", Owner: "legacy-turn", TTLMillis: 2000}).Turn
	require.NotNil(t, oldClaim)
	require.Equal(t, producedOld.Turn.ID, oldClaim.Turn.ID)
	oldRenewed, err := current.RenewTurn(ctx, oldClaim.Turn.Lease, 150*time.Millisecond)
	require.NoError(t, err)
	require.Equal(t, oldClaim.Turn.Lease.Fence, oldRenewed.Fence)
	require.True(t, oldRenewed.ExpiresAt.Before(oldClaim.Turn.Lease.ExpiresAt), "150ms续租必须缩短原2s租约")
	crossVersionWaitTakeover(t, admin, prefix+"channel_turns", oldRenewed)
	newClaim, err := current.LeaseTurn(ctx, core.ClaimOptions{Owner: "current-turn", TTL: 2 * time.Second, MessageDeadline: time.Hour})
	require.NoError(t, err)
	require.NotNil(t, newClaim)
	require.Greater(t, newClaim.Turn.Lease.Fence, oldClaim.Turn.Lease.Fence)
	require.Equal(t, "lease_lost", old(legacyRequest{Command: "renew-turn", Lease: oldClaim.Turn.Lease}).Error)
	newSnapshot := crossVersionSettle(t, current, newClaim.Turn.Lease)
	newDelivery, err := current.LeaseDelivery(ctx, "current-delivery", 2*time.Second)
	require.NoError(t, err)
	require.NotNil(t, newDelivery)
	// 模拟效果已产生但Complete响应丢失；worker A接管必须重用相同UUID。
	_, err = admin.ExecContext(ctx, "INSERT INTO "+prefix+"test_effects (uuid,delivery_id,writer) VALUES (?,?,?)", newDelivery.UUID, newDelivery.ID, "current")
	require.NoError(t, err)
	oldDeliveryRenewed := old(legacyRequest{Command: "renew-delivery", Lease: newDelivery.Lease, TTLMillis: 150})
	require.Empty(t, oldDeliveryRenewed.Error)
	require.True(t, oldDeliveryRenewed.Lease.ExpiresAt.Before(newDelivery.Lease.ExpiresAt), "150ms续租必须缩短原2s租约")
	crossVersionWaitTakeover(t, admin, prefix+"channel_deliveries", oldDeliveryRenewed.Lease)
	oldDeliveryResult := old(legacyRequest{Command: "lease-delivery", Owner: "legacy-delivery", TTLMillis: 2000})
	require.Empty(t, oldDeliveryResult.Error)
	oldDelivery := oldDeliveryResult.Delivery
	require.NotNil(t, oldDelivery)
	require.Equal(t, newDelivery.ID, oldDelivery.ID)
	require.Equal(t, newDelivery.UUID, oldDelivery.UUID)
	require.Equal(t, newDelivery.FirstAttemptAt, oldDelivery.FirstAttemptAt)
	require.Greater(t, oldDelivery.Lease.Fence, newDelivery.Lease.Fence)
	_, err = current.RenewDelivery(ctx, newDelivery.Lease, time.Second)
	require.ErrorIs(t, err, core.ErrLeaseLost)
	require.Empty(t, old(legacyRequest{Command: "complete-delivery", Lease: oldDelivery.Lease}).Error)

	// worker B生产和领取，worker A续租、过期接管、冻结和结算；worker B完成投递。
	producedNew, err := current.Ingest(ctx, core.NewBindingVersion(crossVersionBinding()), crossVersionMessage("new-to-old"))
	require.NoError(t, err)
	initialNew, err := current.LeaseTurn(ctx, core.ClaimOptions{Owner: "current-origin", TTL: 2 * time.Second, MessageDeadline: time.Hour})
	require.NoError(t, err)
	require.NotNil(t, initialNew)
	legacyRenewed := old(legacyRequest{Command: "renew-turn", Lease: initialNew.Turn.Lease, TTLMillis: 150})
	require.Empty(t, legacyRenewed.Error)
	require.True(t, legacyRenewed.Lease.ExpiresAt.Before(initialNew.Turn.Lease.ExpiresAt), "150ms续租必须缩短原2s租约")
	crossVersionWaitTakeover(t, admin, prefix+"channel_turns", legacyRenewed.Lease)
	legacyClaimResult := old(legacyRequest{Command: "lease-turn", Owner: "legacy-takeover", TTLMillis: 2000})
	require.Empty(t, legacyClaimResult.Error)
	legacyClaim := legacyClaimResult.Turn
	require.NotNil(t, legacyClaim)
	require.Equal(t, producedNew.Turn.ID, legacyClaim.Turn.ID)
	require.Greater(t, legacyClaim.Turn.Lease.Fence, initialNew.Turn.Lease.Fence)
	_, err = current.RenewTurn(ctx, initialNew.Turn.Lease, time.Second)
	require.ErrorIs(t, err, core.ErrLeaseLost)
	legacySnapshot := old(legacyRequest{Command: "settle-turn", Lease: legacyClaim.Turn.Lease}).Snapshot
	require.NotNil(t, legacySnapshot)
	legacyOutbox := old(legacyRequest{Command: "lease-delivery", Owner: "legacy-origin", TTLMillis: 2000}).Delivery
	require.NotNil(t, legacyOutbox)
	require.Empty(t, old(legacyRequest{Command: "emit-effect", Lease: legacyOutbox.Lease}).Error)
	currentDeliveryRenewed, err := current.RenewDelivery(ctx, legacyOutbox.Lease, 150*time.Millisecond)
	require.NoError(t, err)
	require.True(t, currentDeliveryRenewed.ExpiresAt.Before(legacyOutbox.Lease.ExpiresAt), "150ms续租必须缩短原2s租约")
	crossVersionWaitTakeover(t, admin, prefix+"channel_deliveries", currentDeliveryRenewed)
	currentOutbox, err := current.LeaseDelivery(ctx, "current-takeover", 2*time.Second)
	require.NoError(t, err)
	require.NotNil(t, currentOutbox)
	require.Equal(t, legacyOutbox.ID, currentOutbox.ID)
	require.Equal(t, legacyOutbox.UUID, currentOutbox.UUID)
	require.Equal(t, legacyOutbox.FirstAttemptAt, currentOutbox.FirstAttemptAt)
	require.Greater(t, currentOutbox.Lease.Fence, legacyOutbox.Lease.Fence)
	require.Equal(t, "lease_lost", old(legacyRequest{Command: "renew-delivery", Lease: legacyOutbox.Lease}).Error)
	_, err = current.RenewDelivery(ctx, currentOutbox.Lease, time.Second)
	require.NoError(t, err)
	_, err = admin.ExecContext(ctx, "INSERT IGNORE INTO "+prefix+"test_effects (uuid,delivery_id,writer) VALUES (?,?,?)", currentOutbox.UUID, currentOutbox.ID, "current")
	require.NoError(t, err)
	require.NoError(t, current.CompleteDelivery(ctx, currentOutbox.Lease, core.DeliveryResult{Status: core.DeliveryDone, RemoteMessageID: "effect-" + currentOutbox.UUID}))

	for _, snapshot := range []core.SessionSnapshot{newSnapshot, *legacySnapshot} {
		stored, err := current.GetSessionSnapshot(ctx, snapshot.ID)
		require.NoError(t, err)
		require.NotNil(t, stored)
		require.True(t, stored.Valid())
		require.Equal(t, snapshot, *stored)
		expected := core.NewSessionSnapshot(stored.Route, stored.Generation, stored.Spec)
		require.Equal(t, expected.ID, stored.ID)
		require.Equal(t, expected.RequestHash, stored.RequestHash)
	}
	for _, ingest := range []*core.IngestResult{producedOld, &producedNew} {
		turn, err := current.GetTurn(ctx, ingest.Turn.ID)
		require.NoError(t, err)
		require.Equal(t, core.TurnDone, turn.Status)
		require.Equal(t, ingest.Turn.InputEventID, turn.InputEventID)
		require.Equal(t, ingest.Turn.BindingVersion, turn.BindingVersion)
		require.Equal(t, ingest.Turn.Snapshot.ID, turn.Snapshot.ID)
		require.True(t, turn.Snapshot.Valid())
		require.Equal(t, uint64(1), turn.Session.Generation)
		require.Equal(t, "session-"+turn.ID, turn.Session.SessionID)
	}
	newDuplicate, err := current.Ingest(ctx, core.NewBindingVersion(crossVersionBinding()), crossVersionMessage("old-to-new"))
	require.NoError(t, err)
	require.True(t, newDuplicate.Duplicate)
	require.Equal(t, producedOld.Message.ContentHash, newDuplicate.Message.ContentHash)
	require.Equal(t, producedOld.Turn.ID, newDuplicate.Turn.ID)
	oldDuplicate := old(legacyRequest{Command: "produce", ID: "new-to-old"}).Ingest
	require.NotNil(t, oldDuplicate)
	require.True(t, oldDuplicate.Duplicate)
	require.Equal(t, producedNew.Message.ContentHash, oldDuplicate.Message.ContentHash)
	require.Equal(t, producedNew.Turn.ID, oldDuplicate.Turn.ID)
	deliveries, err := current.ListDeliveries(ctx, core.DeliveryQuery{Limit: 10})
	require.NoError(t, err)
	require.Len(t, deliveries, 2)
	for _, delivery := range deliveries {
		require.Equal(t, core.DeliveryDone, delivery.Status)
		require.Equal(t, uint64(2), delivery.Attempt)
		require.Equal(t, "answer-"+delivery.Source.ID, delivery.Body.Text)
		require.Equal(t, "effect-"+delivery.UUID, delivery.RemoteMessageID)
		require.NotNil(t, delivery.Snapshot)
		require.True(t, delivery.Snapshot.Valid())
		require.NotEmpty(t, delivery.SessionSnapshotID)
		expected := core.NewDelivery(delivery.Source, 0, "answer", delivery.BindingID, delivery.ReplyTarget, delivery.Body)
		require.Equal(t, expected.ID, delivery.ID)
		require.Equal(t, expected.UUID, delivery.UUID)
	}
	var effects, oldEffects, newEffects int
	require.NoError(t, admin.QueryRowContext(ctx, "SELECT COUNT(*),SUM(writer='legacy'),SUM(writer='current') FROM "+prefix+"test_effects").Scan(&effects, &oldEffects, &newEffects))
	require.Equal(t, 2, effects)
	require.Equal(t, 1, oldEffects)
	require.Equal(t, 1, newEffects)
	stats, err := current.Stats(ctx)
	require.NoError(t, err)
	require.Zero(t, stats.QueuedTurns+stats.RunningTurns+stats.WaitingTurns+stats.OpenDeliveries+stats.FailedDeliveries)
	var turnCount, deliveryCount int
	require.NoError(t, admin.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+prefix+"channel_turns").Scan(&turnCount))
	require.Equal(t, 2, turnCount)
	require.NoError(t, admin.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+prefix+"channel_deliveries").Scan(&deliveryCount))
	require.Equal(t, 2, deliveryCount)
	if profile == "encrypted" {
		assertCrossVersionEncryptedStorage(t, admin, prefix, []string{"input-old-to-new", "input-new-to-old", "answer-" + producedOld.Turn.ID, "answer-" + producedNew.Turn.ID, "fixture-spec-secret", "fixture-override-secret"})
	}
}

// TestCandidateWorkerBuilds 使无数据库环境也能验证独立进程的候选模块与 internal import 边界。
func TestCandidateWorkerBuilds(t *testing.T) {
	binary := buildCandidateWorker(t)
	result := runCandidateWorker(t, binary, "", "", legacyRequest{Command: "version"})
	require.Empty(t, result.Error)
}
