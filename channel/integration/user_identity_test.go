// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	memory "github.com/volcengine/ark-runtime-go/channel/internal/testutil/memorystore"
	"github.com/volcengine/ark-runtime-go/channel/store/embedded"
	sqlstore "github.com/volcengine/ark-runtime-go/channel/store/sql"
)

type userAuthFixture struct {
	store         core.UserAuthStore
	lookupReauth  bool
	scopes        []string
	flow          string
	version       string
	mu            sync.Mutex
	ready         bool
	status        string
	starts, polls int
	query         core.UserGrantQuery
	request       core.AuthRequest
}

func (a *userAuthFixture) Lookup(_ context.Context, q core.UserGrantQuery) (core.UserGrant, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.query = q
	gateReady := false
	if a.lookupReauth && a.store != nil {
		gate, e := a.store.GetUserAuth(context.Background(), q.Principal)
		gateReady = e == nil && gate != nil && gate.State == core.UserAuthReady
	}
	if !a.ready || a.lookupReauth && !gateReady {
		return core.UserGrant{Status: core.AuthReauthRequired}, nil
	}
	return a.grant(q.VaultID), nil
}
func (a *userAuthFixture) Start(_ context.Context, _ core.UserPrincipal, r core.AuthRequest) (core.AuthFlow, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.starts++
	a.request = r
	flow := a.flow
	if flow == "" {
		flow = "flow"
	}
	return core.AuthFlow{ID: flow, URL: "https://auth.test/?secret=prompt", ExpiresAt: time.Now().Add(10 * time.Minute)}, nil
}
func (a *userAuthFixture) Status(_ context.Context, ref core.AuthFlowRef) (core.AuthStatus, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.polls++
	status := core.AuthPending
	if a.ready {
		status = core.AuthReady
	}
	if a.status != "" {
		status = a.status
	}
	return core.AuthStatus{Status: status, Grant: a.grant(ref.VaultID)}, nil
}

type userVaultMA struct {
	core.MAClient
	mu          sync.Mutex
	vault       core.VaultRef
	creates     int
	displayName string
}

func (m *userVaultMA) CreateVault(_ context.Context, s core.BotVaultCreate) (core.VaultRef, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.creates++
	m.displayName = s.DisplayName
	m.vault = core.VaultRef{ID: "user-vault", Name: s.Name, Principal: s.Metadata["ark_channel_principal"]}
	return m.vault, nil
}
func (m *userVaultMA) FindVaultByName(context.Context, string) (core.VaultRef, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.vault, nil
}
func (m *userVaultMA) GetEnvironment(ctx context.Context, id string) (core.EnvironmentConfig, error) {
	return m.MAClient.(core.EnvironmentReader).GetEnvironment(ctx, id)
}
func userBinding() core.Binding {
	b := runtimeBinding()
	b.Owner = "owner"
	b.Project = "project"
	b.ChannelID = "channel"
	b.BindingEpoch = 1
	b.Identity = core.IdentityPolicy{P2P: core.IdentityUser}
	b.UserAuthScopes = []string{"read"}
	return b
}
func newUserStore(t *testing.T) *memory.Store {
	s, e := memory.New(memory.Options{PayloadCodec: &conformance.UserAuthTestCodec{}, PayloadNamespace: "test", RequirePayloadEncryption: true})
	require.NoError(t, e)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}
func TestRuntimeUserAuthorizationWaitResumesOriginalInputAfterRestart(t *testing.T) {
	for _, kind := range []string{"sql", "embedded"} {
		t.Run(kind, func(t *testing.T) {

			s := userRuntimeStore(t, kind)
			f := &conformance.MAServer{EnvironmentType: "cloud"}
			a := &userAuthFixture{lookupReauth: true, store: s.(core.UserAuthStore)}
			p := &runtimeProvider{}
			client := &userVaultMA{MAClient: newMAForHarness(t, f)}
			b := userBinding()
			b.MaxTurnDuration = 0
			options := restartRuntimeOptions()
			options.MessageDeadline = time.Minute
			c := core.Connection{Binding: b, MAClient: client, Provider: p, UserAuthorizer: a}
			first, e := core.NewRuntime(s, []core.Connection{c}, options)
			require.NoError(t, e)
			admitted := mustEnqueue(t, first, b.ID, runtimeMessage())
			t.Cleanup(func() {
				if t.Failed() {
					turn, e := s.GetTurn(context.Background(), admitted.Turn.ID)
					t.Logf("turn=%+v err=%v", turn, e)
					gate, e := s.(core.UserAuthStore).GetUserAuth(context.Background(), userPrincipalFixture(b))
					t.Logf("gate=%+v err=%v", gate, e)
				}
			})
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- first.Run(ctx) }()
			require.Eventually(t, func() bool { turn, e := s.GetTurn(ctx, admitted.Turn.ID); return e == nil && turn.AuthWait != nil }, 15*time.Second, 10*time.Millisecond)
			f.Lock()
			require.Zero(t, f.CreateCount)
			require.Zero(t, f.SendCount)
			f.Unlock()
			before, e := s.GetTurn(ctx, admitted.Turn.ID)
			require.NoError(t, e)
			cancel()
			require.NoError(t, <-done)
			a.mu.Lock()
			a.ready = true
			a.mu.Unlock()
			second, e := core.NewRuntime(s, []core.Connection{c}, options)
			require.NoError(t, e)
			runUntilCleanup(t, second)
			third, e := core.NewRuntime(s, []core.Connection{c}, options)
			require.NoError(t, e)
			runUntilCleanup(t, third)
			f.AppendAfterInput(t, conformance.AnswerEvents("authorized")...)
			require.Eventually(t, func() bool {
				turn, e := s.GetTurn(context.Background(), admitted.Turn.ID)
				return e == nil && turn.Status == core.TurnDone
			}, 15*time.Second, 10*time.Millisecond)
			after, e := s.GetTurn(context.Background(), admitted.Turn.ID)
			require.NoError(t, e)
			require.Equal(t, before.InputEventID, after.InputEventID)
			snapshot, e := s.GetSessionSnapshot(context.Background(), after.Session.SnapshotID)
			require.NoError(t, e)
			require.Equal(t, "user-credential", snapshot.Spec.UserCredential.CredentialID)
			require.Contains(t, snapshot.Spec.Binding.VaultIDs, "user-vault")
			require.Equal(t, "user", snapshot.Spec.EnvironmentOverride.Env[core.EnvLarkSuiteStrictMode])
			a.mu.Lock()
			require.Equal(t, 1, a.starts)
			require.GreaterOrEqual(t, a.polls, 1)
			require.NotEmpty(t, a.request.Lease.Owner)
			a.mu.Unlock()
			f.Lock()
			require.Equal(t, 1, f.CreateCount)
			require.Equal(t, 1, f.SendCount)
			f.Unlock()
			ds, e := s.ListDeliveries(context.Background(), core.DeliveryQuery{})
			require.NoError(t, e)
			require.Len(t, ds, 2)

		})
	}
}

func seedUserSession(t *testing.T, s core.Store, b core.Binding, credential *core.UserCredentialSpec) core.SessionSnapshot {
	t.Helper()
	ctx := context.Background()
	m := runtimeMessage()
	m.MessageID = "seed"
	m.ProviderEventID = "seed"
	_, e := s.Ingest(ctx, core.NewBindingVersion(b), m)
	require.NoError(t, e)
	claim := conformance.Claim("seed")
	claim.UserAuth = true
	leased, e := s.LeaseTurn(ctx, claim)
	require.NoError(t, e)
	require.NotNil(t, leased)
	turn, e := s.UpdateTurn(ctx, leased.Turn.Lease, core.TurnUpdate{Phase: core.PhaseCreate, Session: core.TurnSession{Generation: 1, CreationKey: "seed-create"}})
	require.NoError(t, e)
	snapshot, e := s.FreezeSession(ctx, turn.Lease, core.NewSessionSnapshot(turn.ConversationID, 1, core.CreateSessionSpec{Binding: b, UserCredential: credential, CreationKey: "seed-create", LookupKey: "seed-lookup", ChatType: "p2p"}))
	require.NoError(t, e)
	_, e = s.BindSession(ctx, turn.Lease, core.SessionBinding{SnapshotID: snapshot.ID, State: core.SessionActive, Generation: 1, SessionID: "sesn-demo", CreationKey: "seed-create", LookupKey: "seed-lookup", BindingVersion: turn.BindingVersion})
	require.NoError(t, e)
	require.NoError(t, s.SettleTurn(ctx, turn.Lease, core.TurnOutcome{Status: core.TurnDone}, nil))
	return snapshot
}
func TestRuntimeUserPolicyPreservesOldSessionIdentity(t *testing.T) {
	s := newUserStore(t)
	b := userBinding()
	old := b
	old.Identity = core.IdentityPolicy{}
	snapshot := seedUserSession(t, s, old, nil)
	a := &userAuthFixture{}
	f := &conformance.MAServer{}
	options := testRuntimeOptions()
	options.MessageDeadline = time.Minute
	r, e := core.NewRuntime(s, []core.Connection{{Binding: b, Provider: &runtimeProvider{}, MAClient: newMAForHarness(t, f), UserAuthorizer: a}}, options)
	require.NoError(t, e)
	result := mustEnqueue(t, r, b.ID, runtimeMessage())
	runUntilCleanup(t, r)
	f.AppendAfterInput(t, conformance.AnswerEvents("old session")...)
	require.Eventually(t, func() bool {
		turn, e := s.GetTurn(context.Background(), result.Turn.ID)
		return e == nil && turn.Status == core.TurnDone
	}, time.Second, 10*time.Millisecond)
	a.mu.Lock()
	require.Zero(t, a.starts)
	require.Empty(t, a.query)
	a.mu.Unlock()
	after, e := s.GetTurn(context.Background(), result.Turn.ID)
	require.NoError(t, e)
	require.Equal(t, snapshot.ID, after.Session.SnapshotID)
}
func TestRuntimeUserRecoveredStartUnknownNeverRestarts(t *testing.T) {
	s := newUserStore(t)
	b := userBinding()
	m := runtimeMessage()
	p := core.UserPrincipal{Owner: b.Owner, Project: b.Project, ChannelID: b.ChannelID, Epoch: b.BindingEpoch, AppID: b.AppID, TenantKey: m.TenantKey, OpenID: m.SenderID}
	gate, e := s.BeginUserAuth(context.Background(), core.UserAuthBegin{Principal: p, VaultID: "user-vault", Owner: "crashed", TTL: time.Minute})
	require.NoError(t, e)
	_, e = s.UpdateUserAuth(context.Background(), core.UserAuthUpdate{Principal: p, Lease: gate.Lease, Revision: gate.Revision, State: core.UserAuthVerifying, ExpiresAt: time.Now().Add(time.Minute), Release: true})
	require.NoError(t, e)
	a := &userAuthFixture{}
	f := &conformance.MAServer{}
	options := testRuntimeOptions()
	options.MessageDeadline = time.Minute
	r, e := core.NewRuntime(s, []core.Connection{{Binding: b, Provider: &runtimeProvider{}, MAClient: newMAForHarness(t, f), UserAuthorizer: a}}, options)
	require.NoError(t, e)
	result := mustEnqueue(t, r, b.ID, m)
	runUntilCleanup(t, r)
	require.Eventually(t, func() bool {
		turn, e := s.GetTurn(context.Background(), result.Turn.ID)
		return e == nil && turn.Status == core.TurnFailed
	}, time.Second, 10*time.Millisecond)
	a.mu.Lock()
	require.Zero(t, a.starts)
	a.mu.Unlock()
	gate2, e := s.GetUserAuth(context.Background(), p)
	require.NoError(t, e)
	require.Equal(t, core.UserAuthFailed, gate2.State)
	ds, e := s.ListDeliveries(context.Background(), core.DeliveryQuery{})
	require.NoError(t, e)
	require.Len(t, ds, 1)
	require.NotContains(t, ds[0].Body.Text, "账号不符")
}

func (a *userAuthFixture) grant(vault string) core.UserGrant {
	scopes := a.scopes
	if scopes == nil {
		scopes = []string{"read"}
	}
	flow := a.flow
	if flow == "" {
		flow = "flow"
	}
	version := a.version
	if version == "" {
		version = "grant-1"
	}
	return core.UserGrant{Status: core.AuthReady, VaultID: vault, CredentialID: "user-credential", GrantVersion: version, FlowID: flow, GrantedScopes: append([]string(nil), scopes...)}
}

func userPrincipalFixture(b core.Binding) core.UserPrincipal {
	m := runtimeMessage()
	return core.UserPrincipal{Owner: b.Owner, Project: b.Project, ChannelID: b.ChannelID, Epoch: b.BindingEpoch, AppID: b.AppID, TenantKey: m.TenantKey, OpenID: m.SenderID}
}
func TestRuntimeUserScopeUpgradePreservesSessionAndCredential(t *testing.T) {
	ctx := context.Background()
	s := newUserStore(t)
	b := userBinding()
	p := userPrincipalFixture(b)
	credential := &core.UserCredentialSpec{Principal: p, VaultID: "user-vault", CredentialID: "user-credential", FlowID: "old-flow", GrantVersion: "old-version"}
	b.VaultIDs = []string{"user-vault"}
	snapshot := seedUserSession(t, s, b, credential)
	held, e := s.BeginUserAuth(ctx, core.UserAuthBegin{Principal: p, VaultID: credential.VaultID, Owner: "seed", TTL: time.Minute})
	require.NoError(t, e)
	_, e = s.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: held.Lease, Revision: held.Revision, State: core.UserAuthReady, Grant: core.UserGrant{Status: core.AuthReady, VaultID: credential.VaultID, CredentialID: credential.CredentialID, FlowID: "old-flow", GrantVersion: "old-version", GrantedScopes: []string{"read"}}})
	require.NoError(t, e)
	a := &userAuthFixture{ready: true, flow: "new-flow", version: "new-version"}
	f := &conformance.MAServer{}
	options := testRuntimeOptions()
	options.MessageDeadline = time.Minute
	current := b
	current.UserAuthScopes = []string{"write"}
	r, e := core.NewRuntime(s, []core.Connection{{Binding: current, Provider: &runtimeProvider{}, MAClient: newMAForHarness(t, f), UserAuthorizer: a}}, options)
	require.NoError(t, e)
	result := mustEnqueue(t, r, b.ID, runtimeMessage())
	runUntilCleanup(t, r)
	require.Eventually(t, func() bool { turn, e := s.GetTurn(ctx, result.Turn.ID); return e == nil && turn.AuthWait != nil }, time.Second, 10*time.Millisecond)
	a.mu.Lock()
	require.ElementsMatch(t, []string{"write"}, a.request.RequiredScopes)
	require.Equal(t, credential.CredentialID, a.request.ExistingCredentialID)
	a.scopes = []string{"read", "write"}
	a.mu.Unlock()
	f.AppendAfterInput(t, conformance.AnswerEvents("upgraded")...)
	require.Eventually(t, func() bool {
		turn, e := s.GetTurn(ctx, result.Turn.ID)
		return e == nil && turn.Status == core.TurnDone
	}, time.Second, 10*time.Millisecond)
	after, e := s.GetSessionSnapshot(ctx, snapshot.ID)
	require.NoError(t, e)
	require.Equal(t, snapshot, *after)
	gate, e := s.GetUserAuth(ctx, p)
	require.NoError(t, e)
	require.Equal(t, "new-version", gate.Grant.GrantVersion)
	f.Lock()
	require.Zero(t, f.CreateCount)
	require.Equal(t, 1, f.SendCount)
	f.Unlock()
}
func TestRuntimeUserSubjectMismatchRepliesAndPreservesOldSession(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "new", true: "existing"}[existing], func(t *testing.T) {
			ctx := context.Background()
			s := newUserStore(t)
			b := userBinding()
			var snapshot core.SessionSnapshot
			if existing {
				b.VaultIDs = []string{"user-vault"}
				snapshot = seedUserSession(t, s, b, &core.UserCredentialSpec{Principal: userPrincipalFixture(b), VaultID: "user-vault", CredentialID: "user-credential", GrantVersion: "old", FlowID: "old"})
			}
			a := &userAuthFixture{status: core.AuthSubjectMismatch}
			f := &conformance.MAServer{}
			options := testRuntimeOptions()
			options.MessageDeadline = time.Minute
			r, e := core.NewRuntime(s, []core.Connection{{Binding: b, Provider: &runtimeProvider{}, MAClient: &userVaultMA{MAClient: newMAForHarness(t, f)}, UserAuthorizer: a}}, options)
			require.NoError(t, e)
			result := mustEnqueue(t, r, b.ID, runtimeMessage())
			runUntilCleanup(t, r)
			require.Eventually(t, func() bool {
				turn, e := s.GetTurn(ctx, result.Turn.ID)
				return e == nil && turn.Status == core.TurnFailed
			}, 4*time.Second, 10*time.Millisecond)
			ds, e := s.ListDeliveries(ctx, core.DeliveryQuery{})
			require.NoError(t, e)
			require.Len(t, ds, 2)
			var reply string
			for _, d := range ds {
				if d.Source.Kind == core.SourceTurn {
					reply = d.Body.Text
				}
			}
			require.Contains(t, reply, "未继续执行")
			require.Contains(t, reply, "重新发送请求")
			require.Contains(t, reply, "发送消息的飞书账号")
			if existing {
				require.Contains(t, reply, "原会话已保留")
				conversation, e := s.GetConversation(ctx, result.Turn.ConversationID)
				require.NoError(t, e)
				require.Equal(t, snapshot.ID, conversation.Session.SnapshotID)
			} else {
				require.NotContains(t, reply, "会话已保留")
			}
			f.Lock()
			require.Zero(t, f.CreateCount)
			require.Zero(t, f.SendCount)
			f.Unlock()
		})
	}
}
func TestRuntimeUserAuthorizationAndQueueDeadlinesAreIndependent(t *testing.T) {
	ctx := context.Background()
	clock := conformance.NewClock(time.Now().UTC())
	s, e := memory.New(memory.Options{Clock: clock.Now, QueueTTL: 5 * time.Minute, PayloadCodec: &conformance.UserAuthTestCodec{}, PayloadNamespace: "test", RequirePayloadEncryption: true})
	require.NoError(t, e)
	defer func() { require.NoError(t, s.Close()) }()
	b := userBinding()
	f := &conformance.MAServer{}
	a := &userAuthFixture{}
	options := testRuntimeOptions()
	options.Clock = clock.Now
	options.MessageDeadline = 30 * time.Minute
	r, e := core.NewRuntime(s, []core.Connection{{Binding: b, Provider: &runtimeProvider{}, MAClient: &userVaultMA{MAClient: newMAForHarness(t, f)}, UserAuthorizer: a}}, options)
	require.NoError(t, e)
	first := mustEnqueue(t, r, b.ID, runtimeMessage())
	ctxRun, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- r.Run(ctxRun) }()
	defer func() { cancel(); require.NoError(t, <-done) }()
	// 跳逻辑时间前，等待授权 gate 与 Turn 持久等待完成并归还租约。
	waitAuthReleased := func() {
		require.Eventually(t, func() bool {
			turn, err := s.GetTurn(ctx, first.Turn.ID)
			if err != nil || turn.AuthWait == nil || turn.Lease.Owner != "" {
				return false
			}
			gate, err := s.GetUserAuth(ctx, userPrincipalFixture(b))
			return err == nil && gate != nil && gate.State == core.UserAuthPending && gate.Lease.Owner == ""
		}, time.Second, 10*time.Millisecond)
	}
	waitAuthReleased()
	m := runtimeMessage()
	m.MessageID = "queued"
	m.ProviderEventID = "queued"
	second := mustEnqueue(t, r, b.ID, m)
	clock.Advance(6 * time.Minute)
	require.Eventually(t, func() bool {
		turn, e := s.GetTurn(ctx, second.Turn.ID)
		return e == nil && turn.Status == core.TurnFailed && turn.Outcome.ErrorKind == "queue_deadline"
	}, time.Second, 10*time.Millisecond)
	turn, e := s.GetTurn(ctx, first.Turn.ID)
	require.NoError(t, e)
	require.False(t, turn.Status.Terminal())
	waitAuthReleased()
	clock.Advance(5 * time.Minute)
	require.Eventually(t, func() bool {
		turn, e := s.GetTurn(ctx, first.Turn.ID)
		return e == nil && turn.Status == core.TurnFailed && turn.Outcome.ErrorKind == "user_auth_timeout"
	}, time.Second, 10*time.Millisecond)
	gate, e := s.GetUserAuth(ctx, userPrincipalFixture(b))
	require.NoError(t, e)
	require.Equal(t, core.UserAuthFailed, gate.State)
	f.Lock()
	require.Zero(t, f.CreateCount)
	require.Zero(t, f.SendCount)
	f.Unlock()
}

// userRuntimeStore 打开指定持久后端；memory 复用 sqlstore，不单独列入矩阵。
func userRuntimeStore(t *testing.T, kind string) core.Store {
	t.Helper()
	codec := &conformance.UserAuthTestCodec{}
	if kind == "embedded" {
		s, e := openEmbeddedFixture(filepath.Join(t.TempDir(), "auth.db"), embedded.Options{NoSync: true, PayloadCodec: codec, PayloadNamespace: "test", RequirePayloadEncryption: true})
		require.NoError(t, e)
		t.Cleanup(func() { require.NoError(t, s.Close()) })
		return s
	}
	// 临时库验证授权与恢复语义，避免每次事务刷盘挤占 Runtime 的短执行窗口。
	db, e := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "auth.db")+"?_txlock=immediate&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)")
	require.NoError(t, e)
	db.SetMaxOpenConns(1)
	options := sqlstore.Options{PayloadCodec: codec, PayloadNamespace: "test", RequirePayloadEncryption: true}
	require.NoError(t, sqlstore.Install(context.Background(), db, sqlstore.SQLite, options))
	s, e := openSQLStore(context.Background(), db, sqlstore.SQLite, options)
	require.NoError(t, e)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

func TestRuntimeUserUnknownRemoteDrainsBeforeStartingAuthorization(t *testing.T) {
	ctx := context.Background()
	s := newUserStore(t)
	b := userBinding()
	p := userPrincipalFixture(b)
	b.VaultIDs = []string{"user-vault"}
	snapshot := seedUserSession(t, s, b, &core.UserCredentialSpec{Principal: p, VaultID: "user-vault", CredentialID: "user-credential", GrantVersion: "g1", FlowID: "f1"})
	gate, e := s.BeginUserAuth(ctx, core.UserAuthBegin{Principal: p, VaultID: "user-vault", Owner: "seed", TTL: time.Minute})
	require.NoError(t, e)
	_, e = s.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: gate.Lease, Revision: gate.Revision, State: core.UserAuthReady, Grant: core.UserGrant{Status: core.AuthReady, VaultID: "user-vault", CredentialID: "user-credential", GrantVersion: "g1", FlowID: "f1"}})
	require.NoError(t, e)
	m := runtimeMessage()
	m.MessageID = "unknown-write"
	m.ProviderEventID = "unknown-write"
	_, e = s.Ingest(ctx, core.NewBindingVersion(b), m)
	require.NoError(t, e)
	claim := conformance.Claim("lost-response")
	claim.UserAuth = true
	leased, e := s.LeaseTurn(ctx, claim)
	require.NoError(t, e)
	_, e = s.UpdateTurn(ctx, leased.Turn.Lease, core.TurnUpdate{Phase: core.PhaseSend, Session: core.TurnSession{SnapshotID: snapshot.ID, Generation: 1, SessionID: "sesn-demo", CreationKey: "seed-create"}, Submit: core.Submission{WriteAttempts: 1, SubmittedAt: time.Now()}, RemoteIntent: &core.RemoteIntent{Kind: core.RemoteInput, UserAuthKey: p.Key(), GrantVersion: "g1"}})
	require.NoError(t, e)
	require.NoError(t, s.SettleTurn(ctx, leased.Turn.Lease, core.TurnOutcome{Status: core.TurnFailed, ErrorKind: "send_unconfirmed"}, nil))
	f := &conformance.MAServer{}
	a := &userAuthFixture{}
	options := testRuntimeOptions()
	options.MessageDeadline = time.Minute
	r, e := core.NewRuntime(s, []core.Connection{{Binding: b, Provider: &runtimeProvider{}, MAClient: newMAForHarness(t, f), UserAuthorizer: a}}, options)
	require.NoError(t, e)
	next := mustEnqueue(t, r, b.ID, runtimeMessage())
	runUntilCleanup(t, r)
	require.Eventually(t, func() bool { g, e := s.GetUserAuth(ctx, p); return e == nil && g.State == core.UserAuthDraining }, time.Second, 10*time.Millisecond)
	turn, e := s.GetTurn(ctx, next.Turn.ID)
	require.NoError(t, e)
	require.Nil(t, turn.AuthWait)
	a.mu.Lock()
	require.Zero(t, a.starts)
	a.mu.Unlock()
	f.Lock()
	require.Zero(t, f.SendCount)
	require.Zero(t, f.CreateCount)
	f.Unlock()
}
func TestRuntimeUserStopAndNewDoNotReplayOriginalInput(t *testing.T) {
	ctx := context.Background()
	s := newUserStore(t)
	b := userBinding()
	f := &conformance.MAServer{EnvironmentType: "cloud"}
	a := &userAuthFixture{}
	options := testRuntimeOptions()
	options.MessageDeadline = time.Minute
	r, e := core.NewRuntime(s, []core.Connection{{Binding: b, Provider: &runtimeProvider{}, MAClient: &userVaultMA{MAClient: newMAForHarness(t, f)}, UserAuthorizer: a}}, options)
	require.NoError(t, e)
	original := mustEnqueue(t, r, b.ID, runtimeMessage())
	runUntilCleanup(t, r)
	require.Eventually(t, func() bool { turn, e := s.GetTurn(ctx, original.Turn.ID); return e == nil && turn.AuthWait != nil }, time.Second, 10*time.Millisecond)
	stop := runtimeMessage()
	stop.MessageID = "stop"
	stop.ProviderEventID = "stop"
	stop.Text = "/stop"
	mustEnqueue(t, r, b.ID, stop)
	require.Eventually(t, func() bool {
		turn, e := s.GetTurn(ctx, original.Turn.ID)
		return e == nil && turn.Status == core.TurnStopped
	}, time.Second, 10*time.Millisecond)
	a.mu.Lock()
	a.ready = true
	a.mu.Unlock()
	fresh := runtimeMessage()
	fresh.MessageID = "new"
	fresh.ProviderEventID = "new"
	fresh.Text = "/new"
	result := mustEnqueue(t, r, b.ID, fresh)
	require.Eventually(t, func() bool {
		turn, e := s.GetTurn(ctx, result.Turn.ID)
		return e == nil && turn.Status == core.TurnDone
	}, 3*time.Second, 10*time.Millisecond)
	f.Lock()
	require.Equal(t, 1, f.CreateCount)
	require.Zero(t, f.SendCount)
	require.Zero(t, f.InterruptCount)
	f.Unlock()
}

func TestRuntimeUserGateBlocksConfirmationWhileDraining(t *testing.T) {
	ctx := context.Background()
	s := newUserStore(t)
	b := userBinding()
	b.Approvals = true
	f := &conformance.MAServer{}
	a := &userAuthFixture{ready: true}
	p := &approvalProvider{}
	options := testRuntimeOptions()
	options.MessageDeadline = time.Minute
	r, e := core.NewRuntime(s, []core.Connection{{Binding: b, Provider: p, MAClient: &userVaultMA{MAClient: newMAForHarness(t, f)}, UserAuthorizer: a}}, options)
	require.NoError(t, e)
	result := mustEnqueue(t, r, b.ID, runtimeMessage())
	runUntilCleanup(t, r)
	f.AppendAfterInput(t, askEvents()...)
	require.Eventually(t, func() bool { return len(p.prompted()) == 1 }, time.Second, 10*time.Millisecond)
	_, e = s.BeginUserAuth(ctx, core.UserAuthBegin{Principal: userPrincipalFixture(b), VaultID: "user-vault", Owner: "reauth", TTL: time.Minute})
	require.ErrorIs(t, e, core.ErrRemoteBarrier)
	require.NoError(t, r.SubmitApproval(ctx, core.ApprovalDecision{ChatID: "chat", TurnID: result.Turn.ID, ToolUseID: "tool-ask", Allow: true, DeciderID: "user"}))
	f.Lock()
	require.Empty(t, f.Confirmations)
	f.Unlock()
}

func TestRuntimeUserLateReadyCannotExtendOriginalAuthorizationDeadline(t *testing.T) {
	ctx := context.Background()
	clock := conformance.NewClock(time.Now().UTC())
	s, e := memory.New(memory.Options{Clock: clock.Now, PayloadCodec: &conformance.UserAuthTestCodec{}, PayloadNamespace: "test", RequirePayloadEncryption: true})
	require.NoError(t, e)
	defer func() { require.NoError(t, s.Close()) }()
	b := userBinding()
	p := userPrincipalFixture(b)
	result, e := s.Ingest(ctx, core.NewBindingVersion(b), runtimeMessage())
	require.NoError(t, e)
	claim := conformance.Claim("before-restart")
	claim.UserAuth = true
	leased, e := s.LeaseTurn(ctx, claim)
	require.NoError(t, e)
	turn, e := s.UpdateTurn(ctx, leased.Turn.Lease, core.TurnUpdate{Phase: core.PhaseCreate, Session: core.TurnSession{Generation: 1, CreationKey: "pending-create"}})
	require.NoError(t, e)
	begin := core.UserAuthBegin{Principal: p, VaultID: "user-vault", Owner: "authorizer", TTL: time.Minute}
	gate, e := s.BeginUserAuth(ctx, begin)
	require.NoError(t, e)
	deadline := clock.Now().Add(time.Minute)
	gate, e = s.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: gate.Lease, Revision: gate.Revision, State: core.UserAuthPending, FlowID: "flow", ExpiresAt: deadline})
	require.NoError(t, e)
	require.NoError(t, s.WaitForUserAuth(ctx, turn.Lease, core.UserAuthWait{Principal: p, Lease: gate.Lease, Revision: gate.Revision, Attempt: gate.Attempt, FlowID: "flow", DeadlineAt: deadline, Until: deadline}))
	clock.Advance(2 * time.Minute)
	gate, e = s.BeginUserAuth(ctx, begin)
	require.NoError(t, e)
	a := &userAuthFixture{ready: true}
	grant := a.grant("user-vault")
	_, e = s.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: gate.Lease, Revision: gate.Revision, State: core.UserAuthReady, Grant: grant})
	require.NoError(t, e)
	f := &conformance.MAServer{}
	options := testRuntimeOptions()
	options.Clock = clock.Now
	options.MessageDeadline = time.Hour
	r, e := core.NewRuntime(s, []core.Connection{{Binding: b, Provider: &runtimeProvider{}, MAClient: &userVaultMA{MAClient: newMAForHarness(t, f)}, UserAuthorizer: a}}, options)
	require.NoError(t, e)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- r.Run(runCtx) }()
	defer func() { cancel(); require.NoError(t, <-done) }()
	require.Eventually(t, func() bool {
		turn, e := s.GetTurn(ctx, result.Turn.ID)
		return e == nil && turn.Status == core.TurnFailed && turn.Outcome.ErrorKind == "user_auth_timeout"
	}, time.Second, 10*time.Millisecond)
	f.Lock()
	require.Zero(t, f.SendCount)
	require.Zero(t, f.CreateCount)
	f.Unlock()
}

func TestRuntimeUserExpiredVerifyingPreservesUnknownCiphertext(t *testing.T) {
	ctx := context.Background()
	clock := conformance.NewClock(time.Now().UTC())
	s, e := memory.New(memory.Options{Clock: clock.Now, PayloadCodec: &conformance.UserAuthTestCodec{}, PayloadNamespace: "test", RequirePayloadEncryption: true})
	require.NoError(t, e)
	defer func() { require.NoError(t, s.Close()) }()
	b := userBinding()
	p := userPrincipalFixture(b)
	result, e := s.Ingest(ctx, core.NewBindingVersion(b), runtimeMessage())
	require.NoError(t, e)
	claim := conformance.Claim("before-restart")
	claim.UserAuth = true
	leased, e := s.LeaseTurn(ctx, claim)
	require.NoError(t, e)
	turn, e := s.UpdateTurn(ctx, leased.Turn.Lease, core.TurnUpdate{Phase: core.PhaseCreate, Session: core.TurnSession{Generation: 1, CreationKey: "pending-create"}})
	require.NoError(t, e)
	gate, e := s.BeginUserAuth(ctx, core.UserAuthBegin{Principal: p, VaultID: "user-vault", Owner: "authorizer", TTL: time.Minute})
	require.NoError(t, e)
	secret := &core.UserAuthSecret{Version: 1, KeyID: "key", Ciphertext: []byte("opaque-exchange-unknown")}
	deadline := clock.Now().Add(time.Minute)
	gate, e = s.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: gate.Lease, Revision: gate.Revision, State: core.UserAuthVerifying, FlowID: "flow", ExpiresAt: deadline, Secret: secret})
	require.NoError(t, e)
	require.NoError(t, s.WaitForUserAuth(ctx, turn.Lease, core.UserAuthWait{Principal: p, Lease: gate.Lease, Revision: gate.Revision, Attempt: gate.Attempt, FlowID: "flow", DeadlineAt: deadline, Until: deadline}))
	clock.Advance(2 * time.Minute)
	f := &conformance.MAServer{}
	a := &userAuthFixture{}
	options := testRuntimeOptions()
	options.Clock = clock.Now
	options.MessageDeadline = time.Hour
	r, e := core.NewRuntime(s, []core.Connection{{Binding: b, Provider: &runtimeProvider{}, MAClient: newMAForHarness(t, f), UserAuthorizer: a}}, options)
	require.NoError(t, e)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- r.Run(runCtx) }()
	defer func() { cancel(); require.NoError(t, <-done) }()
	require.Eventually(t, func() bool {
		turn, e := s.GetTurn(ctx, result.Turn.ID)
		return e == nil && turn.Status == core.TurnFailed
	}, time.Second, 10*time.Millisecond)
	saved, e := s.GetUserAuth(ctx, p)
	require.NoError(t, e)
	require.Equal(t, core.UserAuthUncertain, saved.State)
	require.Equal(t, secret, saved.Secret)
	a.mu.Lock()
	require.Zero(t, a.polls)
	require.Zero(t, a.starts)
	a.mu.Unlock()
}

func TestRuntimeUserExpiredRefreshFailsReliablyWithoutHTTP(t *testing.T) {
	ctx := context.Background()
	clock := conformance.NewClock(time.Now().UTC())
	s, e := memory.New(memory.Options{Clock: clock.Now, PayloadCodec: &conformance.UserAuthTestCodec{}, PayloadNamespace: "test", RequirePayloadEncryption: true})
	require.NoError(t, e)
	defer func() { require.NoError(t, s.Close()) }()
	b := userBinding()
	p := userPrincipalFixture(b)
	begin := core.UserAuthBegin{Principal: p, VaultID: "user-vault", Owner: "crashed-refresh", TTL: time.Minute}
	gate, e := s.BeginUserAuth(ctx, begin)
	require.NoError(t, e)
	a := &userAuthFixture{ready: true}
	secret := &core.UserAuthSecret{Version: 1, KeyID: "key", Ciphertext: []byte("opaque-refresh-unknown")}
	_, e = s.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: p, Lease: gate.Lease, Revision: gate.Revision, State: core.UserAuthReady, Grant: a.grant("user-vault"), Secret: secret})
	require.NoError(t, e)
	_, e = s.BeginUserAuthRefresh(ctx, begin)
	require.NoError(t, e)
	clock.Advance(2 * time.Minute)
	f := &conformance.MAServer{}
	options := testRuntimeOptions()
	options.Clock = clock.Now
	r, e := core.NewRuntime(s, []core.Connection{{Binding: b, Provider: &runtimeProvider{}, MAClient: newMAForHarness(t, f), UserAuthorizer: a}}, options)
	require.NoError(t, e)
	result := mustEnqueue(t, r, b.ID, runtimeMessage())
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- r.Run(runCtx) }()
	defer func() { cancel(); require.NoError(t, <-done) }()
	require.Eventually(t, func() bool {
		turn, e := s.GetTurn(ctx, result.Turn.ID)
		return e == nil && turn.Status == core.TurnFailed
	}, time.Second, 10*time.Millisecond)
	saved, e := s.GetUserAuth(ctx, p)
	require.NoError(t, e)
	require.Equal(t, core.UserAuthUncertain, saved.State)
	require.Equal(t, secret, saved.Secret)
	ds, e := s.ListDeliveries(ctx, core.DeliveryQuery{})
	require.NoError(t, e)
	require.Len(t, ds, 1)
	require.Contains(t, ds[0].Body.Text, "无法确认授权结果")
	a.mu.Lock()
	require.Empty(t, a.query)
	require.Zero(t, a.starts)
	require.Zero(t, a.polls)
	a.mu.Unlock()
}

func TestRuntimeUserRenewedSessionUsesNewIdentityPreparation(t *testing.T) {
	s := newUserStore(t)
	b := userBinding()
	old := b
	old.Identity = core.IdentityPolicy{}
	seedUserSession(t, s, old, nil)
	f := &conformance.MAServer{SessionStatus: map[string]string{"sesn-demo": "terminated"}, EnvironmentType: "cloud"}
	a := &userAuthFixture{}
	options := testRuntimeOptions()
	options.MessageDeadline = time.Minute
	r, e := core.NewRuntime(s, []core.Connection{{Binding: b, Provider: &runtimeProvider{}, MAClient: &userVaultMA{MAClient: newMAForHarness(t, f)}, UserAuthorizer: a}}, options)
	require.NoError(t, e)
	result := mustEnqueue(t, r, b.ID, runtimeMessage())
	runUntilCleanup(t, r)
	require.Eventually(t, func() bool {
		turn, e := s.GetTurn(context.Background(), result.Turn.ID)
		return e == nil && turn.AuthWait != nil
	}, time.Second, 10*time.Millisecond)
	a.mu.Lock()
	require.Equal(t, 1, a.starts)
	a.mu.Unlock()
	f.Lock()
	require.Zero(t, f.CreateCount)
	require.Zero(t, f.SendCount)
	f.Unlock()
}
func TestRuntimeUserAuthorizerDoesNotDisableBotValidation(t *testing.T) {
	b := userBinding()
	b.Identity = core.IdentityPolicy{P2P: core.IdentityBot}
	_, e := core.NewRuntime(newUserStore(t), []core.Connection{{Binding: b, Provider: &runtimeProvider{}, MAClient: newMAForHarness(t, &conformance.MAServer{}), UserAuthorizer: &userAuthFixture{}}}, testRuntimeOptions())
	require.ErrorIs(t, e, core.ErrRuntimeConfig)
}

// userRuntimeFixture 为前置授权测试设置一致的执行预算。
func userRuntimeFixture() (core.Binding, core.RuntimeOptions) {
	options := testRuntimeOptions()
	options.MessageDeadline = time.Minute
	options.TurnTimeout, options.LeaseTTL = 5*time.Second, 10*time.Second
	binding := userBinding()
	binding.MaxTurnDuration = options.TurnTimeout
	return binding, options
}
