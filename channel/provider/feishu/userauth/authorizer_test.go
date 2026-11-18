// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package userauth

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	storecodec "github.com/volcengine/ark-runtime-go/channel/store"

	_ "modernc.org/sqlite" // sqlite 测试驱动

	"github.com/stretchr/testify/require"
	"github.com/volcengine/ark-runtime-go/arkruntime"

	"github.com/volcengine/ark-runtime-go/channel"
	"github.com/volcengine/ark-runtime-go/channel/internal/handles"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/codec"
	"github.com/volcengine/ark-runtime-go/channel/ma"
	"github.com/volcengine/ark-runtime-go/channel/provider/feishu/oauth"
	"github.com/volcengine/ark-runtime-go/channel/store/embedded"
	sqlstore "github.com/volcengine/ark-runtime-go/channel/store/sql"
)

type authStore interface {
	core.Store
	core.UserAuthStore
}
type fixture struct {
	driver                           string
	refreshEntered                   chan struct{}
	refreshRelease                   chan struct{}
	t                                *testing.T
	now                              time.Time
	store                            authStore
	options                          embedded.Options
	path                             string
	auth                             *Authorizer
	oauth                            *oauth.Client
	ma                               *ma.Client
	crypt                            codec.Codec
	p                                core.UserPrincipal
	polls, writes, begins, refreshes int
	subject                          string
	pollBody, refreshBody            string
	unknown                          bool
}

func newFixture(t *testing.T, drivers ...string) *fixture {
	f := &fixture{t: t, now: time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC), subject: "user", pollBody: `{"access_token":"uat-secret","refresh_token":"refresh-secret","expires_in":3600,"scope":"read"}`, refreshBody: `{"access_token":"uat-rotated","refresh_token":"refresh-rotated","expires_in":3600}`}
	if len(drivers) > 0 {
		f.driver = drivers[0]
	}
	f.p = core.UserPrincipal{Owner: "owner", Project: "project", ChannelID: "channel", Epoch: 1, AppID: "app", TenantKey: "tenant", OpenID: "user"}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/oauth/v1/device_authorization":
			f.begins++
			_, _ = fmt.Fprint(w, `{"device_code":"device-secret","user_code":"ABCD-EFGH","verification_uri":"https://accounts.feishu.cn/device","expires_in":600,"interval":5}`)
		case "/open-apis/authen/v2/oauth/token":
			require.NoError(t, r.ParseForm())
			if r.Form.Get("grant_type") == "refresh_token" {
				f.refreshes++
				if f.refreshEntered != nil {
					f.refreshEntered <- struct{}{}
					select {
					case <-f.refreshRelease:
					case <-r.Context().Done():
						return
					}
				}
				require.Equal(t, "refresh-secret", r.Form.Get("refresh_token"))
				_, _ = fmt.Fprint(w, f.refreshBody)
			} else {
				f.polls++
				_, _ = fmt.Fprint(w, f.pollBody)
			}
		case "/open-apis/authen/v1/user_info":
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]string{"open_id": f.subject, "tenant_key": "tenant"}})
		case "/api/v3/vaults/vault":
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "vault", "type": "vault", "display_name": "fixture", "created_at": "2026-10-13T00:00:00Z", "updated_at": "2026-10-13T00:00:00Z", "metadata": map[string]string{"ark_channel_principal": f.p.Key()}})
		case "/api/v3/vaults/vault/credentials/credential":
			if r.Method == "GET" {
				_ = json.NewEncoder(w).Encode(map[string]any{"id": "credential", "type": "vault_credential", "vault_id": "vault", "created_at": "2026-10-13T00:00:00Z", "updated_at": "2026-10-13T00:00:00Z", "auth": map[string]string{"type": "environment_variable", "secret_name": ma.UserAccessTokenSecretName}, "metadata": map[string]string{"managed_by": "ark-agent-channel", "ark_channel_principal": f.p.Key()}})
				return
			}
			fallthrough
		case "/api/v3/vaults/vault/credentials":
			f.writes++
			if f.unknown {
				w.WriteHeader(500)
				_, _ = fmt.Fprint(w, `{"secret":"uat-secret"}`)
			} else {
				_, _ = fmt.Fprint(w, `{"id":"credential","type":"vault_credential","vault_id":"vault","auth":{"type":"environment_variable"},"created_at":"2026-10-13T00:00:00Z","updated_at":"2026-10-13T00:00:00Z"}`)
			}
		default:
			t.Errorf("unexpected %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(server.Close)
	var err error
	f.oauth, err = oauth.New(oauth.Config{AppID: "app", AppSecret: "app-secret", AccountsBaseURL: server.URL, OpenBaseURL: server.URL, HTTPClient: server.Client(), Clock: func() time.Time { return f.now }})
	require.NoError(t, err)
	f.ma, err = ma.NewClient(server.URL, "key", ma.ClientOptions{SDKClient: arkruntime.NewClientWithApiKey("key", arkruntime.WithBaseUrl(server.URL+"/api/v3"), arkruntime.WithRetryTimes(0), arkruntime.WithHTTPClient(server.Client()))})
	require.NoError(t, err)
	f.crypt, err = storecodec.NewAESGCM("key", []byte(strings.Repeat("x", 32)))
	require.NoError(t, err)
	f.path = filepath.Join(t.TempDir(), "state.db")
	f.options = embedded.Options{Clock: func() time.Time { return f.now }, PayloadCodec: f.crypt, PayloadNamespace: "ns", RequirePayloadEncryption: true}
	if f.driver == "sqlite" {
		db, e := sql.Open("sqlite", f.path)
		require.NoError(t, e)
		require.NoError(t, sqlstore.Install(context.Background(), db, sqlstore.SQLite, f.sqlOptions()))
		require.NoError(t, db.Close())
	} else {
		require.NoError(t, embedded.Install(f.path, f.options))
	}
	f.open()
	t.Cleanup(func() { require.NoError(t, f.store.Close()) })
	return f
}
func (f *fixture) open() {
	var err error
	var handle *channel.Store
	if f.driver == "sqlite" {
		db, e := sql.Open("sqlite", f.path+"?_txlock=immediate&_pragma=busy_timeout(5000)")
		require.NoError(f.t, e)
		db.SetMaxOpenConns(1)
		handle, err = sqlstore.Open(context.Background(), db, sqlstore.SQLite, f.sqlOptions())
	} else {
		handle, err = embedded.Open(f.path, f.options)
	}
	require.NoError(f.t, err)
	f.store = handles.StoreValue(handle).(authStore)
	f.auth, err = New(Config{AppID: "app", OAuth: f.oauth, Credentials: f.ma, Store: f.store, Codec: f.crypt, Clock: func() time.Time { return f.now }})
	require.NoError(f.t, err)
}
func (f *fixture) start() core.AuthFlow {
	r, err := f.store.BeginUserAuth(context.Background(), core.UserAuthBegin{Principal: f.p, VaultID: "vault", Owner: "runtime", TTL: time.Hour})
	require.NoError(f.t, err)
	r, err = f.store.UpdateUserAuth(context.Background(), core.UserAuthUpdate{Principal: f.p, Lease: r.Lease, Revision: r.Revision, State: core.UserAuthVerifying, ExpiresAt: f.now.Add(10 * time.Minute)})
	require.NoError(f.t, err)
	flow, err := f.auth.Start(context.Background(), f.p, core.AuthRequest{VaultID: "vault", ExistingCredentialID: r.CredentialID, Lease: r.Lease, Revision: r.Revision, Attempt: r.Attempt, RequiredScopes: []string{"read"}})
	require.NoError(f.t, err)
	return flow
}
func (f *fixture) status() core.AuthStatus {
	r, err := f.store.GetUserAuth(context.Background(), f.p)
	require.NoError(f.t, err)
	s, err := f.auth.Status(context.Background(), core.AuthFlowRef{Principal: f.p, VaultID: "vault", FlowID: r.FlowID, Lease: r.Lease, Revision: r.Revision, RequiredScopes: []string{"read"}})
	require.NoError(f.t, err)
	return s
}
func (f *fixture) ready(s core.AuthStatus) {
	r, err := f.store.GetUserAuth(context.Background(), f.p)
	require.NoError(f.t, err)
	_, err = f.store.UpdateUserAuth(context.Background(), core.UserAuthUpdate{Principal: f.p, Lease: r.Lease, Revision: r.Revision, State: core.UserAuthReady, Grant: s.Grant})
	require.NoError(f.t, err)
}

func TestDurableDevicePendingSlowDownAndEncryptedReopen(t *testing.T) {
	f := newFixture(t)
	flow := f.start()
	require.Equal(t, "ABCD-EFGH", flow.UserCode)
	require.NotContains(t, fmt.Sprintf("%+v %#v", flow, &flow), "ABCD")
	require.NoError(t, f.store.Close())
	f.open()
	r, err := f.store.GetUserAuth(context.Background(), f.p)
	require.NoError(t, err)
	raw, _ := json.Marshal(r)
	require.NotContains(t, string(raw), "device-secret")
	require.NotContains(t, string(raw), "ABCD")
	require.NotContains(t, string(raw), "accounts.feishu")
	recovered, err := f.auth.AuthPrompt(context.Background(), core.AuthFlowRef{Principal: f.p, VaultID: "vault", FlowID: r.FlowID})
	require.NoError(t, err)
	require.Equal(t, flow, recovered)
	require.Equal(t, 1, f.begins)
	require.Equal(t, core.AuthPending, f.status().Status)
	require.Zero(t, f.polls)
	f.now = f.now.Add(5 * time.Second)
	f.pollBody = `{"error":"slow_down"}`
	s := f.status()
	require.Equal(t, f.now.Add(10*time.Second), s.NextPollAt)
	f.now = f.now.Add(9 * time.Second)
	require.Equal(t, core.AuthPending, f.status().Status)
	require.Equal(t, 1, f.polls)
	f.now = f.now.Add(time.Second)
	f.pollBody = `{"error":"authorization_pending"}`
	require.Equal(t, core.AuthPending, f.status().Status)
	require.Equal(t, 2, f.polls)
}
func TestDeviceVerifiesSubjectAndActualScopeBeforePublicWrite(t *testing.T) {
	for _, mode := range []string{"wrong_subject", "unknown_scope", "empty_scope", "success"} {
		t.Run(mode, func(t *testing.T) {
			f := newFixture(t)
			f.start()
			f.now = f.now.Add(5 * time.Second)
			switch mode {
			case "wrong_subject":
				f.subject = "wrong"
			case "unknown_scope":
				f.pollBody = `{"access_token":"uat-secret","expires_in":3600}`
			case "empty_scope":
				f.pollBody = `{"access_token":"uat-secret","expires_in":3600,"scope":""}`
			}
			s := f.status()
			if mode != "success" {
				require.NotEqual(t, core.AuthReady, s.Status)
				require.Zero(t, f.writes)
				return
			}
			require.Equal(t, core.AuthReady, s.Status)
			require.Equal(t, 1, f.writes)
			r, e := f.store.GetUserAuth(context.Background(), f.p)
			require.NoError(t, e)
			require.Equal(t, "credential", r.CredentialID)
			require.Equal(t, core.UserAuthVerifying, r.State)
			require.NotEmpty(t, r.Lease.Owner)
		})
	}
}
func TestDeviceSyncUnknownNeverReplaysAfterReopen(t *testing.T) {
	f := newFixture(t)
	f.start()
	f.now = f.now.Add(5 * time.Second)
	f.unknown = true
	require.Equal(t, core.AuthUnknown, f.status().Status)
	require.Equal(t, 1, f.writes)
	require.NoError(t, f.store.Close())
	f.open()
	require.Equal(t, core.AuthUnknown, f.status().Status)
	require.Equal(t, 1, f.writes)
	require.Equal(t, 1, f.polls)
}
func TestDeviceRefreshRotationKeepsFlowDeadlineAndGrantVersion(t *testing.T) {
	f := newFixture(t)
	flow := f.start()
	f.now = f.now.Add(5 * time.Second)
	s := f.status()
	require.Equal(t, core.AuthReady, s.Status)
	f.ready(s)
	f.now = f.now.Add(time.Hour)
	g, err := f.auth.Lookup(context.Background(), core.UserGrantQuery{Principal: f.p, VaultID: "vault", CredentialID: "credential", RequiredScopes: []string{"read"}})
	require.NoError(t, err)
	require.Equal(t, core.AuthReady, g.Status)
	require.Equal(t, s.Grant.GrantVersion, g.GrantVersion)
	require.Equal(t, 1, f.refreshes)
	require.Equal(t, 2, f.writes)
	r, err := f.store.GetUserAuth(context.Background(), f.p)
	require.NoError(t, err)
	require.Equal(t, flow.ExpiresAt, r.ExpiresAt)
	require.NoError(t, f.store.Close())
	f.open()
	g, err = f.auth.Lookup(context.Background(), core.UserGrantQuery{Principal: f.p, VaultID: "vault", CredentialID: "credential", RequiredScopes: []string{"read"}})
	require.NoError(t, err)
	require.Equal(t, core.AuthReady, g.Status)
	require.Equal(t, 1, f.refreshes)
}

func TestDeviceExpiredTokenNeverWritesCredential(t *testing.T) {
	f := newFixture(t)
	f.start()
	f.now = f.now.Add(5 * time.Second)
	r, err := f.store.GetUserAuth(context.Background(), f.p)
	require.NoError(t, err)
	secret, err := f.auth.decrypt(context.Background(), *r)
	require.NoError(t, err)
	secret.Phase = phaseTokens
	secret.Tokens = oauth.Tokens{AccessToken: "expired-secret", ExpiresAt: f.now.Add(-time.Second), ScopeKnown: true, GrantedScopes: []string{"read"}}
	_, err = f.auth.checkpoint(*r, secret, core.UserAuthVerifying, "")
	require.NoError(t, err)
	require.Equal(t, core.AuthReauthRequired, f.status().Status)
	require.Zero(t, f.writes)
}

func (f *fixture) sqlOptions() sqlstore.Options {
	return sqlstore.Options{Clock: func() time.Time { return f.now }, PayloadCodec: f.crypt, PayloadNamespace: "ns", RequirePayloadEncryption: true}
}
func TestDeviceSQLiteTokenExchangeUnknownAndExpiredLeaseNeverReplays(t *testing.T) {
	f := newFixture(t, "sqlite")
	f.start()
	f.now = f.now.Add(5 * time.Second)
	f.pollBody = `{"access_token":"lost-token","expires_in":"corrupt"}`
	require.Equal(t, core.AuthUnknown, f.status().Status)
	require.NoError(t, f.store.Close())
	f.open()
	r, err := f.store.GetUserAuth(context.Background(), f.p)
	require.NoError(t, err)
	_, err = f.store.UpdateUserAuth(context.Background(), core.UserAuthUpdate{Principal: f.p, Lease: r.Lease, Revision: r.Revision, State: core.UserAuthUncertain})
	require.NoError(t, err)
	f.now = f.now.Add(2 * time.Hour)
	_, err = f.store.BeginUserAuth(context.Background(), core.UserAuthBegin{Principal: f.p, VaultID: "vault", Owner: "new-process", TTL: time.Minute})
	require.ErrorIs(t, err, core.ErrUserAuthGate)
	g, err := f.auth.Lookup(context.Background(), core.UserGrantQuery{Principal: f.p, VaultID: "vault"})
	require.NoError(t, err)
	require.Equal(t, core.AuthUnknown, g.Status)
	require.Equal(t, 1, f.polls)
	require.Zero(t, f.writes)
	require.Equal(t, 1, f.begins)
}
func TestDeviceRefreshSingleFlightAndReopen(t *testing.T) {
	f := newFixture(t, "sqlite")
	f.start()
	f.now = f.now.Add(5 * time.Second)
	status := f.status()
	f.ready(status)
	f.now = f.now.Add(time.Hour)
	f.refreshEntered = make(chan struct{}, 1)
	f.refreshRelease = make(chan struct{})
	query := core.UserGrantQuery{Principal: f.p, VaultID: "vault", CredentialID: "credential", RequiredScopes: []string{"read"}}
	result := make(chan core.UserGrant, 1)
	failure := make(chan error, 1)
	go func() { g, e := f.auth.Lookup(context.Background(), query); result <- g; failure <- e }()
	select {
	case <-f.refreshEntered:
	case <-time.After(time.Second):
		t.Fatal("refresh did not start")
	}
	var wg sync.WaitGroup
	var pending atomic.Int32
	for range 5 {
		wg.Go(func() {
			g, e := f.auth.Lookup(context.Background(), query)
			if e == nil && g.Status == core.AuthPending {
				pending.Add(1)
			}
		})
	}
	wg.Wait()
	require.EqualValues(t, 5, pending.Load())
	close(f.refreshRelease)
	require.Equal(t, core.AuthReady, (<-result).Status)
	require.NoError(t, <-failure)
	require.Equal(t, 1, f.refreshes)
	require.NoError(t, f.store.Close())
	f.open()
	g, e := f.auth.Lookup(context.Background(), query)
	require.NoError(t, e)
	require.Equal(t, core.AuthReady, g.Status)
	require.Equal(t, 1, f.refreshes)
}
func TestDeviceNoRefreshTokenRequiresReauthorizationAtExpiry(t *testing.T) {
	f := newFixture(t)
	f.start()
	f.now = f.now.Add(5 * time.Second)
	f.pollBody = `{"access_token":"uat-secret","expires_in":60,"scope":"read"}`
	s := f.status()
	f.ready(s)
	f.now = f.now.Add(time.Minute)
	g, e := f.auth.Lookup(context.Background(), core.UserGrantQuery{Principal: f.p, VaultID: "vault", CredentialID: "credential", RequiredScopes: []string{"read"}})
	require.NoError(t, e)
	require.Equal(t, core.AuthReauthRequired, g.Status)
	require.Zero(t, f.refreshes)
}
func TestDeviceRefreshExplicitEmptyScopesCannotInherit(t *testing.T) {
	f := newFixture(t)
	f.start()
	f.now = f.now.Add(5 * time.Second)
	s := f.status()
	f.ready(s)
	f.now = f.now.Add(time.Hour)
	f.refreshBody = `{"access_token":"rotated","expires_in":3600,"scope":""}`
	g, e := f.auth.Lookup(context.Background(), core.UserGrantQuery{Principal: f.p, VaultID: "vault", CredentialID: "credential", RequiredScopes: []string{"read"}})
	require.NoError(t, e)
	require.Equal(t, core.AuthReauthRequired, g.Status)
	require.Equal(t, 1, f.writes)
}
func TestDeviceCancelledBeforePollDoesNotSend(t *testing.T) {
	f := newFixture(t)
	f.start()
	f.now = f.now.Add(5 * time.Second)
	r, e := f.store.GetUserAuth(context.Background(), f.p)
	require.NoError(t, e)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, e = f.auth.Status(ctx, core.AuthFlowRef{Principal: f.p, VaultID: "vault", FlowID: r.FlowID, Lease: r.Lease, Revision: r.Revision})
	require.Error(t, e)
	require.Zero(t, f.polls)
}
func TestDeviceRefreshUnknownNeverRetriesOrOpensGate(t *testing.T) {
	f := newFixture(t)
	f.start()
	f.now = f.now.Add(5 * time.Second)
	s := f.status()
	f.ready(s)
	f.now = f.now.Add(time.Hour)
	f.refreshBody = `{"access_token":"lost","expires_in":null}`
	q := core.UserGrantQuery{Principal: f.p, VaultID: "vault", CredentialID: "credential", RequiredScopes: []string{"read"}}
	g, e := f.auth.Lookup(context.Background(), q)
	require.NoError(t, e)
	require.Equal(t, core.AuthUnknown, g.Status)
	require.NoError(t, f.store.Close())
	f.open()
	g, e = f.auth.Lookup(context.Background(), q)
	require.NoError(t, e)
	require.Equal(t, core.AuthUnknown, g.Status)
	require.Equal(t, 1, f.refreshes)
	require.Equal(t, 1, f.writes)
}

func TestDeviceCancelAfterExchangeSentPersistsUnknown(t *testing.T) {
	f := newFixture(t)
	f.start()
	f.now = f.now.Add(5 * time.Second)
	entered := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		_, _ = fmt.Fprint(w, `{"access_token":"late-secret","expires_in":3600,"scope":"read"}`)
	}))
	defer server.Close()
	protocol, err := oauth.New(oauth.Config{AppID: "app", AppSecret: "app-secret", OpenBaseURL: server.URL, HTTPClient: server.Client(), Clock: func() time.Time { return f.now }})
	require.NoError(t, err)
	f.auth.config.OAuth = protocol
	r, err := f.store.GetUserAuth(context.Background(), f.p)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan core.AuthStatus, 1)
	fail := make(chan error, 1)
	go func() {
		s, e := f.auth.Status(ctx, core.AuthFlowRef{Principal: f.p, VaultID: "vault", FlowID: r.FlowID, Lease: r.Lease, Revision: r.Revision, RequiredScopes: []string{"read"}})
		result <- s
		fail <- e
	}()
	<-entered
	cancel()
	require.Equal(t, core.AuthUnknown, (<-result).Status)
	require.NoError(t, <-fail)
	close(release)
	require.NoError(t, f.store.Close())
	f.open()
	require.Equal(t, core.AuthUnknown, f.status().Status)
	require.Zero(t, f.writes)
}

type expiringCredentialWriter struct {
	ma.UserCredentialWriter
	clock *time.Time
}

func (w expiringCredentialWriter) SyncUserCredential(ctx context.Context, target ma.UserCredentialTarget, value string) (string, error) {
	id, err := w.UserCredentialWriter.SyncUserCredential(ctx, target, value)
	*w.clock = w.clock.Add(time.Minute)
	return id, err
}
func TestDeviceTokenExpiresDuringSyncNeverReturnsReady(t *testing.T) {
	f := newFixture(t)
	f.pollBody = `{"access_token":"uat-secret","expires_in":60,"scope":"read"}`
	f.start()
	f.now = f.now.Add(5 * time.Second)
	f.auth.config.Credentials = expiringCredentialWriter{UserCredentialWriter: f.ma, clock: &f.now}
	s := f.status()
	require.Equal(t, core.AuthReauthRequired, s.Status)
	r, e := f.store.GetUserAuth(context.Background(), f.p)
	require.NoError(t, e)
	require.Equal(t, "credential", r.CredentialID)
}

type delayedRefreshStore struct {
	core.UserAuthStore
	entered, release chan struct{}
}

func (s *delayedRefreshStore) BeginUserAuthRefresh(ctx context.Context, begin core.UserAuthBegin) (core.UserAuthRecord, error) {
	close(s.entered)
	select {
	case <-s.release:
	case <-ctx.Done():
		return core.UserAuthRecord{}, ctx.Err()
	}
	return s.UserAuthStore.BeginUserAuthRefresh(ctx, begin)
}

func TestDeviceDelayedRefreshAcquisitionUsesLatestRotatedSecret(t *testing.T) {
	for _, driver := range []string{"embedded", "sqlite"} {
		for _, offset := range []time.Duration{0, -20 * time.Second} {
			t.Run(driver+"/"+offset.String(), func(t *testing.T) {
				f := newFixture(t, driver)
				f.start()
				f.now = f.now.Add(5 * time.Second)
				status := f.status()
				f.ready(status)
				f.now = status.Grant.ExpiresAt.Add(offset)
				delayed := &delayedRefreshStore{UserAuthStore: f.store, entered: make(chan struct{}), release: make(chan struct{})}
				config := f.auth.config
				config.Store = delayed
				late, err := New(config)
				require.NoError(t, err)
				query := core.UserGrantQuery{Principal: f.p, VaultID: "vault", CredentialID: "credential", FlowID: status.Grant.FlowID, RequiredScopes: []string{"read"}}
				lateResult := make(chan core.UserGrant, 1)
				lateError := make(chan error, 1)
				go func() { grant, e := late.Lookup(context.Background(), query); lateResult <- grant; lateError <- e }()
				select {
				case <-delayed.entered:
				case <-time.After(time.Second):
					t.Fatal("late caller did not reach acquisition")
				}
				fresh, err := f.auth.Lookup(context.Background(), query)
				require.NoError(t, err)
				require.Equal(t, core.AuthReady, fresh.Status)
				require.Equal(t, 1, f.refreshes)
				afterFirst, err := f.store.GetUserAuth(context.Background(), f.p)
				require.NoError(t, err)
				firstSecret, err := f.auth.decrypt(context.Background(), *afterFirst)
				require.NoError(t, err)
				require.Equal(t, "refresh-rotated", firstSecret.Tokens.RefreshToken)
				close(delayed.release)
				lateGrant := <-lateResult
				require.NoError(t, <-lateError)
				require.Equal(t, core.AuthReady, lateGrant.Status)
				require.Equal(t, fresh, lateGrant)
				require.Equal(t, 1, f.refreshes, "new lease cannot authorize replaying pre-lease refresh token")
				require.Equal(t, 2, f.writes, "late acquisition cannot resync or overwrite the rotated credential")
				latest, err := f.store.GetUserAuth(context.Background(), f.p)
				require.NoError(t, err)
				require.Equal(t, core.UserAuthReady, latest.State)
				require.Equal(t, afterFirst.Secret, latest.Secret)
				require.Empty(t, latest.Lease.Owner)
				require.NoError(t, f.store.Close())
				f.open()
				grant, err := f.auth.Lookup(context.Background(), query)
				require.NoError(t, err)
				require.Equal(t, fresh, grant)
				require.Equal(t, 1, f.refreshes)
			})
		}
	}
}

func TestDeviceDelayedRefreshRechecksLatestActualScopesWithoutClosingNewGrant(t *testing.T) {
	f := newFixture(t)
	f.pollBody = `{"access_token":"uat-secret","refresh_token":"refresh-secret","expires_in":3600,"scope":"read other"}`
	f.start()
	f.now = f.now.Add(5 * time.Second)
	status := f.status()
	f.ready(status)
	f.now = f.now.Add(time.Hour)
	f.refreshBody = `{"access_token":"uat-rotated","refresh_token":"refresh-rotated","expires_in":3600,"scope":"other"}`
	delayed := &delayedRefreshStore{UserAuthStore: f.store, entered: make(chan struct{}), release: make(chan struct{})}
	config := f.auth.config
	config.Store = delayed
	late, err := New(config)
	require.NoError(t, err)
	query := core.UserGrantQuery{Principal: f.p, VaultID: "vault", CredentialID: "credential", FlowID: status.Grant.FlowID, RequiredScopes: []string{"read"}}
	result := make(chan core.UserGrant, 1)
	failure := make(chan error, 1)
	go func() { g, e := late.Lookup(context.Background(), query); result <- g; failure <- e }()
	<-delayed.entered
	other := query
	other.RequiredScopes = []string{"other"}
	fresh, err := f.auth.Lookup(context.Background(), other)
	require.NoError(t, err)
	require.Equal(t, core.AuthReady, fresh.Status)
	close(delayed.release)
	require.Equal(t, core.AuthReauthRequired, (<-result).Status)
	require.NoError(t, <-failure)
	require.Equal(t, 1, f.refreshes)
	require.Equal(t, 2, f.writes)
	saved, err := f.store.GetUserAuth(context.Background(), f.p)
	require.NoError(t, err)
	require.Equal(t, core.UserAuthReady, saved.State)
	require.Equal(t, fresh, saved.Grant)
}

func TestDeviceDelayedLookupNewFlowIsReferenceConflict(t *testing.T) {
	f := newFixture(t)
	f.start()
	f.now = f.now.Add(5 * time.Second)
	status := f.status()
	f.ready(status)
	f.now = f.now.Add(time.Hour)
	delayed := &delayedRefreshStore{UserAuthStore: f.store, entered: make(chan struct{}), release: make(chan struct{})}
	config := f.auth.config
	config.Store = delayed
	late, err := New(config)
	require.NoError(t, err)
	query := core.UserGrantQuery{Principal: f.p, VaultID: "vault", CredentialID: "credential", FlowID: status.Grant.FlowID, RequiredScopes: []string{"read"}}
	done := make(chan error, 1)
	go func() {
		g, e := late.Lookup(context.Background(), query)
		if e == nil && g.Status == core.AuthSubjectMismatch {
			e = fmt.Errorf("reference drift misclassified as subject mismatch")
		}
		done <- e
	}()
	<-delayed.entered
	f.start()
	f.now = f.now.Add(5 * time.Second)
	fresh := f.status()
	f.ready(fresh)
	before, err := f.store.GetUserAuth(context.Background(), f.p)
	require.NoError(t, err)
	close(delayed.release)
	require.ErrorIs(t, <-done, core.ErrConflict)
	after, err := f.store.GetUserAuth(context.Background(), f.p)
	require.NoError(t, err)
	require.Equal(t, core.UserAuthReady, after.State)
	require.Equal(t, before.Secret, after.Secret)
	require.Equal(t, before.Grant, after.Grant)
	require.Equal(t, before.FlowID, after.FlowID)
	require.Zero(t, f.refreshes)
	_, err = f.auth.Lookup(context.Background(), query)
	require.ErrorIs(t, err, core.ErrConflict)
}
