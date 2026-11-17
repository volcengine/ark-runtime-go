// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	memory "github.com/volcengine/ark-runtime-go/channel/internal/testutil/memorystore"
)

type batchesAuthorizer struct {
	userAuthFixture
	verifiedGrant core.UserGrant
	batches       [][]string
	actual        []string
	attempt       uint64
	fail          string
	pauseSecond   bool
	clock         func() time.Time
}

// Lookup 返回最近一次 Status 确认的授权，模拟提交重试时读取同一份真实 grant。
func (a *batchesAuthorizer) Lookup(_ context.Context, q core.UserGrantQuery) (core.UserGrant, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	grant := a.verifiedGrant
	if grant.Status != core.AuthReady {
		return core.UserGrant{Status: core.AuthReauthRequired}, nil
	}
	if grant.VaultID != q.VaultID {
		return core.UserGrant{}, core.ErrConflict
	}
	grant.GrantedScopes = append([]string(nil), grant.GrantedScopes...)
	return grant, nil
}

func (a *batchesAuthorizer) Start(_ context.Context, _ core.UserPrincipal, r core.AuthRequest) (core.AuthFlow, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.batches = append(a.batches, append([]string(nil), r.RequiredScopes...))
	if a.attempt == 0 {
		a.attempt = r.Attempt
	}
	if a.attempt != r.Attempt {
		return core.AuthFlow{}, fmt.Errorf("attempt changed")
	}
	if len(a.batches) > 1 && r.ExistingCredentialID != "user-credential" {
		return core.AuthFlow{}, fmt.Errorf("credential changed")
	}
	a.flow = fmt.Sprintf("batch-%d", len(a.batches))
	a.actual = append(a.actual, r.RequiredScopes...)
	now := time.Now
	if a.clock != nil {
		now = a.clock
	}
	return core.AuthFlow{ID: a.flow, URL: "https://auth.test", ExpiresAt: now().Add(time.Minute)}, nil
}
func (a *batchesAuthorizer) Status(_ context.Context, r core.AuthFlowRef) (core.AuthStatus, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.batches) > 1 && a.pauseSecond {
		return core.AuthStatus{Status: core.AuthPending}, nil
	}
	if len(a.batches) > 1 && a.fail != "" {
		if a.fail == "lost_previous" {
			a.actual = append([]string(nil), a.batches[len(a.batches)-1]...)
		} else {
			return core.AuthStatus{Status: a.fail}, nil
		}
	}
	a.verifiedGrant = core.UserGrant{Status: core.AuthReady, VaultID: r.VaultID, CredentialID: "user-credential", FlowID: a.flow, GrantVersion: a.flow, GrantedScopes: append([]string(nil), a.actual...)}
	return core.AuthStatus{Status: core.AuthReady, Grant: a.verifiedGrant}, nil
}
func TestRuntimeUserAuthorizationScopeBatches(t *testing.T) {
	for _, n := range []int{200, 201, 401} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			clock := conformance.NewClock(time.Now())
			s, e := memory.New(memory.Options{Clock: clock.Now, PayloadCodec: &conformance.UserAuthTestCodec{}, PayloadNamespace: "test", RequirePayloadEncryption: true})
			require.NoError(t, e)
			t.Cleanup(func() { require.NoError(t, s.Close()) })
			b, options := userRuntimeFixture()
			options.Clock = clock.Now
			b.UserAuthScopes = nil
			for i := 0; i < n; i++ {
				b.UserAuthScopes = append(b.UserAuthScopes, fmt.Sprintf("s%03d", i))
			}
			a := &batchesAuthorizer{clock: clock.Now}
			f := &conformance.MAServer{EnvironmentType: "cloud"}
			c := core.Connection{Binding: b, MAClient: &userVaultMA{MAClient: newMAForHarness(t, f)}, Provider: &runtimeProvider{}, UserAuthorizer: a}
			r, e := core.NewRuntime(s, []core.Connection{c}, options)
			require.NoError(t, e)
			admitted := mustEnqueue(t, r, b.ID, runtimeMessage())
			runUntilCleanup(t, r)
			t.Cleanup(logUserAuthOnFailure(t, func() core.Store { return s }, admitted.Turn.ID, userPrincipalFixture(b)))
			require.Eventually(t, func() bool {
				gate, e := s.GetUserAuth(context.Background(), userPrincipalFixture(b))
				if e != nil || gate == nil {
					return false
				}
				if gate.State == core.UserAuthReady {
					return true
				}
				turn, e := s.GetTurn(context.Background(), admitted.Turn.ID)
				if e != nil || turn == nil || turn.Status != core.TurnWaiting || turn.AuthWait == nil ||
					gate.State != core.UserAuthPending || gate.Lease.Owner != "" || turn.Lease.Owner != "" ||
					turn.AuthWait.FlowID != gate.FlowID || turn.AuthWait.Batch != gate.Batch || turn.AuthWait.Attempt != gate.Attempt {
					return false
				}
				deliveries, e := s.ListDeliveries(context.Background(), core.DeliveryQuery{})
				if e != nil {
					return false
				}
				for _, delivery := range deliveries {
					if delivery.Lease.Owner != "" {
						return false
					}
				}
				// 仅在授权等待已提交、相关租约已归还后，跳到该批持久化的轮询时间。
				if delay := turn.AuthWait.Until.Sub(clock.Now()); delay > 0 {
					clock.Advance(delay)
				}
				return false
			}, 5*time.Second, 10*time.Millisecond)
			a.mu.Lock()
			require.Len(t, a.batches, (n+199)/200)
			for _, batch := range a.batches {
				require.LessOrEqual(t, len(batch), 200)
			}
			require.Equal(t, b.UserAuthScopes, a.actual, "全部 scope 必须按顺序恰好请求一次")
			a.mu.Unlock()
			require.Eventually(t, func() bool { f.Lock(); defer f.Unlock(); return f.SendCount == 1 }, 5*time.Second, 10*time.Millisecond)
		})
	}
}

func TestRuntimeUserAuthorizationLaterBatchFailureClosesGate(t *testing.T) {
	for _, failure := range []string{core.AuthDenied, core.AuthSubjectMismatch, "lost_previous"} {
		t.Run(failure, func(t *testing.T) {
			s := newUserStore(t)
			b := userBinding()
			b.UserAuthScopes = nil
			for i := 0; i < 201; i++ {
				b.UserAuthScopes = append(b.UserAuthScopes, fmt.Sprintf("s%03d", i))
			}
			a := &batchesAuthorizer{fail: failure}
			f := &conformance.MAServer{EnvironmentType: "cloud"}
			c := core.Connection{Binding: b, MAClient: &userVaultMA{MAClient: newMAForHarness(t, f)}, Provider: &runtimeProvider{}, UserAuthorizer: a}
			options := testRuntimeOptions()
			options.MessageDeadline = time.Minute
			r, e := core.NewRuntime(s, []core.Connection{c}, options)
			require.NoError(t, e)
			admitted := mustEnqueue(t, r, b.ID, runtimeMessage())
			runUntilCleanup(t, r)
			require.Eventually(t, func() bool {
				turn, e := s.GetTurn(context.Background(), admitted.Turn.ID)
				return e == nil && turn.Status == core.TurnFailed
			}, 15*time.Second, 10*time.Millisecond)
			gate, e := s.GetUserAuth(context.Background(), userPrincipalFixture(b))
			require.NoError(t, e)
			require.NotEqual(t, core.UserAuthReady, gate.State)
			a.mu.Lock()
			require.Len(t, a.batches, 2)
			a.mu.Unlock()
			f.Lock()
			require.Zero(t, f.CreateCount)
			require.Zero(t, f.SendCount)
			f.Unlock()
		})
	}
}

func TestRuntimeUserAuthorizationMiddleBatchRestart(t *testing.T) {
	s := newUserStore(t)
	b := userBinding()
	b.UserAuthScopes = nil
	for i := 0; i < 201; i++ {
		b.UserAuthScopes = append(b.UserAuthScopes, fmt.Sprintf("s%03d", i))
	}
	a := &batchesAuthorizer{pauseSecond: true}
	f := &conformance.MAServer{EnvironmentType: "cloud"}
	c := core.Connection{Binding: b, MAClient: &userVaultMA{MAClient: newMAForHarness(t, f)}, Provider: &runtimeProvider{}, UserAuthorizer: a}
	options := restartRuntimeOptions()
	options.MessageDeadline = time.Minute
	r, e := core.NewRuntime(s, []core.Connection{c}, options)
	require.NoError(t, e)
	admitted := mustEnqueue(t, r, b.ID, runtimeMessage())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	require.Eventually(t, func() bool {
		turn, e := s.GetTurn(context.Background(), admitted.Turn.ID)
		return e == nil && turn.AuthWait != nil && turn.AuthWait.Batch == 2
	}, 15*time.Second, 10*time.Millisecond)
	before, e := s.GetTurn(context.Background(), admitted.Turn.ID)
	require.NoError(t, e)
	cancel()
	require.NoError(t, <-done)
	a.mu.Lock()
	a.pauseSecond = false
	a.mu.Unlock()
	second, e := core.NewRuntime(s, []core.Connection{c}, options)
	require.NoError(t, e)
	runUntilCleanup(t, second)
	require.Eventually(t, func() bool {
		gate, e := s.GetUserAuth(context.Background(), userPrincipalFixture(b))
		return e == nil && gate != nil && gate.State == core.UserAuthReady
	}, 15*time.Second, 10*time.Millisecond)
	after, e := s.GetTurn(context.Background(), admitted.Turn.ID)
	require.NoError(t, e)
	require.Equal(t, before.Schedule.DeadlineAt, after.Schedule.DeadlineAt)
	a.mu.Lock()
	require.Len(t, a.batches, 2)
	a.mu.Unlock()
}

func TestRuntimeUserScopeBatchesSurviveSubmissionRetry(t *testing.T) {
	s := newUserStore(t)
	b := userBinding()
	b.UserAuthScopes = nil
	for i := 0; i < 201; i++ {
		b.UserAuthScopes = append(b.UserAuthScopes, fmt.Sprintf("s%03d", i))
	}
	a := &batchesAuthorizer{}
	f := &conformance.MAServer{EnvironmentType: "cloud", SendCodes: []int{http.StatusTooManyRequests, 0}}
	c := core.Connection{Binding: b, MAClient: &userVaultMA{MAClient: newMAForHarness(t, f)}, Provider: &runtimeProvider{}, UserAuthorizer: a}
	options := testRuntimeOptions()
	options.MessageDeadline = time.Minute
	r, err := core.NewRuntime(s, []core.Connection{c}, options)
	require.NoError(t, err)
	admitted := mustEnqueue(t, r, b.ID, runtimeMessage())
	runUntilCleanup(t, r)
	require.Eventually(t, func() bool { f.Lock(); defer f.Unlock(); return f.SendCount == 2 }, 20*time.Second, 10*time.Millisecond)
	f.AppendAfterInput(t, conformance.AnswerEvents("authorized after throttling")...)
	require.Eventually(t, func() bool {
		turn, e := s.GetTurn(context.Background(), admitted.Turn.ID)
		return e == nil && turn.Status == core.TurnDone
	}, time.Second, 10*time.Millisecond)
	a.mu.Lock()
	require.Len(t, a.batches, 2, "a rejected submission must reuse the completed authorization")
	a.mu.Unlock()
	f.Lock()
	require.Equal(t, 1, f.CreateCount)
	require.Equal(t, 2, f.SendCount)
	f.Unlock()
}
