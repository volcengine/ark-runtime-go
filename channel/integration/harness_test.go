// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/volcengine/ark-runtime-go/arkruntime"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/ma"
	"github.com/volcengine/ark-runtime-go/channel/store/embedded"
)

type runtimeProvider struct {
	mu         sync.Mutex
	deliveries []core.Delivery
	err        error
	errs       []error
}

func (p *runtimeProvider) Start(ctx context.Context, _ func(context.Context, core.Message) error) error {
	<-ctx.Done()
	return ctx.Err()
}

func (p *runtimeProvider) Deliver(_ context.Context, delivery core.Delivery) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.deliveries = append(p.deliveries, delivery)
	if len(p.deliveries) <= len(p.errs) && p.errs[len(p.deliveries)-1] != nil {
		return "", p.errs[len(p.deliveries)-1]
	}
	if p.err != nil {
		return "", p.err
	}
	return "provider-message", nil
}

func (p *runtimeProvider) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.deliveries)
}

func (p *runtimeProvider) delivered() []core.Delivery {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]core.Delivery(nil), p.deliveries...)
}

type runtimeDeliveryError struct {
	kind      string
	uncertain bool
	retryable bool
}

func (e runtimeDeliveryError) Error() string     { return e.kind }
func (e runtimeDeliveryError) ErrorKind() string { return e.kind }
func (e runtimeDeliveryError) Uncertain() bool   { return e.uncertain }
func (e runtimeDeliveryError) Retryable() bool   { return e.retryable }

// runtimeBinding 不限制单轮时长；验证期限语义的用例通过 deadlineRuntimeOptions 控制。
func runtimeBinding() core.Binding {
	return core.Binding{ID: "binding", AppID: "app", AgentID: "agent", AgentVersion: "v1", EnvironmentID: "env"}
}

// runtimeMessage 是 p2p 会话上的输入；附件、上下文等测试按需改字段。
func runtimeMessage() core.Message {
	return core.Message{BindingID: "binding", ProviderEventID: "event", MessageID: "source-message", TenantKey: "tenant", ChatID: "chat", ChatType: "p2p", SenderID: "user", Text: "hello"}
}

// testUserAuthPollInterval 让授权等待按毫秒级复查；生产默认 2 秒的首轮间隔不承载任何断言。
const testUserAuthPollInterval = 20 * time.Millisecond

// testRuntimeOptions 用于验证成功完成、投递、快照与恢复契约。期限远大于测试等待窗口，
// CPU 配额受限的 CI runner 上 race 插桩的往返不会先触发 Runtime 超时。
func testRuntimeOptions() core.RuntimeOptions {
	return core.RuntimeOptions{Workers: 2, PollInterval: 10 * time.Millisecond, TurnTimeout: 5 * time.Second, LeaseTTL: 15 * time.Second, DeliveryTimeout: 2 * time.Second, DeliveryMaxAttempts: 3, UnknownWriteGrace: 200 * time.Millisecond, MessageDeadline: 30 * time.Second, UserAuthPollInterval: testUserAuthPollInterval}
}

// restartRuntimeOptions 用于取消后用同一 Store 重启的用例：取消时持有的 Turn 与授权门租约不会释放，
// 新实例须等其到期才能接管，租约取 5s 限定接管等待；执行期限 3s 仍覆盖慢 runner 上的单次调用。
func restartRuntimeOptions() core.RuntimeOptions {
	options := testRuntimeOptions()
	options.TurnTimeout, options.LeaseTTL = 3*time.Second, 5*time.Second
	return options
}

// deadlineRuntimeOptions 只用于验证超时、未知写宽限、重试耗尽与租约接管等期限语义，
// 断言依赖这些短期限先于测试等待窗口到期。
func deadlineRuntimeOptions() core.RuntimeOptions {
	return core.RuntimeOptions{Workers: 2, PollInterval: 10 * time.Millisecond, TurnTimeout: 500 * time.Millisecond, LeaseTTL: 2 * time.Second, DeliveryTimeout: 200 * time.Millisecond, DeliveryMaxAttempts: 3, UnknownWriteGrace: 200 * time.Millisecond, MessageDeadline: 1500 * time.Millisecond, UserAuthPollInterval: testUserAuthPollInterval}
}

type rtHarness struct {
	t       *testing.T
	runtime *core.Runtime
	store   testStore
	fixture *conformance.MAServer
	server  *httptest.Server
}

func newHarness(t *testing.T, f *conformance.MAServer, p core.Provider) *rtHarness {
	return newHarnessWith(t, f, p, runtimeBinding(), testRuntimeOptions())
}

// newDeadlineHarness 使用短期限配置，只供验证期限语义的用例使用。
func newDeadlineHarness(t *testing.T, f *conformance.MAServer, p core.Provider) *rtHarness {
	return newHarnessWith(t, f, p, runtimeBinding(), deadlineRuntimeOptions())
}

func newHarnessWith(t *testing.T, f *conformance.MAServer, p core.Provider, binding core.Binding, options core.RuntimeOptions) *rtHarness {
	t.Helper()
	store, err := openEmbeddedFixture(filepath.Join(t.TempDir(), "state.db"), embedded.Options{Clock: options.Clock, NoSync: true})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	server := httptest.NewServer(http.HandlerFunc(f.ServeHTTP))
	t.Cleanup(server.Close)
	client, err := newTestMA(server.URL, "fixture-key", ma.ClientOptions{RequestTimeout: time.Second, StreamTimeout: time.Second})
	require.NoError(t, err)
	runtime, err := core.NewRuntime(store, []core.Connection{{Binding: binding, MAClient: client, Provider: p}}, options)
	require.NoError(t, err)
	return &rtHarness{t: t, runtime: runtime, store: store, fixture: f, server: server}
}

func (h *rtHarness) enqueue(message core.Message) core.IngestResult {
	return mustEnqueue(h.t, h.runtime, "binding", message)
}

func mustEnqueue(t require.TestingT, runtime *core.Runtime, bindingID string, message core.Message) core.IngestResult {
	result, err := runtime.Enqueue(context.Background(), bindingID, message)
	require.NoError(t, err)
	return result
}

func (h *rtHarness) run() context.CancelFunc {
	h.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.runtime.Run(ctx) }()
	h.t.Cleanup(func() {
		cancel()
		err := <-done
		if err != nil {
			h.t.Logf("DIAG runtime exit cause: %v", errors.Unwrap(err))
		}
		require.NoError(h.t, err)
	})
	return cancel
}

// settleState 描述等待中的单轮终态。
type settleState struct {
	turn     *core.Turn
	turns    []core.Turn
	messages map[string]core.InboundMessage
	delivery map[string]core.Delivery
	export   core.Export
}

// waitSettled 等待最新一个 Turn（预置的历史轮次之后）达到 wantTurn，且全部投递达到 wantDelivery。
func (h *rtHarness) waitSettled(wantTurn core.TurnStatus, wantDelivery core.DeliveryStatus, within time.Duration) settleState {
	h.t.Helper()
	return waitLatest(h.t, h.store, wantTurn, wantDelivery, within)
}

func waitLatest(t *testing.T, store core.Exporter, wantTurn core.TurnStatus, wantDelivery core.DeliveryStatus, within time.Duration) settleState {
	t.Helper()
	var state settleState
	defer func() {
		if t.Failed() {
			export, _ := store.Export(context.Background())
			for _, turn := range export.Turns {
				t.Logf("DIAG turn=%s status=%s phase=%s wait=%q outcome=%+v attempt=%d writes=%d started=%s deadline=%s", turn.ID, turn.Status, turn.Phase, turn.WaitReason, turn.Outcome, turn.Schedule.Attempt, turn.Submit.WriteAttempts, turn.Schedule.StartedAt.Format("15:04:05.000"), turn.Schedule.DeadlineAt.Format("15:04:05.000"))
			}
			for _, d := range export.Deliveries {
				t.Logf("DIAG delivery=%s status=%s attempt=%d err=%q", d.ID, d.Status, d.Attempt, d.ErrorKind)
			}
		}
	}()
	require.Eventually(t, func() bool {
		export, err := store.Export(context.Background())
		if err != nil || len(export.Turns) == 0 {
			return false
		}
		latest := export.Turns[0]
		for _, turn := range export.Turns {
			if turn.ID > latest.ID {
				latest = turn
			}
		}
		if latest.Status != wantTurn {
			return false
		}
		for _, delivery := range export.Deliveries {
			if delivery.Status != wantDelivery {
				return false
			}
		}
		state.export = export
		state.turn = &latest
		return true
	}, within, 10*time.Millisecond)
	return project(state)
}

func project(state settleState) settleState {
	state.turns = state.export.Turns
	state.delivery = map[string]core.Delivery{}
	state.messages = map[string]core.InboundMessage{}
	for _, delivery := range state.export.Deliveries {
		state.delivery[delivery.ID] = delivery
	}
	for _, message := range state.export.Messages {
		state.messages[message.ID] = message
	}
	return state
}

// waitAllSettled 等待全部 Turn 终态、全部投递达到 wantDelivery，返回投影。
func (h *rtHarness) waitAllSettled(wantDelivery core.DeliveryStatus, within time.Duration) settleState {
	h.t.Helper()
	var state settleState
	require.Eventually(h.t, func() bool {
		export, err := h.store.Export(context.Background())
		if err != nil || len(export.Turns) == 0 && len(export.Deliveries) == 0 {
			return false
		}
		for _, turn := range export.Turns {
			if !turn.Status.Terminal() {
				return false
			}
		}
		for _, delivery := range export.Deliveries {
			if delivery.Status != wantDelivery {
				return false
			}
		}
		state.export = export
		return true
	}, within, 10*time.Millisecond)
	return project(state)
}

func textsBySource(state settleState) map[string]string {
	result := map[string]string{}
	for _, delivery := range state.export.Deliveries {
		if delivery.Body.Type == core.BodyText {
			result[delivery.Source.String()] = delivery.Body.Text
		}
	}
	return result
}

func deliveryTexts(state settleState) []string {
	var texts []string
	for _, delivery := range state.export.Deliveries {
		if delivery.Body.Type == core.BodyText {
			texts = append(texts, delivery.Body.Text)
		}
	}
	return texts
}

func (h *rtHarness) phaseEventually(phase core.TurnPhase) {
	h.t.Helper()
	require.Eventually(h.t, func() bool {
		export, err := h.store.Export(context.Background())
		return err == nil && len(export.Turns) == 1 && export.Turns[0].Phase == phase
	}, 2*time.Second, 5*time.Millisecond)
}

func (h *rtHarness) turnByID(id string) *core.Turn {
	turn, err := h.store.GetTurn(context.Background(), id)
	require.NoError(h.t, err)
	return turn
}

// seedConversation 预置一个已结算轮次和它绑定的会话，模拟同一对话已有 Session。
func seedConversation(t *testing.T, store core.Store, sessionID string) core.Conversation {
	t.Helper()
	ctx := context.Background()
	message := runtimeMessage()
	message.ProviderEventID, message.MessageID = "seed-event", "seed-message"
	ingested, err := store.Ingest(ctx, core.NewBindingVersion(runtimeBinding()), message)
	require.NoError(t, err)
	lease, err := store.LeaseTurn(ctx, conformance.Claim("setup"))
	require.NoError(t, err)
	binding := core.SessionBinding{State: core.SessionActive, Generation: 1, SessionID: sessionID, CreationKey: "seed-key", LookupKey: "seed-lookup", BindingVersion: ingested.Turn.BindingVersion}
	conversation, err := store.BindSession(ctx, lease.Turn.Lease, binding)
	require.NoError(t, err)
	require.NoError(t, store.SettleTurn(ctx, lease.Turn.Lease, core.TurnOutcome{Status: core.TurnDone}, nil))
	return conversation
}

// failSettleStore 在第一次 SettleTurn 时失败，模拟映射已写入但结算崩溃。
type failSettleStore struct {
	core.Store
	fail bool
}

func (s *failSettleStore) SettleTurn(ctx context.Context, lease core.Lease, outcome core.TurnOutcome, deliveries []core.Delivery) error {
	if s.fail {
		s.fail = false
		return errors.New("simulated crash after mapping")
	}
	return s.Store.SettleTurn(ctx, lease, outcome, deliveries)
}

// newTestMA makes injected transport failures visible to Runtime recovery tests.
// Retry/redirect ownership is exercised independently by the MA adapter tests.
func newTestMA(baseURL, key string, options ma.ClientOptions) (*ma.Client, error) {
	if options.SDKClient == nil {
		endpoint := strings.TrimSuffix(baseURL, "/")
		if !strings.HasSuffix(endpoint, "/api/v3") {
			endpoint += "/api/v3"
		}
		config := []arkruntime.ConfigOption{arkruntime.WithBaseUrl(endpoint), arkruntime.WithRetryTimes(0)}
		if options.HTTPClient != nil {
			config = append(config, arkruntime.WithHTTPClient(options.HTTPClient))
		}
		options.SDKClient = arkruntime.NewClientWithApiKey(key, config...)
	}
	return ma.NewClient(baseURL, key, options)
}
