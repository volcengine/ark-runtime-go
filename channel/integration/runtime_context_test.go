// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/volcengine/ark-runtime-go/channel/internal/conformance"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/ma"
)

// historyProvider 实现 HistoryFetcher。
type historyProvider struct {
	runtimeProvider
	hmu     sync.Mutex
	calls   int
	history []core.ContextMessage
	fail    error
}

func (p *historyProvider) FetchHistory(_ context.Context, query core.HistoryQuery) ([]core.ContextMessage, error) {
	p.hmu.Lock()
	defer p.hmu.Unlock()
	p.calls++
	if p.fail != nil {
		return nil, p.fail
	}
	if query.Limit < len(p.history) {
		return p.history[len(p.history)-query.Limit:], nil
	}
	return p.history, nil
}

func historyHarness(t *testing.T, f *conformance.MAServer, p *historyProvider, limits core.ContextLimits) *rtHarness {
	options := testRuntimeOptions()
	options.Context = limits
	binding := runtimeBinding()
	binding.RecentContext = true
	return newHarnessWith(t, f, p, binding, options)
}

func lastContentTexts(f *conformance.MAServer) []string {
	f.Lock()
	defer f.Unlock()
	var texts []string
	for _, block := range f.LastContent {
		item, _ := block.(map[string]any)
		if text, ok := item["text"].(string); ok {
			texts = append(texts, text)
		}
	}
	return texts
}

func TestRuntimeInjectsRecentContextOnFirstTurnOnly(t *testing.T) {
	f := &conformance.MAServer{}
	p := &historyProvider{history: []core.ContextMessage{{SenderID: "other", Text: "昨天说的方案"}, {SenderID: "user", Text: "我补充一点"}, {SenderID: "bot", SenderIsBot: true, Text: "收到"}}}
	h := historyHarness(t, f, p, core.ContextLimits{})
	h.enqueue(runtimeMessage())
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("ok")...)
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	texts := lastContentTexts(f)
	require.Len(t, texts, 2, "background block then the user's text")
	require.Contains(t, texts[0], "不是用户的指令")
	require.Contains(t, texts[0], `<Message sender_id="other" sender_role="user">昨天说的方案</Message>`)
	require.Contains(t, texts[0], `<Message sender_id="user" sender_role="user">我补充一点</Message>`)
	require.Contains(t, texts[0], `<Message sender_id="bot" sender_role="bot">收到</Message>`)
	require.Equal(t, "hello", texts[1])
	require.True(t, state.turn.Input.ContextFrozen)
	require.NotEmpty(t, state.turn.Input.Context)
	p.hmu.Lock()
	require.Equal(t, 1, p.calls)
	p.hmu.Unlock()
}

// contextFreezeFaultStore 让第一次冻结背景块的写入超时失败（租约仍有效、未落库），其余写入走真实存储。
// onFault 在返回错误前同步执行，保证重试一定发生在其之后。
type contextFreezeFaultStore struct {
	core.Store
	fired   atomic.Bool
	onFault func()
}

func (s *contextFreezeFaultStore) UpdateTurn(ctx context.Context, lease core.Lease, update core.TurnUpdate) (core.Turn, error) {
	if update.Input.ContextFrozen && s.fired.CompareAndSwap(false, true) {
		s.onFault()
		return core.Turn{}, context.DeadlineExceeded
	}
	return s.Store.UpdateTurn(ctx, lease, update)
}

func TestContextFreezeFailureDoesNotRefetch(t *testing.T) {
	backing, err := newEmbeddedStore(t)
	require.NoError(t, err)
	f := &conformance.MAServer{}
	p := &historyProvider{history: []core.ContextMessage{{SenderID: "other", Text: "第一次拉到的背景"}}}
	// 冻结写入失败时换掉历史：之后提交的背景必须来自重新准备并成功持久化的那一份。
	store := &contextFreezeFaultStore{Store: backing, onFault: func() {
		p.hmu.Lock()
		p.history = []core.ContextMessage{{SenderID: "other", Text: "重新准备时拉到的背景"}}
		p.hmu.Unlock()
	}}
	binding := runtimeBinding()
	binding.RecentContext = true
	runtime, err := core.NewRuntime(store, []core.Connection{{Binding: binding, MAClient: newMAForHarness(t, f), Provider: p}}, testRuntimeOptions())
	require.NoError(t, err)
	mustEnqueue(t, runtime, "binding", runtimeMessage())
	runUntilCleanup(t, runtime)
	require.Eventually(t, func() bool {
		f.Lock()
		defer f.Unlock()
		return f.SendCount > 0
	}, 4*time.Second, 5*time.Millisecond)
	f.AppendAfterInput(t, conformance.AnswerEvents("ok")...)
	waitLatest(t, backing, core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.True(t, store.fired.Load())
	texts := lastContentTexts(f)
	require.Len(t, texts, 2)
	require.NotContains(t, texts[0], "第一次拉到的背景", "an unpersisted background must never be submitted")
	require.Contains(t, texts[0], "重新准备时拉到的背景")
	exported, err := backing.Export(context.Background())
	require.NoError(t, err)
	require.Len(t, exported.Turns, 1)
	require.True(t, exported.Turns[0].Input.ContextFrozen)
	require.Contains(t, exported.Turns[0].Input.Context, "重新准备时拉到的背景", "the submitted background must equal the persisted one")
}

func TestRuntimeFollowUpDoesNotInjectContext(t *testing.T) {
	f := &conformance.MAServer{}
	p := &historyProvider{history: []core.ContextMessage{{SenderID: "other", Text: "旧消息"}}}
	h := historyHarness(t, f, p, core.ContextLimits{})
	seedConversation(t, h.store, "sesn-existing")
	h.enqueue(runtimeMessage())
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("ok")...)
	h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Equal(t, []string{"hello"}, lastContentTexts(f))
	p.hmu.Lock()
	require.Zero(t, p.calls, "follow-ups rely on session memory")
	p.hmu.Unlock()
}

func TestRuntimeContextFetchFailureStillAnswers(t *testing.T) {
	f := &conformance.MAServer{}
	p := &historyProvider{fail: errors.New("no permission")}
	h := historyHarness(t, f, p, core.ContextLimits{})
	h.enqueue(runtimeMessage())
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("ok")...)
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Equal(t, "ok", state.export.Deliveries[0].Body.Text)
	require.Equal(t, []string{"hello"}, lastContentTexts(f))
}

func TestRuntimeContextRespectsCharBudget(t *testing.T) {
	f := &conformance.MAServer{}
	p := &historyProvider{history: []core.ContextMessage{{SenderID: "a", Text: strings.Repeat("旧", 500)}, {SenderID: "a", Text: "最新一条"}}}
	h := historyHarness(t, f, p, core.ContextLimits{MaxChars: 240})
	h.enqueue(runtimeMessage())
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("ok")...)
	h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	texts := lastContentTexts(f)
	require.Len(t, texts, 2)
	require.Contains(t, texts[0], "最新一条")
	require.NotContains(t, texts[0], "旧旧", "older messages are dropped first")
	_, err := core.RuntimeOptions{Context: core.ContextLimits{MaxMessages: 1000}}.Normalize()
	require.Error(t, err)
}

func hookHarness(t *testing.T, f *conformance.MAServer, hooks core.Hooks, mutate func(*core.Binding)) *rtHarness {
	t.Helper()
	options := testRuntimeOptions()
	options.Hooks, options.HookTimeout = hooks, 100*time.Millisecond
	binding := runtimeBinding()
	binding.MaxTurnDuration = 0
	if mutate != nil {
		mutate(&binding)
	}
	return newHarnessWith(t, f, &runtimeProvider{}, binding, options)
}

func TestHookBeforeTriggerDenyRepliesSummaryWithoutSession(t *testing.T) {
	f := &conformance.MAServer{}
	var seen core.HookMessage
	h := hookHarness(t, f, core.Hooks{BeforeTrigger: func(_ context.Context, m core.HookMessage) (core.TriggerDecision, error) {
		seen = m
		return core.TriggerDecision{Allow: false, Summary: "本月额度已用完"}, nil
	}}, nil)
	h.enqueue(runtimeMessage())
	h.run()
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Equal(t, "trigger_denied", state.turn.Outcome.ErrorKind)
	require.Equal(t, "本月额度已用完", state.export.Deliveries[0].Body.Text)
	require.Equal(t, core.HookMessage{BindingID: "binding", TenantKey: "tenant", ChatID: "chat", ChatType: "p2p", SenderID: "user", Text: "hello"}, seen)
	f.Lock()
	require.Zero(t, f.CreateCount)
	f.Unlock()
}

func TestHookBeforeTriggerTimeoutFailsClosedByDefaultAndOpenWhenConfigured(t *testing.T) {
	for _, lateSuccess := range []bool{false, true} {
		t.Run(map[bool]string{false: "hook_error", true: "late_success"}[lateSuccess], func(t *testing.T) {
			slow := core.Hooks{BeforeTrigger: func(ctx context.Context, _ core.HookMessage) (core.TriggerDecision, error) {
				<-ctx.Done()
				if lateSuccess {
					return core.TriggerDecision{Allow: true}, nil
				}
				return core.TriggerDecision{}, ctx.Err()
			}}
			f := &conformance.MAServer{}
			h := hookHarness(t, f, slow, nil)
			h.enqueue(runtimeMessage())
			h.run()
			state := h.waitSettled(core.TurnFailed, core.DeliveryDone, 6*time.Second)
			require.Equal(t, "trigger_hook_failed", state.turn.Outcome.ErrorKind)

			f2 := &conformance.MAServer{}
			h2 := hookHarness(t, f2, slow, func(b *core.Binding) { b.HookFailOpen = true })
			h2.enqueue(runtimeMessage())
			h2.run()
			f2.AppendAfterInput(t, conformance.AnswerEvents("ok")...)
			state2 := h2.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
			require.Equal(t, "ok", state2.export.Deliveries[0].Body.Text)
		})
	}
}

func TestHookBuildSessionSpecAppendsAndRejectsReservedTags(t *testing.T) {
	f := &conformance.MAServer{}
	h := hookHarness(t, f, core.Hooks{BuildSessionSpec: func(context.Context, core.HookMessage) (core.SessionExtras, error) {
		return core.SessionExtras{VaultIDs: []string{"vlt-proj"}, MemoryIDs: []string{"mem-proj"}, Tags: map[string]string{"project": "p1"}}, nil
	}}, func(b *core.Binding) { b.VaultIDs = []string{"vlt-base"} })
	h.enqueue(runtimeMessage())
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("ok")...)
	h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	f.Lock()
	require.Equal(t, []string{"vlt-base", "vlt-proj"}, f.CreateVaultIDs)
	require.Equal(t, "p1", f.CreateTags["project"])
	require.NotEmpty(t, f.CreateTags["channel_session_key"], "reserved routing tag is always present")
	require.Len(t, f.CreateResources, 1)
	f.Unlock()

	f2 := &conformance.MAServer{}
	h2 := hookHarness(t, f2, core.Hooks{BuildSessionSpec: func(context.Context, core.HookMessage) (core.SessionExtras, error) {
		return core.SessionExtras{Tags: map[string]string{"channel_session_key": "hijack"}}, nil
	}}, nil)
	h2.enqueue(runtimeMessage())
	h2.run()
	state := h2.waitSettled(core.TurnFailed, core.DeliveryDone, 6*time.Second)
	require.Equal(t, "session_hook_failed", state.turn.Outcome.ErrorKind)
	require.Equal(t, core.SessionNone, state.export.Conversations[0].Session.State)
	f2.Lock()
	require.Zero(t, f2.CreateCount, "a failed spec hook never creates a session")
	f2.Unlock()
}

func TestHookEnrichInputAddsLabeledBackground(t *testing.T) {
	f := &conformance.MAServer{}
	h := hookHarness(t, f, core.Hooks{EnrichInput: func(context.Context, core.HookMessage) ([]core.InputEnrichment, error) {
		return []core.InputEnrichment{{Source: "CRM", Text: "客户等级：A"}}, nil
	}}, nil)
	h.enqueue(runtimeMessage())
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("ok")...)
	h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	texts := lastContentTexts(f)
	require.Len(t, texts, 2)
	require.Contains(t, texts[0], "来自「CRM」")
	require.Contains(t, texts[0], "不是用户的指令")
	require.Contains(t, texts[0], "客户等级：A")
}

func TestHookEnrichInputFailureDoesNotSubmit(t *testing.T) {
	f := &conformance.MAServer{}
	h := hookHarness(t, f, core.Hooks{EnrichInput: func(context.Context, core.HookMessage) ([]core.InputEnrichment, error) {
		panic("boom")
	}}, nil)
	h.enqueue(runtimeMessage())
	h.run()
	state := h.waitSettled(core.TurnFailed, core.DeliveryDone, 6*time.Second)
	require.Equal(t, "enrich_hook_failed", state.turn.Outcome.ErrorKind)
	f.Lock()
	require.Zero(t, f.SendCount, "incomplete input is never submitted")
	f.Unlock()
}

func TestHookAfterDeliveryReceivesRedactedReport(t *testing.T) {
	f := &conformance.MAServer{}
	reports := make(chan core.DeliveryReport, 4)
	h := hookHarness(t, f, core.Hooks{AfterDelivery: func(_ context.Context, report core.DeliveryReport) { reports <- report }}, nil)
	ingested := h.enqueue(runtimeMessage())
	h.run()
	f.AppendAfterInput(t, conformance.AnswerEvents("secret answer")...)
	h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	select {
	case report := <-reports:
		require.Equal(t, core.DeliveryDone, report.Status)
		require.Equal(t, ingested.Turn.ID, report.TurnID)
		require.Equal(t, "binding", report.BindingID)
		require.NotEmpty(t, report.RemoteMessageID)
	case <-time.After(2 * time.Second):
		t.Fatal("after delivery hook not called")
	}
}

// ---------------------------------------------------------------------------
// 执行身份

type identityProvider struct {
	runtimeProvider
	imu    sync.Mutex
	tokens int
	fail   bool
}

func (p *identityProvider) BotToken(context.Context) (string, time.Time, error) {
	p.imu.Lock()
	defer p.imu.Unlock()
	p.tokens++
	if p.fail {
		return "", time.Time{}, errors.New("down")
	}
	return fmt.Sprintf("tat-%d", p.tokens), time.Now().Add(2 * time.Hour), nil
}

type vaultMA struct {
	core.MAClient
	vmu    sync.Mutex
	writes []string
}

func (m *vaultMA) UpsertEnvCredential(_ context.Context, spec core.BotCredentialSpec, value string) (string, error) {
	m.vmu.Lock()
	defer m.vmu.Unlock()
	m.writes = append(m.writes, spec.VaultID+"/"+spec.SecretName+"="+value)
	return "cred_1", nil
}

// GetEnvironment 透传到被包装的真实 MA client（测试夹具），使包装器同时满足 EnvironmentReader。
func (m *vaultMA) GetEnvironment(ctx context.Context, id string) (core.EnvironmentConfig, error) {
	if reader, ok := m.MAClient.(core.EnvironmentReader); ok {
		return reader.GetEnvironment(ctx, id)
	}
	return core.EnvironmentConfig{}, nil
}

func identityBinding() core.Binding {
	binding := runtimeBinding()
	binding.Identity = core.IdentityPolicy{Group: core.IdentityBot, P2P: core.IdentityBot}
	binding.VaultIDs = []string{"vlt_bot"}
	binding.BotCredential = core.BotCredentialSpec{VaultID: "vlt_bot", SecretName: "FEISHU_TENANT_ACCESS_TOKEN", AllowedHosts: []string{"open.feishu.cn"}}
	return binding
}

func identityHarness(t *testing.T, f *conformance.MAServer, wrapped core.MAClient, p core.Provider, binding core.Binding, options core.RuntimeOptions) *rtHarness {
	t.Helper()
	store, err := newEmbeddedStore(t)
	require.NoError(t, err)
	return &rtHarness{t: t, store: store, fixture: f, runtime: mustRuntime(t, store, binding, wrapped, p, options)}
}

func mustRuntime(t *testing.T, store core.Store, binding core.Binding, client core.MAClient, p core.Provider, options core.RuntimeOptions) *core.Runtime {
	runtime, err := core.NewRuntime(store, []core.Connection{{Binding: binding, MAClient: client, Provider: p}}, options)
	require.NoError(t, err)
	return runtime
}

func TestRuntimeWritesBotCredentialBeforeCreatingSession(t *testing.T) {
	f := &conformance.MAServer{}
	serverClient := newMAForHarness(t, f)
	wrapped := &vaultMA{MAClient: serverClient}
	h := identityHarness(t, f, wrapped, &identityProvider{}, identityBinding(), testRuntimeOptions())
	h.run()
	h.enqueue(runtimeMessage())
	f.AppendAfterInput(t, conformance.AnswerEvents("ok")...)
	h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	wrapped.vmu.Lock()
	require.NotEmpty(t, wrapped.writes)
	require.Equal(t, "vlt_bot/FEISHU_TENANT_ACCESS_TOKEN=tat-1", wrapped.writes[0])
	require.Len(t, wrapped.writes, 1, "a fresh token is reused until it nears expiry")
	wrapped.vmu.Unlock()
	f.Lock()
	require.Equal(t, []string{"vlt_bot"}, f.CreateVaultIDs)
	f.Unlock()
}

func TestRuntimeBotTokenFailureDoesNotCreateSession(t *testing.T) {
	f := &conformance.MAServer{}
	serverClient := newMAForHarness(t, f)
	h := identityHarness(t, f, &vaultMA{MAClient: serverClient}, &identityProvider{fail: true}, identityBinding(), deadlineRuntimeOptions())
	h.run()
	h.enqueue(runtimeMessage())
	state := h.waitSettledWithinExternal(t, h.store, core.TurnFailed, core.DeliveryDone, 4*time.Second)
	require.Equal(t, "identity_unavailable", state.turn.Outcome.ErrorKind)
	f.Lock()
	require.Zero(t, f.CreateCount)
	f.Unlock()
}

func TestRuntimeRejectsUnsupportedIdentityConfig(t *testing.T) {
	store, err := newEmbeddedStore(t)
	require.NoError(t, err)
	user := runtimeBinding()
	user.Identity = core.IdentityPolicy{P2P: core.IdentityUser}
	_, err = core.NewRuntime(store, []core.Connection{{Binding: user, MAClient: fakeClientForConfig{}, Provider: &runtimeProvider{}}}, testRuntimeOptions())
	require.ErrorIs(t, err, core.ErrIdentityUnsupported)
	bot := identityBinding()
	bot.VaultIDs = nil
	_, err = core.NewRuntime(store, []core.Connection{{Binding: bot, MAClient: fakeClientForConfig{}, Provider: &runtimeProvider{}}}, testRuntimeOptions())
	require.ErrorIs(t, err, core.ErrRuntimeConfig, "the bot vault must be mounted on sessions")
}

type fakeClientForConfig struct{ core.MAClient }

// ---------------------------------------------------------------------------
// 产物

type artifactProvider struct {
	runtimeProvider
	amu       sync.Mutex
	artifacts []string
	order     []string
}

func (p *artifactProvider) Deliver(ctx context.Context, delivery core.Delivery) (string, error) {
	p.amu.Lock()
	p.order = append(p.order, "text")
	p.amu.Unlock()
	return p.runtimeProvider.Deliver(ctx, delivery)
}

func (p *artifactProvider) DeliverArtifact(_ context.Context, delivery core.Delivery, content core.FileContent) (string, error) {
	data, err := io.ReadAll(content.Body)
	if err != nil {
		return "", err
	}
	p.amu.Lock()
	defer p.amu.Unlock()
	p.order = append(p.order, "artifact")
	artifact := delivery.Body.Artifact
	p.artifacts = append(p.artifacts, artifact.Kind+":"+content.FileName+":"+string(data))
	return "om-" + artifact.FileID, nil
}

type downloadMA struct {
	core.MAClient
	files map[string]string
}

func (m downloadMA) DownloadFile(_ context.Context, id string, _ int64) (core.FileContent, error) {
	data, ok := m.files[id]
	if !ok {
		return core.FileContent{}, errors.New("missing")
	}
	return core.FileContent{Body: io.NopCloser(strings.NewReader(data)), FileName: id + ".bin", Size: int64(len(data))}, nil
}

func toolResultWithFiles(id string, blocks ...map[string]any) map[string]any {
	content := make([]any, 0, len(blocks))
	for _, block := range blocks {
		content = append(content, block)
	}
	return map[string]any{"id": id, "type": "agent.tool_result", "tool_use_id": "tu-" + id, "content": content}
}

func artifactHarness(t *testing.T, client core.MAClient, p *artifactProvider) *rtHarness {
	t.Helper()
	binding := runtimeBinding()
	binding.DeliverArtifacts = true
	store, err := newEmbeddedStore(t)
	require.NoError(t, err)
	return &rtHarness{t: t, store: store, fixture: &conformance.MAServer{}, runtime: mustRuntime(t, store, binding, client, p, testRuntimeOptions())}
}

func TestRuntimeDeliversToolResultArtifactsAfterAnswer(t *testing.T) {
	f := &conformance.MAServer{}
	artifactP := &artifactProvider{}
	client := newMAForHarness(t, f)
	h := artifactHarness(t, downloadMA{MAClient: client, files: map[string]string{"file-img": "PNG", "file-doc": "PDF"}}, artifactP)
	h.fixture = f
	h.run()
	h.enqueue(runtimeMessage())
	events := []map[string]any{
		{"id": "tool-use-1", "type": "agent.tool_use", "name": "draw"},
		toolResultWithFiles("tool-res-1",
			map[string]any{"type": "image", "source": map[string]any{"type": "file", "file_id": "file-img"}},
			map[string]any{"type": "text", "text": "done"},
			map[string]any{"type": "document", "title": "report.pdf", "source": map[string]any{"type": "file", "file_id": "file-doc"}},
			map[string]any{"type": "image", "source": map[string]any{"type": "url", "url": "https://example.com/x.png"}}),
		toolResultWithFiles("tool-res-2", map[string]any{"type": "image", "source": map[string]any{"type": "file", "file_id": "file-img"}}),
	}
	events = append(events, conformance.AnswerEvents("见附件")...)
	f.AppendAfterInput(t, events...)
	require.Eventually(t, func() bool {
		export, err := h.store.Export(context.Background())
		if err != nil || len(export.Deliveries) != 3 {
			return false
		}
		for _, delivery := range export.Deliveries {
			if !delivery.Status.Terminal() {
				return false
			}
		}
		return true
	}, 4*time.Second, 10*time.Millisecond)
	artifactP.amu.Lock()
	defer artifactP.amu.Unlock()
	require.Equal(t, []string{"image:file-img.bin:PNG", "document:file-doc.bin:PDF"}, artifactP.artifacts, "url sources are ignored and file ids are deduplicated")
	require.Equal(t, []string{"text", "artifact", "artifact"}, artifactP.order, "the answer is delivered before artifacts, artifacts in order")
}

func TestRuntimeArtifactsOffByDefault(t *testing.T) {
	f, p := &conformance.MAServer{}, &runtimeProvider{}
	h := newHarness(t, f, p)
	h.enqueue(runtimeMessage())
	h.run()
	events := []map[string]any{toolResultWithFiles("tool-res-1", map[string]any{"type": "image", "source": map[string]any{"type": "file", "file_id": "file-img"}})}
	f.AppendAfterInput(t, append(events, conformance.AnswerEvents("ok")...)...)
	state := h.waitSettled(core.TurnDone, core.DeliveryDone, 6*time.Second)
	require.Len(t, state.export.Deliveries, 1)
}

func newMAForHarness(t *testing.T, f *conformance.MAServer) core.MAClient {
	t.Helper()
	server := httptest.NewServer(f)
	t.Cleanup(server.Close)
	client, err := newTestMA(server.URL, "fixture-key", ma.ClientOptions{RequestTimeout: time.Second, StreamTimeout: time.Second})
	require.NoError(t, err)
	return client
}
