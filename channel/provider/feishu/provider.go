// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	"github.com/larksuite/oapi-sdk-go/v3/ws"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/secret"
)

const requestTimeout = 15 * time.Second

// 事件接收方式。
const (
	// ReceiveWebSocket 由 SDK 主动建立飞书长连接，适合客户自托管 Runner。
	ReceiveWebSocket = "websocket"
	// ReceiveWebhook 由飞书回调 HTTP 端点，适合 Hosted 部署；需要公网可达的 HTTPS 入口。
	ReceiveWebhook = "webhook"
)

// ProviderConfig 保存非密渠道配置与显式注入的密钥解析器。
//
// Webhook 模式下 VerificationTokenRef 与 EncryptKeyRef 均必填；
// 使用加密回调与 X-Lark-Signature 验签，禁止启动无签名密钥的接收端。
type ProviderConfig struct {
	// CardKitLimiter 在同一运行时的全部连接间共享流式结果请求额度。
	CardKitLimiter *CardKitLimiter
	// COTLimiter 由宿主在全部连接间共享，以约束同 Bot 的原生 COT 请求总速率。
	COTLimiter           *COTLimiter
	Binding              core.Binding
	AppSecretRef         string
	SecretResolver       secret.Resolver
	HTTPClient           *http.Client
	OpenBaseURL          string
	BotOpenID            string
	ReceiveMode          string
	VerificationTokenRef string
	EncryptKeyRef        string
}

// Provider 是飞书文本渠道适配器。
type Provider struct {
	messageContextCache messageContextCache
	cardKitLimiter      *CardKitLimiter
	cotLimit            COTLimiter
	cotShared           *COTLimiter
	identityMu          sync.Mutex
	identityReady       bool
	binding             core.Binding
	botOpenID           string
	appSecret           string
	client              *lark.Client
	httpClient          *http.Client
	downloadClient      *http.Client
	baseURL             string
	receiveMode         string
	verificationToken   string
	encryptKey          string
	webhook             atomic.Pointer[http.Handler]
	starting            atomic.Bool
	ready               chan struct{}
	readyOnce           sync.Once
	tenantTokens        tenantTokenCache
	control             controlHolder
}

var _ core.Provider = (*Provider)(nil)

// NewProvider 验证非密配置并只在适配器边界解析 App Secret。
func NewProvider(config ProviderConfig) (*Provider, error) {
	if config.Binding.ID == "" || config.Binding.AppID == "" || config.SecretResolver == nil {
		return nil, errors.New("feishu configuration incomplete")
	}
	if err := secret.Validate(config.AppSecretRef); err != nil {
		return nil, errors.New("feishu secret reference invalid")
	}
	appSecret, err := config.SecretResolver.Resolve(config.AppSecretRef)
	if err != nil || appSecret == "" {
		return nil, errors.New("feishu app secret unavailable")
	}
	mode := config.ReceiveMode
	if mode == "" {
		mode = ReceiveWebSocket
	}
	if mode != ReceiveWebSocket && mode != ReceiveWebhook {
		return nil, errors.New("feishu receive mode unsupported")
	}
	var verificationToken, encryptKey string
	if mode == ReceiveWebhook {
		if secret.Validate(config.VerificationTokenRef) != nil {
			return nil, errors.New("feishu webhook requires a verification token reference")
		}
		if verificationToken, err = config.SecretResolver.Resolve(config.VerificationTokenRef); err != nil || verificationToken == "" {
			return nil, errors.New("feishu verification token unavailable")
		}
		if secret.Validate(config.EncryptKeyRef) != nil {
			return nil, errors.New("feishu webhook requires an encrypt key reference")
		}
		if encryptKey, err = config.SecretResolver.Resolve(config.EncryptKeyRef); err != nil || encryptKey == "" {
			return nil, errors.New("feishu encrypt key unavailable")
		}
	}
	httpClient := boundedClient(config.HTTPClient)
	cardKitLimiter := config.CardKitLimiter
	if cardKitLimiter == nil {
		cardKitLimiter = &CardKitLimiter{}
	}
	cotShared := config.COTLimiter
	if cotShared == nil {
		cotShared = &COTLimiter{}
	}
	opts := []lark.ClientOptionFunc{lark.WithHttpClient(sdkHTTPClient{client: httpClient}), lark.WithLogger(noopLogger{})}
	if config.OpenBaseURL != "" {
		opts = append(opts, lark.WithOpenBaseUrl(config.OpenBaseURL))
	}
	return &Provider{
		cardKitLimiter: cardKitLimiter,
		cotShared:      cotShared,
		binding:        config.Binding, botOpenID: config.BotOpenID, appSecret: appSecret,
		client:     lark.NewClient(config.Binding.AppID, appSecret, opts...),
		httpClient: httpClient, baseURL: config.OpenBaseURL, downloadClient: downloadClient(config.HTTPClient),
		receiveMode: mode, verificationToken: verificationToken, encryptKey: encryptKey,
		ready: make(chan struct{}), tenantTokens: tenantTokenCache{refresh: make(chan struct{}, 1)},
	}, nil
}

// downloadClient 用于附件下载：大文件需要更长时限，且同样不跟随重定向。
func downloadClient(input *http.Client) *http.Client {
	var client http.Client
	if input != nil {
		client = *input
	}
	client.Timeout = 2 * time.Minute
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &client
}

func boundedClient(input *http.Client) *http.Client {
	var client http.Client
	if input != nil {
		client = *input
	}
	if client.Timeout <= 0 || client.Timeout > requestTimeout {
		client.Timeout = requestTimeout
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &client
}

// Start 校验 Bot 身份，随后阻塞接收事件直到上下文取消。
//
// WebSocket 模式下主动连接飞书；Webhook 模式下装配 HTTP 处理器（见 WebhookHandler）并等待取消。
// 同一 Provider 可顺序重启，但不允许并发 Start。
func (p *Provider) Start(ctx context.Context, accept func(context.Context, core.Message) error) error {
	if ctx == nil || accept == nil || p.client == nil {
		return errors.New("feishu receiver unavailable")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !p.starting.CompareAndSwap(false, true) {
		return errors.New("feishu receiver already running")
	}
	defer p.starting.Store(false)
	if _, err := p.InitializeBot(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	dispatch := p.dispatcher(accept)
	if p.receiveMode == ReceiveWebhook {
		var handler http.Handler = webhookHandler{dispatch: dispatch}
		p.webhook.Store(&handler)
		defer p.webhook.Store(nil)
		p.readyOnce.Do(func() { close(p.ready) })
		<-ctx.Done()
		return nil
	}
	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = requestTimeout
	opts := []ws.ClientOption{
		ws.WithEventHandler(dispatch), ws.WithLogger(noopLogger{}),
		ws.WithHttpClient(p.httpClient), ws.WithWebSocketDialer(&dialer),
	}
	if p.baseURL != "" {
		opts = append(opts, ws.WithDomain(p.baseURL))
	}
	p.readyOnce.Do(func() { close(p.ready) })
	return ws.NewClient(p.binding.AppID, p.appSecret, opts...).Start(ctx)
}

// InitializeBot 显式校验并缓存 Bot 身份，供宿主 webhook 装配使用；不会启动接收循环。
func (p *Provider) InitializeBot(ctx context.Context) (string, error) {
	p.identityMu.Lock()
	defer p.identityMu.Unlock()
	if p.identityReady {
		return p.botOpenID, nil
	}
	botOpenID, err := p.getBotOpenID(ctx)
	if err != nil {
		return "", err
	}
	if p.botOpenID != "" && p.botOpenID != botOpenID {
		return "", errors.New("feishu bot identity mismatch")
	}
	p.botOpenID = botOpenID
	p.identityReady = true
	return botOpenID, nil
}

// dispatcher 构造事件分发器；两种接收方式共用同一套事件与卡片回调处理。
func (p *Provider) dispatcher(accept func(context.Context, core.Message) error) *dispatcher.EventDispatcher {
	dispatch := dispatcher.NewEventDispatcher(p.verificationToken, p.encryptKey)
	dispatch.Config.Logger = noopLogger{}
	dispatch.OnP2MessageReceiveV1(func(ctx context.Context, event *larkim.P2MessageReceiveV1) error {
		return p.handle(ctx, event, accept)
	})
	dispatch.OnP2MessageRecalledV1(func(ctx context.Context, event *larkim.P2MessageRecalledV1) error {
		message, ok, err := normalizeRecall(event, p.binding)
		if err != nil || !ok {
			return err
		}
		return accept(ctx, message)
	})
	p.registerCallbacks(dispatch, accept)
	return dispatch
}

// WebhookHandler 返回飞书事件回调的 HTTP 处理器。Start 完成 Bot 身份校验前返回 503，让飞书
// 稍后重投；只接受 POST，请求体上限 1 MiB。处理器只做验签、解密、落 Inbox 与 ACK。
func (p *Provider) WebhookHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler := p.webhook.Load()
		if handler == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		(*handler).ServeHTTP(w, r)
	})
}

// Ready 在 Provider 首次完成启动装配后关闭；顺序重启复用该通知。
func (p *Provider) Ready() <-chan struct{} { return p.ready }

type webhookHandler struct {
	dispatch *dispatcher.EventDispatcher
}

const maxWebhookBody = 1 << 20

func (h webhookHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxWebhookBody+1))
	if err != nil || len(body) > maxWebhookBody {
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		return
	}
	req := &larkevent.EventReq{Header: r.Header, Body: body, RequestURI: r.RequestURI}
	// URL 校验只解密、校验 verification token 并 echo，不进入业务处理器。
	// 官方未保证该握手携带签名头；业务事件始终要求新鲜签名，不能降级到 token 校验。
	if !h.isChallenge(r.Context(), req) && !validWebhookTimestamp(r.Header.Get(larkevent.EventRequestTimestamp), time.Now()) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	resp := h.dispatch.Handle(r.Context(), req)
	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(resp.Body)
}

func (p *Provider) getBotOpenID(ctx context.Context) (string, error) {
	ctx, cancel := boundedContext(ctx)
	defer cancel()
	resp, err := p.client.Get(ctx, "/open-apis/bot/v3/info", nil, larkcore.AccessTokenTypeTenant)
	if err != nil || resp == nil || resp.StatusCode != http.StatusOK {
		return "", errors.New("feishu bot identity request failed")
	}
	var body struct {
		Code int `json:"code"`
		Bot  struct {
			OpenID string `json:"open_id"`
		} `json:"bot"`
	}
	if err := json.Unmarshal(resp.RawBody, &body); err != nil || body.Code != 0 || body.Bot.OpenID == "" {
		return "", errors.New("feishu bot identity invalid")
	}
	return body.Bot.OpenID, nil
}

func boundedContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(ctx, requestTimeout)
}

type noopLogger struct{}

func (noopLogger) Debug(context.Context, ...interface{}) {}
func (noopLogger) Info(context.Context, ...interface{})  {}
func (noopLogger) Warn(context.Context, ...interface{})  {}
func (noopLogger) Error(context.Context, ...interface{}) {}

// Deliver 按持久 ReplyTarget 和 UUID 投递一次文本回复。
func (p *Provider) Deliver(ctx context.Context, delivery core.Delivery) (string, error) {
	if p.client == nil || ctx == nil {
		return "", failure{"feishu client unavailable", false, false, "invalid_client"}
	}
	if delivery.Body.Type != core.BodyText || strings.TrimSpace(delivery.Body.Text) == "" || delivery.UUID == "" {
		return "", failure{"feishu delivery invalid", false, false, "invalid_delivery"}
	}
	content, err := json.Marshal(struct {
		Text string `json:"text"`
	}{Text: delivery.Body.Text})
	if err != nil {
		return "", failure{"feishu delivery invalid", false, false, "invalid_delivery"}
	}
	ctx, cancel := boundedContext(ctx)
	defer cancel()
	return p.send(ctx, delivery, "text", string(content))
}

// send 按回复目标发送一条消息；UUID 保证同一投递重试时飞书侧去重。
func (p *Provider) send(ctx context.Context, delivery core.Delivery, msgType, content string) (string, error) {
	target := delivery.ReplyTarget
	switch target.Mode {
	case "chat":
		if target.ChatID == "" {
			return "", failure{"feishu chat target missing", false, false, "invalid_target"}
		}
		body := larkim.NewCreateMessageReqBodyBuilder().ReceiveId(target.ChatID).MsgType(msgType).Content(content).Uuid(delivery.UUID).Build()
		resp, err := p.client.Im.Message.Create(ctx, larkim.NewCreateMessageReqBuilder().ReceiveIdType("chat_id").Body(body).Build())
		if err != nil {
			return "", transportFailure(err)
		}
		if resp == nil || resp.ApiResp == nil {
			return "", failure{"feishu response missing", true, false, "response_missing"}
		}
		if err := responseFailure(resp.StatusCode, resp.Code, resp.Success(), resp.Data != nil && resp.Data.MessageId != nil && *resp.Data.MessageId != ""); err != nil {
			return "", err
		}
		return *resp.Data.MessageId, nil
	case "reply", "thread":
		if target.MessageID == "" {
			return "", failure{"feishu reply target missing", false, false, "invalid_target"}
		}
		body := larkim.NewReplyMessageReqBodyBuilder().MsgType(msgType).Content(content).Uuid(delivery.UUID)
		if target.Mode == "thread" {
			body.ReplyInThread(true)
		}
		resp, err := p.client.Im.Message.Reply(ctx, larkim.NewReplyMessageReqBuilder().MessageId(target.MessageID).Body(body.Build()).Build())
		if err != nil {
			return "", transportFailure(err)
		}
		if resp == nil || resp.ApiResp == nil {
			return "", failure{"feishu response missing", true, false, "response_missing"}
		}
		if err := responseFailure(resp.StatusCode, resp.Code, resp.Success(), resp.Data != nil && resp.Data.MessageId != nil && *resp.Data.MessageId != ""); err != nil {
			return "", err
		}
		return *resp.Data.MessageId, nil
	default:
		return "", failure{"feishu reply mode unsupported", false, false, "invalid_target"}
	}
}
