// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

type mapSecret map[string]string

func (m mapSecret) Resolve(ref string) (string, error) {
	if value, ok := m[ref]; ok {
		return value, nil
	}
	return "", io.EOF
}

// encryptEvent 按飞书事件加密规范生成密文：key=sha256(encryptKey)，AES-256-CBC，随机 IV 前置，PKCS7 填充。
func encryptEvent(t *testing.T, key string, plain []byte) string {
	t.Helper()
	sum := sha256.Sum256([]byte(key))
	block, err := aes.NewCipher(sum[:])
	require.NoError(t, err)
	pad := aes.BlockSize - len(plain)%aes.BlockSize
	plain = append(plain, bytes.Repeat([]byte{byte(pad)}, pad)...)
	out := make([]byte, aes.BlockSize+len(plain))
	_, err = rand.Read(out[:aes.BlockSize])
	require.NoError(t, err)
	cipher.NewCBCEncrypter(block, out[:aes.BlockSize]).CryptBlocks(out[aes.BlockSize:], plain)
	return base64.StdEncoding.EncodeToString(out)
}

func webhookProvider(t *testing.T, encryptKey string) (*Provider, chan core.Message, context.CancelFunc) {
	t.Helper()
	if encryptKey == "" {
		encryptKey = "encrypt-key-fixture"
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal") {
			tokenResponse(w)
			return
		}
		_, _ = io.WriteString(w, `{"code":0,"bot":{"open_id":"ou-bot"}}`)
	}))
	t.Cleanup(server.Close)
	secrets := mapSecret{"env:APP": "app-secret", "env:TOKEN": "verify-token"}
	config := ProviderConfig{Binding: core.Binding{ID: "binding-1", AppID: "app-1", GroupTrigger: "mention"}, AppSecretRef: "env:APP", SecretResolver: secrets, OpenBaseURL: server.URL, ReceiveMode: ReceiveWebhook, VerificationTokenRef: "env:TOKEN"}
	if encryptKey != "" {
		secrets["env:ENCRYPT"] = encryptKey
		config.EncryptKeyRef = "env:ENCRYPT"
	}
	p, err := NewProvider(config)
	require.NoError(t, err)
	messages := make(chan core.Message, 4)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- p.Start(ctx, func(_ context.Context, m core.Message) error { messages <- m; return nil })
	}()
	t.Cleanup(func() { cancel(); require.NoError(t, <-done) })
	select {
	case <-p.Ready():
	case <-time.After(3 * time.Second):
		t.Fatal("webhook provider not ready")
	}
	return p, messages, cancel
}

func messageEvent(token string) []byte {
	event := map[string]any{
		"schema": "2.0",
		"header": map[string]any{"event_id": "ev-webhook", "event_type": "im.message.receive_v1", "create_time": "1700000000000", "token": token, "app_id": "app-1", "tenant_key": "tenant-1"},
		"event": map[string]any{
			"sender":  map[string]any{"sender_id": map[string]any{"open_id": "ou-sender"}, "sender_type": "user"},
			"message": map[string]any{"message_id": "om-webhook", "chat_id": "oc-1", "chat_type": "p2p", "message_type": "text", "content": `{"text":"hi from webhook"}`},
		},
	}
	data, _ := json.Marshal(event)
	return data
}

func post(t *testing.T, handler http.Handler, body []byte, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/feishu/events", bytes.NewReader(body))
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func TestWebhookEncryptedChallengeAndMessage(t *testing.T) {
	p, messages, _ := webhookProvider(t, "")
	rec := signedPost(t, p, []byte(`{"type":"url_verification","challenge":"c-123","token":"verify-token"}`))
	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{"challenge":"c-123"}`, rec.Body.String())
	rec = signedPost(t, p, []byte(`{"type":"url_verification","challenge":"c-123","token":"wrong"}`))
	require.Equal(t, http.StatusInternalServerError, rec.Code, "a forged challenge is rejected")
	rec = signedPost(t, p, messageEvent("verify-token"))
	require.Equal(t, http.StatusOK, rec.Code)
	select {
	case message := <-messages:
		require.Equal(t, "hi from webhook", message.Text)
		require.Equal(t, "om-webhook", message.MessageID)
		require.Equal(t, "binding-1", message.BindingID)
	case <-time.After(2 * time.Second):
		t.Fatal("message not accepted")
	}
}

func TestWebhookEncryptedRequiresValidSignature(t *testing.T) {
	const key = "encrypt-key-fixture"
	p, messages, _ := webhookProvider(t, key)
	handler := p.WebhookHandler()
	body, err := json.Marshal(map[string]string{"encrypt": encryptEvent(t, key, messageEvent("verify-token"))})
	require.NoError(t, err)
	timestamp, nonce := strconv.FormatInt(time.Now().Unix(), 10), "nonce-1"
	signature := larkevent.Signature(timestamp, nonce, key, string(body))
	rec := post(t, handler, body, map[string]string{"X-Lark-Request-Timestamp": timestamp, "X-Lark-Request-Nonce": nonce, "X-Lark-Signature": "forged"})
	require.Equal(t, http.StatusInternalServerError, rec.Code, "bad signature is rejected")
	select {
	case <-messages:
		t.Fatal("forged request must not reach the inbox")
	default:
	}
	rec = post(t, handler, body, map[string]string{"X-Lark-Request-Timestamp": timestamp, "X-Lark-Request-Nonce": nonce, "X-Lark-Signature": signature})
	require.Equal(t, http.StatusOK, rec.Code)
	select {
	case message := <-messages:
		require.Equal(t, "hi from webhook", message.Text)
	case <-time.After(2 * time.Second):
		t.Fatal("encrypted message not accepted")
	}
	plain := post(t, handler, messageEvent("verify-token"), nil)
	require.NotEqual(t, http.StatusOK, plain.Code, "plaintext is refused once encryption is configured")
}

func TestWebhookUnavailableBeforeStartAndLimits(t *testing.T) {
	p, err := NewProvider(ProviderConfig{Binding: core.Binding{ID: "b", AppID: "app"}, AppSecretRef: "env:APP", SecretResolver: mapSecret{"env:APP": "s", "env:TOKEN": "t", "env:ENCRYPT": "fixture"}, ReceiveMode: ReceiveWebhook, VerificationTokenRef: "env:TOKEN", EncryptKeyRef: "env:ENCRYPT"})
	require.NoError(t, err)
	rec := post(t, p.WebhookHandler(), []byte(`{}`), nil)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code, "feishu retries until the provider is ready")
	_, err = NewProvider(ProviderConfig{Binding: core.Binding{ID: "b", AppID: "app"}, AppSecretRef: "env:APP", SecretResolver: mapSecret{"env:APP": "s"}, ReceiveMode: ReceiveWebhook})
	require.Error(t, err, "webhook requires a verification token")
	_, err = NewProvider(ProviderConfig{Binding: core.Binding{ID: "b", AppID: "app"}, AppSecretRef: "env:APP", SecretResolver: mapSecret{"env:APP": "s"}, ReceiveMode: "poll"})
	require.Error(t, err)
	ready, _, _ := webhookProvider(t, "")
	handler := ready.WebhookHandler()
	get := httptest.NewRecorder()
	handler.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/feishu/events", nil))
	require.Equal(t, http.StatusMethodNotAllowed, get.Code)
	big := post(t, handler, bytes.Repeat([]byte("x"), maxWebhookBody+1), nil)
	require.Equal(t, http.StatusRequestEntityTooLarge, big.Code)
}

func TestWebhookConcurrentDeliveryIsSafe(t *testing.T) {
	p, messages, _ := webhookProvider(t, "")
	var wg sync.WaitGroup
	codes := make(chan int, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			codes <- signedPost(t, p, messageEvent("verify-token")).Code
		}()
	}
	received := 0
	deadline := time.After(3 * time.Second)
	for received < 8 {
		select {
		case <-messages:
			received++
		case <-deadline:
			t.Fatalf("received %d of 8", received)
		}
	}
	wg.Wait()
	close(codes)
	for code := range codes {
		require.Equal(t, http.StatusOK, code)
	}
}

func TestWebhookRejectsConfigurationWithoutSigningKey(t *testing.T) {
	for _, ref := range []string{"", "raw", "env:ABSENT", "env:EMPTY"} {
		t.Run(ref, func(t *testing.T) {
			_, err := NewProvider(ProviderConfig{Binding: core.Binding{ID: "binding", AppID: "app"},
				AppSecretRef: "env:APP", ReceiveMode: ReceiveWebhook,
				VerificationTokenRef: "env:TOKEN", EncryptKeyRef: ref,
				SecretResolver: mapSecret{"env:APP": "fixture", "env:TOKEN": "token", "env:EMPTY": ""}})
			require.Error(t, err, "webhook must not start without an available signing key")
		})
	}
}

func signedPost(t *testing.T, p *Provider, plain []byte) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(map[string]string{"encrypt": encryptEvent(t, p.encryptKey, plain)})
	require.NoError(t, err)
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	return post(t, p.WebhookHandler(), body, map[string]string{
		"X-Lark-Request-Timestamp": timestamp, "X-Lark-Request-Nonce": "nonce-fixture",
		"X-Lark-Signature": larkevent.Signature(timestamp, "nonce-fixture", p.encryptKey, string(body)),
	})
}

func TestWebhookUnsignedMessageNeverReachesInbox(t *testing.T) {
	p, messages, _ := webhookProvider(t, "")
	for _, token := range []string{"verify-token", "forged-token", ""} {
		rec := post(t, p.WebhookHandler(), messageEvent(token), nil)
		require.NotEqual(t, http.StatusOK, rec.Code)
	}
	select {
	case <-messages:
		t.Fatal("unsigned message reached inbox")
	default:
	}
}
