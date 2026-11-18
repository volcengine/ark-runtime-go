// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"encoding/json"
	"net/http"
	"strconv"
	"testing"
	"time"

	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
	"github.com/stretchr/testify/require"
)

func TestWebhookRejectsStaleOrInvalidSignatureTimestamp(t *testing.T) {
	for _, timestamp := range []string{"1700000000", "", "not-a-time", "-1", "9223372036854775808", strconv.FormatInt(time.Now().Add(10*time.Minute).Unix(), 10)} {
		t.Run(timestamp, func(t *testing.T) {
			p, messages, _ := webhookProvider(t, "")
			body, err := json.Marshal(map[string]string{"encrypt": encryptEvent(t, p.encryptKey, messageEvent("verify-token"))})
			require.NoError(t, err)
			rec := post(t, p.WebhookHandler(), body, map[string]string{
				"X-Lark-Request-Timestamp": timestamp, "X-Lark-Request-Nonce": "nonce",
				"X-Lark-Signature": larkevent.Signature(timestamp, "nonce", p.encryptKey, string(body)),
			})
			require.NotEqual(t, http.StatusOK, rec.Code)
			select {
			case <-messages:
				t.Fatal("expired or invalid request reached the inbox")
			default:
			}
		})
	}
}

func TestWebhookCurrentSignatureAcceptsDelayedEvent(t *testing.T) {
	p, messages, _ := webhookProvider(t, "")
	require.Equal(t, http.StatusOK, signedPost(t, p, messageEvent("verify-token")).Code)
	select {
	case message := <-messages:
		require.Equal(t, "om-webhook", message.MessageID)
	case <-time.After(time.Second):
		t.Fatal("current signed callback was not accepted")
	}
}

func TestWebhookEncryptedChallengeNeedsNoSignatureHeaders(t *testing.T) {
	p, messages, _ := webhookProvider(t, "")
	for _, plain := range []string{
		`{"type":"url_verification","challenge":"challenge","token":"verify-token"}`,
		`{"type":"url_verification","challenge":"challenge","header":{"token":"verify-token","event_type":"im.message.receive_v1"},"event":{}}`,
	} {
		body, err := json.Marshal(map[string]string{"encrypt": encryptEvent(t, p.encryptKey, []byte(plain))})
		require.NoError(t, err)
		rec := post(t, p.WebhookHandler(), body, nil)
		require.Equal(t, http.StatusOK, rec.Code)
		require.JSONEq(t, `{"challenge":"challenge"}`, rec.Body.String())
	}
	for _, plain := range []string{
		`{"type":"url_verification","challenge":"challenge","token":"wrong"}`,
		`{"type":"url_verification","challenge":"challenge"}`,
		`malformed}{`,
	} {
		body, err := json.Marshal(map[string]string{"encrypt": encryptEvent(t, p.encryptKey, []byte(plain))})
		require.NoError(t, err)
		require.NotEqual(t, http.StatusOK, post(t, p.WebhookHandler(), body, nil).Code)
	}
	body, err := json.Marshal(map[string]string{"encrypt": encryptEvent(t, p.encryptKey, messageEvent("verify-token"))})
	require.NoError(t, err)
	require.NotEqual(t, http.StatusOK, post(t, p.WebhookHandler(), body, nil).Code, "business events cannot fall back to token verification")
	require.NotEqual(t, http.StatusOK, post(t, p.WebhookHandler(), []byte(`{"type":"url_verification","challenge":"challenge","token":"verify-token"}`), nil).Code)
	select {
	case <-messages:
		t.Fatal("challenge must not execute a business handler")
	default:
	}
}

func TestWebhookTimestampWindowBoundaries(t *testing.T) {
	now := time.Unix(1800000000, 0)
	for _, tc := range []struct {
		offset time.Duration
		valid  bool
	}{
		{-5*time.Minute - time.Second, false}, {-5 * time.Minute, true},
		{0, true}, {5 * time.Minute, true}, {5*time.Minute + time.Second, false},
	} {
		require.Equal(t, tc.valid, validWebhookTimestamp(strconv.FormatInt(now.Add(tc.offset).Unix(), 10), now))
	}
}
