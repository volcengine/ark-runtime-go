// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestMessageReactionReplacesOnlyOwnState(t *testing.T) {
	for _, tc := range []struct {
		state     core.MessageReaction
		old, next string
	}{
		{core.ReactionRunning, "OneSecond", "Get"},
		{core.ReactionDone, "Get", "DONE"},
		{"", "Get", ""},
	} {
		t.Run(string(tc.state), func(t *testing.T) {
			var calls []string
			p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal") {
					tokenResponse(w)
					return
				}
				switch r.Method {
				case http.MethodGet:
					_, _ = fmt.Fprintf(w, `{"code":0,"data":{"items":[{"reaction_id":"mine","reaction_type":{"emoji_type":%q},"operator":{"operator_id":"app-1","operator_type":"app"}},{"reaction_id":"user","reaction_type":{"emoji_type":%q},"operator":{"operator_id":"ou-user","operator_type":"user"}}],"has_more":false}}`, tc.old, tc.old)
				case http.MethodDelete:
					require.True(t, strings.HasSuffix(r.URL.Path, "/mine"), "must never delete someone else's reaction")
					calls = append(calls, "delete")
					_, _ = w.Write([]byte(`{"code":0,"data":{}}`))
				case http.MethodPost:
					var body struct {
						ReactionType struct {
							EmojiType string `json:"emoji_type"`
						} `json:"reaction_type"`
					}
					require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
					calls = append(calls, body.ReactionType.EmojiType)
					_, _ = w.Write([]byte(`{"code":0,"data":{"reaction_id":"new"}}`))
				}
			})
			reporter, ok := any(p).(core.MessageReactionReporter)
			require.True(t, ok, "Feishu must implement processing reactions")
			require.NoError(t, reporter.SyncMessageReaction(context.Background(), core.ReplyTarget{ChatID: "oc-chat", MessageID: "om-original"}, tc.state))
			want := []string{"delete"}
			if tc.next != "" {
				want = append(want, tc.next)
			}
			require.Equal(t, want, calls)
		})
	}
}

func TestMessageReactionAlreadyAppliedAndGone(t *testing.T) {
	for _, gone := range []bool{false, true} {
		p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
			if strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal") {
				tokenResponse(w)
				return
			}
			require.Equal(t, http.MethodGet, r.Method)
			w.Header().Set("Content-Type", "application/json")
			if gone {
				_, _ = w.Write([]byte(`{"code":231003,"msg":"missing"}`))
				return
			}
			_, _ = w.Write([]byte(`{"code":0,"data":{"items":[{"reaction_id":"existing","reaction_type":{"emoji_type":"DONE"},"operator":{"operator_id":"app-1","operator_type":"app"}}],"has_more":false}}`))
		})
		reporter, ok := any(p).(core.MessageReactionReporter)
		require.True(t, ok)
		require.NoError(t, reporter.SyncMessageReaction(context.Background(), core.ReplyTarget{MessageID: "om-original"}, core.ReactionDone))
	}
}

func TestMessageReactionProcessingRejectionCanRetry(t *testing.T) {
	p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal") {
			tokenResponse(w)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte(`{"code":0,"data":{"items":[],"has_more":false}}`))
			return
		}
		_, _ = w.Write([]byte(`{"code":231015,"msg":"request is processing"}`))
	})
	err := p.SyncMessageReaction(context.Background(), core.ReplyTarget{MessageID: "om-original"}, core.ReactionDone)
	var failure core.DeliveryFailure
	require.ErrorAs(t, err, &failure)
	require.True(t, failure.Retryable())
}
