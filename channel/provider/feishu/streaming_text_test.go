// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

func TestCardKitTransientBusinessErrorsAllowHigherSequenceRetry(t *testing.T) {
	for _, code := range []int{300120, 200810} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			failed := false
			var lastSequence uint64
			p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal") {
					tokenResponse(w)
					return
				}
				require.NotEqual(t, http.MethodPost, r.Method, "recovery must retain the original message")
				var body struct{ Sequence uint64 }
				require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
				require.Greater(t, body.Sequence, lastSequence)
				lastSequence = body.Sequence
				if strings.HasSuffix(r.URL.Path, "/content") && !failed {
					failed = true
					w.WriteHeader(http.StatusBadRequest)
					_, _ = io.WriteString(w, `{"code":`+strconv.Itoa(code)+`}`)
					return
				}
				_, _ = io.WriteString(w, `{"code":0}`)
			})
			d := streamDelivery("final")
			d.TextStream = &core.TextStreamState{Handle: "7355439197428236291", MessageID: "original", FirstSendAt: time.Now(), Position: 1, PendingPosition: 1, LastContentAt: time.Now()}
			save := func(_ context.Context, next core.TextStreamState) error {
				require.NoError(t, d.TextStream.ValidateUpdate(next))
				*d.TextStream = next
				return nil
			}
			_, err := p.DeliverTextStream(t.Context(), d, save)
			var failure core.DeliveryFailure
			require.True(t, errors.As(err, &failure))
			require.True(t, failure.Retryable(), "documented transient HTTP400 must remain recoverable")
			_, err = p.DeliverTextStream(t.Context(), d, save)
			require.NoError(t, err)
			require.Equal(t, 5, d.TextStream.Position)
			require.Equal(t, "original", d.TextStream.MessageID)
		})
	}
}

func streamDelivery(text string) core.Delivery {
	return core.NewDelivery(core.DeliverySource{Kind: core.SourceTurn, ID: "turn"}, 0, "answer", "binding", core.ReplyTarget{ChatID: "chat", MessageID: "source", Mode: "thread"}, core.StreamTextBody(text))
}

func TestTextStreamUsesOneCardAndConfirmedFinalText(t *testing.T) {
	const cardID, messageID = "7355439197428236291", "om-result"
	text := "最终答案🙂完成"
	var mu sync.Mutex
	state := core.TextStreamState{}
	creates, sends := 0, 0
	var contents []string
	var sequences []uint64
	closed := false
	p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal") {
			tokenResponse(w)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/open-apis/cardkit/v1/cards":
			creates++
			var body struct{ Type, Data string }
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Equal(t, "card_json", body.Type)
			var card struct {
				Config struct {
					Streaming bool `json:"streaming_mode"`
				}
				Body struct{ Elements []struct{ Content string } }
			}
			require.NoError(t, json.Unmarshal([]byte(body.Data), &card))
			require.True(t, card.Config.Streaming)
			require.Equal(t, "最", card.Body.Elements[0].Content, "the first character also comes from the confirmed final answer")
			_, _ = io.WriteString(w, `{"code":0,"data":{"card_id":"`+cardID+`"}}`)
		case r.URL.Path == "/open-apis/im/v1/messages/source/reply":
			sends++
			require.Equal(t, cardID, state.Handle, "card handle must be durable before message send")
			require.False(t, state.FirstSendAt.IsZero())
			var body struct {
				MsgType       string `json:"msg_type"`
				Content, UUID string
				Thread        bool `json:"reply_in_thread"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Equal(t, "interactive", body.MsgType)
			require.Equal(t, streamDelivery(text).UUID, body.UUID)
			require.True(t, body.Thread)
			require.JSONEq(t, `{"type":"card","data":{"card_id":"`+cardID+`"}}`, body.Content)
			_, _ = io.WriteString(w, `{"code":0,"data":{"message_id":"`+messageID+`"}}`)
		case strings.HasSuffix(r.URL.Path, "/content"):
			var body struct {
				Content, UUID string
				Sequence      uint64
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Equal(t, state.Sequence, body.Sequence, "sequence must be reserved before the HTTP operation")
			require.Equal(t, state.PendingPosition, utf8.RuneCountInString(body.Content))
			require.True(t, utf8.ValidString(body.Content))
			require.True(t, strings.HasPrefix(text, body.Content))
			require.NotEmpty(t, body.UUID)
			contents = append(contents, body.Content)
			sequences = append(sequences, body.Sequence)
			_, _ = io.WriteString(w, `{"code":0}`)
		case strings.HasSuffix(r.URL.Path, "/settings"):
			var body struct {
				Settings string
				Sequence uint64
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Equal(t, state.Sequence, body.Sequence)
			if strings.Contains(body.Settings, `"streaming_mode":false`) {
				require.Equal(t, utf8.RuneCountInString(text), state.Position)
				closed = true
			}
			sequences = append(sequences, body.Sequence)
			_, _ = io.WriteString(w, `{"code":0}`)
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	})
	port, ok := any(p).(core.TextStreamProvider)
	require.True(t, ok)
	save := func(_ context.Context, next core.TextStreamState) error {
		mu.Lock()
		defer mu.Unlock()
		require.NoError(t, state.ValidateUpdate(next))
		state = next
		return nil
	}
	d := streamDelivery(text)
	result, err := port.DeliverTextStream(t.Context(), d, save)
	require.NoError(t, err)
	require.Empty(t, result.MessageID)
	require.False(t, result.RetryAt.IsZero())
	time.Sleep(time.Until(result.RetryAt))
	d.TextStream = &state
	result, err = port.DeliverTextStream(t.Context(), d, save)
	require.NoError(t, err)
	require.Equal(t, messageID, result.MessageID)
	require.True(t, state.Done)
	require.True(t, closed)
	require.Equal(t, 1, creates)
	require.Equal(t, 1, sends)
	require.Equal(t, text, contents[len(contents)-1])
	for i := 1; i < len(sequences); i++ {
		require.Greater(t, sequences[i], sequences[i-1])
	}
}

func TestTextStreamResumeReopensOriginalCard(t *testing.T) {
	var reopened bool
	p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal") {
			tokenResponse(w)
			return
		}
		require.NotEqual(t, http.MethodPost, r.Method, "resume cannot create or send a replacement")
		if strings.HasSuffix(r.URL.Path, "/settings") {
			reopened = true
		} else {
			require.True(t, reopened, "an automatically closed partial card must reopen before content")
		}
		_, _ = io.WriteString(w, `{"code":0}`)
	})
	port, ok := any(p).(core.TextStreamProvider)
	require.True(t, ok)
	d := streamDelivery("final")
	d.TextStream = &core.TextStreamState{Handle: "7355439197428236291", MessageID: "message", Sequence: 7, Position: 2, PendingPosition: 3, FirstSendAt: time.Now(), LastContentAt: time.Now()}
	_, err := port.DeliverTextStream(t.Context(), d, func(_ context.Context, next core.TextStreamState) error {
		require.NoError(t, d.TextStream.ValidateUpdate(next))
		*d.TextStream = next
		return nil
	})
	require.NoError(t, err)
	require.True(t, reopened)
	require.Equal(t, 5, d.TextStream.Position)
}

func TestTextStreamRejectsExpiredUnknownSendAndOversizeBeforeHTTP(t *testing.T) {
	p := newFixtureProvider(t, func(_ http.ResponseWriter, _ *http.Request) {
		t.Error("unsafe or oversized delivery must not send HTTP")
	})
	port, ok := any(p).(core.TextStreamProvider)
	require.True(t, ok)
	for _, d := range []core.Delivery{
		func() core.Delivery {
			d := streamDelivery("final")
			d.TextStream = &core.TextStreamState{Handle: "7355439197428236291", FirstSendAt: time.Now().Add(-2 * time.Hour)}
			return d
		}(),
		streamDelivery(strings.Repeat("a", 24000)),
	} {
		_, err := port.DeliverTextStream(t.Context(), d, func(context.Context, core.TextStreamState) error { return nil })
		require.Error(t, err)
	}
}

func TestTextStreamUnknownRequestsResumeWithoutReplacement(t *testing.T) {
	for _, fault := range []string{"send", "content", "close"} {
		t.Run(fault, func(t *testing.T) {
			var mu sync.Mutex
			state := core.TextStreamState{}
			creates, sends := 0, 0
			failed, closed := false, false
			lastText := ""
			var lastSequence uint64
			uuids := map[string]bool{}
			p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal") {
					tokenResponse(w)
					return
				}
				mu.Lock()
				defer mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				stage := ""
				switch {
				case r.URL.Path == "/open-apis/cardkit/v1/cards":
					creates++
					_, _ = io.WriteString(w, `{"code":0,"data":{"card_id":"7355439197428236291"}}`)
					return
				case strings.Contains(r.URL.Path, "/im/v1/"):
					sends++
					stage = "send"
					var body struct{ UUID string }
					require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
					uuids[body.UUID] = true
				default:
					var body struct {
						Content, Settings string
						Sequence          uint64
					}
					require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
					require.Greater(t, body.Sequence, lastSequence)
					lastSequence = body.Sequence
					if strings.HasSuffix(r.URL.Path, "/content") {
						stage = "content"
						require.True(t, strings.HasPrefix(body.Content, lastText))
						lastText = body.Content
					} else if strings.Contains(body.Settings, `"streaming_mode":false`) {
						stage = "close"
						closed = true
					} else {
						require.False(t, closed, "confirmed full content must not reopen after closing")
					}
				}
				if stage == fault && !failed {
					failed = true
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = io.WriteString(w, `{"code":999}`)
					return
				}
				if stage == "send" {
					_, _ = io.WriteString(w, `{"code":0,"data":{"message_id":"original-result"}}`)
				} else {
					_, _ = io.WriteString(w, `{"code":0}`)
				}
			})
			port := any(p).(core.TextStreamProvider)
			save := func(_ context.Context, next core.TextStreamState) error {
				mu.Lock()
				defer mu.Unlock()
				require.NoError(t, state.ValidateUpdate(next))
				state = next
				return nil
			}
			d := streamDelivery("最终🙂结果")
			var result core.TextStreamResult
			for n := 0; n < 8; n++ {
				mu.Lock()
				snapshot := state
				mu.Unlock()
				d.TextStream = &snapshot
				var err error
				result, err = port.DeliverTextStream(t.Context(), d, save)
				if err != nil {
					continue
				}
				if result.MessageID != "" {
					break
				}
				time.Sleep(time.Until(result.RetryAt))
			}
			require.Equal(t, "original-result", result.MessageID)
			require.True(t, failed)
			require.True(t, closed)
			require.True(t, state.Done)
			require.Equal(t, 1, creates)
			require.Len(t, uuids, 1)
			require.Equal(t, d.Body.Text, lastText)
			if fault == "send" {
				require.Equal(t, 2, sends)
			} else {
				require.Equal(t, 1, sends)
			}
		})
	}
}

func TestTextStreamDoesNotPublishBeforeCheckpointConfirmation(t *testing.T) {
	creates, sends := 0, 0
	p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal") {
			tokenResponse(w)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/open-apis/cardkit/v1/cards" {
			creates++
			_, _ = io.WriteString(w, `{"code":0,"data":{"card_id":"7355439197428236291"}}`)
			return
		}
		sends++
		_, _ = io.WriteString(w, `{"code":0}`)
	})
	_, err := p.DeliverTextStream(t.Context(), streamDelivery("final"), func(context.Context, core.TextStreamState) error { return core.ErrLeaseLost })
	require.ErrorIs(t, err, core.ErrLeaseLost)
	require.Equal(t, 1, creates)
	require.Zero(t, sends)
}

func TestCardKitMissingCodeIsNotAcknowledged(t *testing.T) {
	for _, raw := range []string{`{}`, `{"code":230001}`} {
		t.Run(raw, func(t *testing.T) {
			p := newFixtureProvider(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/tenant_access_token/internal") {
					tokenResponse(w)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, raw)
			})
			err := p.configureTextStream(t.Context(), core.TextStreamState{Handle: "7355439197428236291", Sequence: 1}, "operation", false, "final")
			require.Error(t, err)
		})
	}
}
