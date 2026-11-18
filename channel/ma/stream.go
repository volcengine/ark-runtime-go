// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package ma

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/session"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

type eventStream struct {
	reader   *session.StreamDecoder
	ctx      context.Context
	cancel   context.CancelFunc
	once     sync.Once
	threadID string
}

// StreamEvents 打开公开事件流；调用方必须关闭它，EOF 不代表执行完成。
func (c *Client) StreamEvents(ctx context.Context, sessionID string) (core.EventStream, error) {
	path, err := sessionPath(sessionID)
	if err != nil {
		return nil, err
	}
	return c.openStream(ctx, path+"/events/stream", nil, "")
}

// StreamEventPreviews 订阅主线程文本/思考增量，归属由订阅范围证明。
func (c *Client) StreamEventPreviews(ctx context.Context, sessionID string) (core.EventStream, error) {
	path, err := sessionPath(sessionID)
	if err != nil {
		return nil, err
	}
	suffix, ok := strings.CutPrefix(sessionID, "sesn-")
	if !ok || suffix == "" {
		return nil, errors.New("ma primary thread unavailable")
	}
	threadID := "sthr-" + suffix
	return c.openStream(ctx, path+"/threads/"+threadID+"/stream", url.Values{"event_deltas": {"agent.message", "agent.thinking"}}, threadID)
}

func (c *Client) openStream(ctx context.Context, path string, query url.Values, threadID string) (core.EventStream, error) {
	streamCtx, cancel := context.WithTimeout(ctx, c.streamTimeout)
	sessionID := strings.SplitN(strings.TrimPrefix(path, "/sessions/"), "/", 2)[0]
	decoder, requestCancel, err := callSDK(c, streamCtx, http.MethodGet, path, sdkCallOptions{Timeout: c.streamTimeout}, func(ctx context.Context, sdk *arkruntime.Client, options []arkruntime.RequestOption) (*session.StreamDecoder, error) {
		if threadID == "" {
			return sdk.StreamSessionEvents(ctx, sessionID, options...)
		}
		params := &session.SessionThreadsStreamEventsParams{EventDeltas: query["event_deltas"]}
		return sdk.StreamSessionThreadEventsWithParams(ctx, sessionID, threadID, params, options...)
	})
	if err != nil {
		cancel()
		return nil, err
	}
	return &eventStream{reader: decoder, ctx: streamCtx, cancel: func() { requestCancel(); cancel() }, threadID: threadID}, nil
}

func (s *eventStream) project(data session.ManagedAgentsSessionEvent) (core.Event, error) {
	event, err := projectSDKEvent(data)
	if err != nil || s.threadID == "" || event.Preview == nil {
		return event, err
	}
	if event.SessionThreadID != "" && event.SessionThreadID != s.threadID {
		return core.Event{}, errors.New("ma preview thread conflicts with subscription")
	}
	// 仅主线程专用订阅的预览可补归属；不修改 wire、history 或普通 session 流。
	event.SessionThreadID = s.threadID
	// 此端点使用实时 pubsub，不回放增量；断线后可能从 delta 中途继续。
	event.Preview.Live = true
	return event, nil
}

// Close 释放 SSE 响应和关联的请求上下文。
func (s *eventStream) Close() error {
	var err error
	s.once.Do(func() { s.cancel(); err = s.reader.Close() })
	return err
}

// Recv 将主 SDK 的 typed SSE 事件映射为领域事件；EOF 不表示 Agent 完成。
func (s *eventStream) Recv(ctx context.Context) (core.Event, error) {
	if err := ctx.Err(); err != nil {
		return core.Event{}, err
	}
	if err := s.ctx.Err(); err != nil {
		return core.Event{}, err
	}
	stop := context.AfterFunc(ctx, func() { _ = s.Close() })
	defer stop()
	if !s.reader.Next() {
		if ctx.Err() != nil {
			return core.Event{}, ctx.Err()
		}
		if s.ctx.Err() != nil {
			return core.Event{}, s.ctx.Err()
		}
		if err := s.reader.Err(); err != nil {
			return core.Event{}, err
		}
		return core.Event{}, io.EOF
	}
	return s.project(s.reader.Event().Data)
}
