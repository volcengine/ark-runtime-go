// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0
package selfhosted

import "context"

// EventStreamer 是支持 session SSE 事件流的可选 API 能力。
type EventStreamer interface {
	StreamEvents(ctx context.Context, req StreamEventsRequest) (*EventStream, error)
}

// EventStream 是 session event stream 的读取句柄。
type EventStream struct {
	events <-chan Event
	close  func() error
	err    func() error
}

// Events 返回事件通道。
func (s *EventStream) Events() <-chan Event {
	if s == nil {
		ch := make(chan Event)
		close(ch)
		return ch
	}
	return s.events
}

// Close 关闭事件流。
func (s *EventStream) Close() error {
	if s == nil || s.close == nil {
		return nil
	}
	return s.close()
}

// Err 返回事件流结束后的错误。
func (s *EventStream) Err() error {
	if s == nil || s.err == nil {
		return nil
	}
	return s.err()
}
