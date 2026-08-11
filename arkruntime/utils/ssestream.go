// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
)

type Event struct {
	Type string
	Data []byte
}

// A base implementation of a Decoder for text/event-stream.
type EventStreamDecoder struct {
	evt    Event
	rc     io.ReadCloser
	reader *bufio.Reader
	err    error
}

func NewEventStreamDecoder(rc io.ReadCloser) *EventStreamDecoder {
	return &EventStreamDecoder{
		rc:     rc,
		reader: bufio.NewReader(rc),
	}
}

func (s *EventStreamDecoder) Next() bool {
	if s.err != nil {
		return false
	}

	event := ""
	data := bytes.NewBuffer(nil)

	for {
		txt, err := s.reader.ReadBytes('\n')
		if err != nil {
			s.err = err
			return false
		}

		// txt is trimed with leading space
		txt = bytes.TrimSpace(txt)

		// Dispatch event on an empty line
		if len(txt) == 0 {
			payload := data.Bytes()
			// The ark-managed-agents event stream (and other JSON-framed
			// SSE endpoints here) only emit `data: {..., "type": "..."}`
			// — no `event:` line. Peek the top-level "type" so callers
			// can read evt.Type instead of re-parsing evt.Data manually.
			// Explicit `event:` lines still win when the server sends
			// them; non-JSON payloads (e.g. `[DONE]`) fall through with
			// Type unset.
			if event == "" && len(payload) > 0 && payload[0] == '{' {
				var peek struct {
					Type string `json:"type"`
				}
				if json.Unmarshal(payload, &peek) == nil {
					event = peek.Type
				}
			}
			s.evt = Event{
				Type: event,
				Data: payload,
			}
			return true
		}

		// Split a string like "event: bar" into name="event" and value=" bar".
		name, value, _ := bytes.Cut(txt, []byte(":"))

		// Consume an optional space after the colon if it exists.
		if len(value) > 0 && value[0] == ' ' {
			value = value[1:]
		}

		switch string(name) {
		case "":
			// An empty line in the for ": something" is a comment and should be ignored.
			continue
		case "event":
			event = string(value)
		case "data":
			_, s.err = data.Write(value)
			if s.err != nil {
				return false
			}
			_, s.err = data.WriteRune('\n')
			if s.err != nil {
				return false
			}
		}
	}
}

func (s *EventStreamDecoder) Event() Event {
	return s.evt
}

func (s *EventStreamDecoder) Close() error {
	return s.rc.Close()
}

func (s *EventStreamDecoder) Err() error {
	return s.err
}
