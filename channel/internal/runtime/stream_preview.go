// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"sort"
	"strings"
	"sync"
	"unicode/utf8"
)

const maxPreviewRunes = 16000

// streamPreview 是有界的尽力预览缓存；历史输入锚点和明确主线程归属缺一不可。
type streamPreview struct {
	mu                 sync.Mutex
	sessionID, inputID string
	anchored           bool
	active, text       string
	blocks             map[int]string
	ignored, started   map[string]bool
}

func newStreamPreview(sessionID, inputID string) *streamPreview {
	return &streamPreview{sessionID: sessionID, inputID: inputID, ignored: map[string]bool{}, started: map[string]bool{}}
}

func (p *streamPreview) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blocks, p.started = nil, map[string]bool{}
	p.active, p.text = "", ""
}

func (p *streamPreview) confirm(events []Event, j *job) {
	p.mu.Lock()
	defer p.mu.Unlock()
	index := -1
	for i, event := range events {
		if event.ID == p.inputID && matchingHistoryInput(event, j) {
			index = i
			break
		}
		p.ignored[event.ID] = true
	}
	if index < 0 {
		return
	}
	p.anchored = true
	for _, event := range events[index+1:] {
		if !onMainThread(event, p.sessionID) {
			continue
		}
		if event.Type == "user.message" {
			p.anchored, p.text = false, ""
			return
		}
		if event.Type != "agent.message" {
			continue
		}
		p.ignored[event.ID] = true
		delete(p.started, event.ID)
		if text, ok := messageText(event); ok && strings.TrimSpace(text) != "" && utf8.RuneCountInString(text) <= maxPreviewRunes {
			if p.blocks == nil || p.active == event.ID {
				p.active, p.text, p.blocks = event.ID, text, nil
			}
		}
	}
}

func (p *streamPreview) accept(event Event) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.anchored || event.SessionThreadID == "" || event.SessionThreadID != mainThread(p.sessionID) ||
		event.Preview == nil {
		return
	}
	v := event.Preview
	if p.ignored[v.EventID] {
		return
	}
	if event.Type == "event_start" {
		if p.started[v.EventID] {
			p.ignored[v.EventID] = true
			if p.active == v.EventID {
				p.blocks, p.text = nil, ""
			}
			return
		}
		if v.EventType != "agent.message" || len(p.started) >= 8 {
			return
		}
		p.started[v.EventID] = true
		p.active, p.text, p.blocks = v.EventID, "", map[int]string{}
		return
	}
	if event.Type != "event_delta" || v.EventID != p.active || p.blocks == nil || v.Index < 0 || v.Index >= 128 {
		return
	}
	p.blocks[v.Index] += v.Text
	keys := make([]int, 0, len(p.blocks))
	for key := range p.blocks {
		keys = append(keys, key)
	}
	sort.Ints(keys)
	var parts []string
	for _, key := range keys {
		parts = append(parts, p.blocks[key])
	}
	text := strings.Join(parts, "\n")
	if utf8.RuneCountInString(text) > maxPreviewRunes {
		p.ignored[p.active], p.blocks, p.text = true, nil, ""
		return
	}
	p.text = text
}

func (p *streamPreview) snapshot() (string, string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.active, p.text
}
