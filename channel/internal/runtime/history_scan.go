// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"time"
)

const historyRefreshInterval = 30 * time.Second

type historySnapshot struct {
	events      []Event
	tailPage    string
	tailStart   int
	prefixPages int
}

// historyScanner 只属于一次观察调用；缓存未持久化，接管或重启自然重新对账。
type historyScanner struct {
	client       MAClient
	sessionID    string
	since        *time.Time
	clock        func() time.Time
	snapshot     historySnapshot
	initialized  bool
	lastFull     time.Time
	lastReadFull bool
}

func (s *historyScanner) read(ctx context.Context, forceFull bool) ([]Event, error) {
	now := s.clock()
	full := forceFull || !s.initialized || now.Before(s.lastFull) || now.Sub(s.lastFull) >= historyRefreshInterval
	var prefix *historySnapshot
	if !full {
		prefix = &s.snapshot
	}
	next, err := readHistory(ctx, s.client, s.sessionID, s.since, prefix)
	if !full && (errors.Is(err, errHistoryConflict) || errors.Is(err, errHistoryLimit) || invalidHistoryCursor(err)) {
		// 缓存与当前排序可能因迟到或重投发生分歧；用完整历史重建，不能据缓存直接失败。
		full = true
		next, err = readHistory(ctx, s.client, s.sessionID, s.since, nil)
	}
	if err != nil {
		return nil, err
	}
	// 全部页成功才提交缓存；部分页失败时仍从原 tailPage 重试。
	s.snapshot, s.initialized, s.lastReadFull = next, true, full
	if full {
		s.lastFull = s.clock()
	}
	return next.events, nil
}

func invalidHistoryCursor(err error) bool {
	status, code := apiError(err)
	return status == 410 || status == 400 && (code == "InvalidParameter" || strings.HasPrefix(code, "InvalidParameter."))
}

func (s *historyScanner) readTurn(ctx context.Context, j *job) ([]Event, error) {
	events, err := s.read(ctx, false)
	if err != nil || s.lastReadFull {
		return events, err
	}
	outcome := judgeTurn(events, j)
	if outcome.Status != "" || outcome.WaitingAction {
		// seq 是生产时间，迟到事件可能落在缓存前缀；任何终态或审批副作用前必须完整校验。
		return s.read(ctx, true)
	}
	return events, nil
}

func scanEvents(ctx context.Context, client MAClient, sessionID string, since *time.Time) ([]Event, error) {
	snapshot, err := readHistory(ctx, client, sessionID, since, nil)
	return snapshot.events, err
}

func readHistory(ctx context.Context, client MAClient, sessionID string, since *time.Time, prefix *historySnapshot) (historySnapshot, error) {
	const maxPages, maxEvents, maxBytes = 100, 10000, 16 << 20
	page, prefixPages := "", 0
	var events []Event
	byID := make(map[string]Event)
	bytesUsed := 0
	appendEvent := func(event Event) error {
		if event.ID == "" {
			return errHistoryConflict
		}
		if old, ok := byID[event.ID]; ok {
			candidate := event
			old.Raw, candidate.Raw = nil, nil
			if !reflect.DeepEqual(old, candidate) {
				return errHistoryConflict
			}
			return nil
		}
		event = compactHistoryEvent(event)
		bytesUsed += historyEventBytes(event)
		if bytesUsed > maxBytes || len(events) >= maxEvents {
			return errHistoryLimit
		}
		byID[event.ID] = event
		events = append(events, event)
		return nil
	}
	if prefix != nil {
		page, prefixPages = prefix.tailPage, prefix.prefixPages
		for _, event := range prefix.events[:prefix.tailStart] {
			if err := appendEvent(event); err != nil {
				return historySnapshot{}, err
			}
		}
	}
	pages := map[string]bool{page: true}
	for i := prefixPages; i < maxPages; i++ {
		tailStart := len(events)
		result, err := client.ListEvents(ctx, sessionID, EventQuery{Page: page, Limit: 200, Types: observedTypes, CreatedAtGte: since})
		if err != nil {
			if errors.Is(err, ErrEventTooLarge) {
				err = errors.Join(errHistoryLimit, err)
			}
			return historySnapshot{}, err
		}
		for _, event := range result.Events {
			if err := appendEvent(event); err != nil {
				return historySnapshot{}, err
			}
		}
		if result.NextPage == "" {
			return historySnapshot{events: events, tailPage: page, tailStart: tailStart, prefixPages: i}, nil
		}
		if pages[result.NextPage] {
			return historySnapshot{}, errHistoryLimit
		}
		pages[result.NextPage] = true
		page = result.NextPage
	}
	return historySnapshot{}, errHistoryLimit
}
