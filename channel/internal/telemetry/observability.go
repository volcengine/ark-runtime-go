// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Package observability 提供 core.Observer 的日志与指标实现，以及 Runner 的本地管理 HTTP。
//
// 指标只用有限枚举（name/status/error_kind）作 label；任务、会话、路由 ID 只进日志。导出格式
// 为 Prometheus 文本协议，不引入额外依赖。
package observability

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

// maxSeries 限制指标序列数，防止异常输入撑爆内存；超出后归入 overflow 序列。
const maxSeries = 512

// durationBuckets 是耗时直方图的上界（秒）。
var durationBuckets = []float64{0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800}

type seriesKey struct {
	name      string
	status    string
	errorKind string
}

type histogram struct {
	counts []uint64
	sum    float64
	total  uint64
}

// Metrics 聚合观测记录为计数与耗时直方图，并发安全。
type Metrics struct {
	mu        sync.Mutex
	counters  map[seriesKey]uint64
	durations map[seriesKey]*histogram
	gauges    map[string]float64
	started   time.Time
}

// NewMetrics 返回空的指标聚合器。
func NewMetrics() *Metrics {
	return &Metrics{counters: map[seriesKey]uint64{}, durations: map[seriesKey]*histogram{}, gauges: map[string]float64{}, started: time.Now()}
}

func labelValue(value string) string {
	if value == "" {
		return ""
	}
	// label 只接受小写字母、数字、下划线；其他字符替换，避免注入导出格式。
	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r + 'a' - 'A')
		default:
			b.WriteByte('_')
		}
		if b.Len() >= 64 {
			break
		}
	}
	return b.String()
}

// Observe 实现 core.Observer。
func (m *Metrics) Observe(_ context.Context, item core.Observation) {
	key := seriesKey{name: labelValue(item.Name), status: labelValue(item.Status), errorKind: labelValue(item.ErrorKind)}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.counters[key]; !ok && len(m.counters) >= maxSeries {
		key = seriesKey{name: "overflow"}
	}
	m.counters[key]++
	if item.Duration > 0 {
		h, ok := m.durations[key]
		if !ok {
			if len(m.durations) >= maxSeries {
				return
			}
			h = &histogram{counts: make([]uint64, len(durationBuckets))}
			m.durations[key] = h
		}
		seconds := item.Duration.Seconds()
		for i, bound := range durationBuckets {
			if seconds <= bound {
				h.counts[i]++
			}
		}
		h.sum += seconds
		h.total++
	}
}

// SetGauge 设置瞬时值指标（如队列深度）；name 会被规范化。
func (m *Metrics) SetGauge(name string, value float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gauges[labelValue(name)] = value
}

// Count 返回指定序列的累计计数，供测试与自检使用。
func (m *Metrics) Count(name, status, errorKind string) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counters[seriesKey{name: labelValue(name), status: labelValue(status), errorKind: labelValue(errorKind)}]
}

func labels(key seriesKey) string {
	return fmt.Sprintf(`event="%s",status="%s",error_kind="%s"`, key.name, key.status, key.errorKind)
}

func sortedKeys[V any](items map[seriesKey]V) []seriesKey {
	keys := make([]seriesKey, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.name != b.name {
			return a.name < b.name
		}
		if a.status != b.status {
			return a.status < b.status
		}
		return a.errorKind < b.errorKind
	})
	return keys
}

// WriteText 以 Prometheus 文本协议导出全部指标。
func (m *Metrics) WriteText(w io.Writer) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var b strings.Builder
	b.WriteString("# HELP channel_events_total Channel runtime events by stage and outcome.\n# TYPE channel_events_total counter\n")
	for _, key := range sortedKeys(m.counters) {
		fmt.Fprintf(&b, "channel_events_total{%s} %d\n", labels(key), m.counters[key])
	}
	b.WriteString("# HELP channel_event_duration_seconds Duration attached to channel events.\n# TYPE channel_event_duration_seconds histogram\n")
	for _, key := range sortedKeys(m.durations) {
		h := m.durations[key]
		for i, bound := range durationBuckets {
			fmt.Fprintf(&b, "channel_event_duration_seconds_bucket{%s,le=\"%g\"} %d\n", labels(key), bound, h.counts[i])
		}
		fmt.Fprintf(&b, "channel_event_duration_seconds_bucket{%s,le=\"+Inf\"} %d\n", labels(key), h.total)
		fmt.Fprintf(&b, "channel_event_duration_seconds_sum{%s} %g\n", labels(key), h.sum)
		fmt.Fprintf(&b, "channel_event_duration_seconds_count{%s} %d\n", labels(key), h.total)
	}
	gaugeNames := make([]string, 0, len(m.gauges))
	for name := range m.gauges {
		gaugeNames = append(gaugeNames, name)
	}
	sort.Strings(gaugeNames)
	for _, name := range gaugeNames {
		value := m.gauges[name]
		if math.IsNaN(value) || math.IsInf(value, 0) {
			continue
		}
		fmt.Fprintf(&b, "# TYPE channel_%s gauge\nchannel_%s %g\n", name, name, value)
	}
	fmt.Fprintf(&b, "# TYPE channel_uptime_seconds gauge\nchannel_uptime_seconds %g\n", time.Since(m.started).Seconds())
	_, err := io.WriteString(w, b.String())
	return err
}

// Logger 把观测记录写为结构化日志。正常推进的高频记录用 Debug，异常与终态用 Info/Warn。
type Logger struct {
	logger *slog.Logger
}

// NewLogger 包装 slog.Logger；logger 为空时使用 slog.Default。
func NewLogger(logger *slog.Logger) *Logger {
	if logger == nil {
		logger = slog.Default()
	}
	return &Logger{logger: logger}
}

func level(item core.Observation) slog.Level {
	switch {
	case item.Status == "failed" || item.Status == "rejected" || item.Status == "error" || item.Status == "timeout":
		return slog.LevelWarn
	case item.Name == core.EventTurnSettle || item.Name == core.EventSessionRenew || item.Name == core.EventStopRequest || item.Name == core.EventCleanup || item.Name == core.EventAuth:
		return slog.LevelInfo
	case item.Name == core.EventTurnWait || item.Name == core.EventHistoryError || item.Status == "uncertain" || item.Status == "unknown":
		return slog.LevelInfo
	}
	return slog.LevelDebug
}

// Observe 实现 core.Observer。
func (l *Logger) Observe(ctx context.Context, item core.Observation) {
	lvl := level(item)
	if !l.logger.Enabled(ctx, lvl) {
		return
	}
	attrs := []slog.Attr{slog.String("event", item.Name)}
	add := func(key, value string) {
		if value != "" {
			attrs = append(attrs, slog.String(key, value))
		}
	}
	add("status", item.Status)
	add("error_kind", item.ErrorKind)
	add("binding_id", item.BindingID)
	add("turn_id", item.TurnID)
	add("delivery_id", item.DeliveryID)
	add("conversation_id", string(item.ConversationID))
	add("session_id", item.SessionID)
	if item.Duration > 0 {
		attrs = append(attrs, slog.Duration("duration", item.Duration))
	}
	if item.Count > 0 {
		attrs = append(attrs, slog.Int("count", item.Count))
	}
	l.logger.LogAttrs(ctx, lvl, "channel", attrs...)
}

// Fanout 把一条观测记录分发给多个 Observer；单个 Observer panic 不影响其他。
type Fanout []core.Observer

// Observe 实现 core.Observer。
func (f Fanout) Observe(ctx context.Context, item core.Observation) {
	for _, observer := range f {
		if observer == nil {
			continue
		}
		func() {
			defer func() { _ = recover() }()
			observer.Observe(ctx, item)
		}()
	}
}
