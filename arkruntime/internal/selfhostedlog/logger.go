// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Package selfhostedlog 为 self-hosted worker 提供兼容 Go 1.20 的结构化日志适配。
package selfhostedlog

import (
	"fmt"
	"log"
	"strconv"
	"strings"
)

// Logger 在标准库 log.Logger 上保留 self-hosted worker 使用的键值日志接口。
type Logger struct {
	base  *log.Logger
	attrs []any
}

// New 创建日志适配器，base 为空时使用 log.Default。
func New(base *log.Logger) *Logger {
	if base == nil {
		base = log.Default()
	}
	return &Logger{base: base}
}

// With 返回附带固定字段的新日志适配器。
func (l *Logger) With(args ...any) *Logger {
	if l == nil {
		l = New(nil)
	}
	attrs := make([]any, 0, len(l.attrs)+len(args))
	attrs = append(attrs, l.attrs...)
	attrs = append(attrs, args...)
	return &Logger{base: l.base, attrs: attrs}
}

// Debug 记录调试日志。
func (l *Logger) Debug(message string, args ...any) { l.output("DEBUG", message, args...) }

// Info 记录信息日志。
func (l *Logger) Info(message string, args ...any) { l.output("INFO", message, args...) }

// Warn 记录警告日志。
func (l *Logger) Warn(message string, args ...any) { l.output("WARN", message, args...) }

// Error 记录错误日志。
func (l *Logger) Error(message string, args ...any) { l.output("ERROR", message, args...) }

func (l *Logger) output(level, message string, args ...any) {
	if l == nil {
		l = New(nil)
	}
	all := make([]any, 0, len(l.attrs)+len(args))
	all = append(all, l.attrs...)
	all = append(all, args...)
	l.base.Printf("level=%s msg=%s%s", level, quoteValue(message), formatAttrs(all))
}

func formatAttrs(attrs []any) string {
	if len(attrs) == 0 {
		return ""
	}
	var builder strings.Builder
	for index := 0; index < len(attrs); index += 2 {
		key := fmt.Sprint(attrs[index])
		value := any("<missing>")
		if index+1 < len(attrs) {
			value = attrs[index+1]
		}
		builder.WriteByte(' ')
		builder.WriteString(key)
		builder.WriteByte('=')
		builder.WriteString(quoteValue(value))
	}
	return builder.String()
}

func quoteValue(value any) string {
	text := fmt.Sprint(value)
	if text == "" || strings.ContainsAny(text, " \t\r\n\"=") {
		return strconv.Quote(text)
	}
	return text
}
