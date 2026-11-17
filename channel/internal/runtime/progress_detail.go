// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"encoding/json"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

const maxToolDetailBytes = 12000

// flushToolProgress 在 history 终态结算前补齐工具明细，正文仍只在持有 Turn 租约时发送。
func (r *Runtime) flushToolProgress(j *job, events []Event) {
	if j.binding.ProgressProtocol != "native_cot" || j.binding.ProgressMode == "status_only" || j.binding.ProgressMode == "" && !j.binding.ShowProgress {
		return
	}
	steps := progressFromEvents(events, j)
	if len(steps) == 0 {
		return
	}
	r.reportNativeProgress(j, ProgressUpdate{TurnID: j.ID, SessionID: j.Session.SessionID, ReplyTarget: ReplyTargetFor(j.msg, j.binding), State: ProgressRunning, Steps: steps, At: r.options.Clock(), Protocol: "native_cot"}, true)
}

var (
	toolDetailSecretKey  = regexp.MustCompile(`(?i)(?:api[_-]?key|access[_-]?key(?:[_-]?id)?|token|secret|password|passwd|authorization|private[_-]?key)$`)
	toolDetailAssignment = regexp.MustCompile(`(?i)(\b[\w.-]*(?:api[_-]?key|access[_-]?key(?:[_-]?id)?|token|secret|password|passwd|authorization|private[_-]?key)\b[ \t]*[:=][ \t]*)("(?:\\.|[^"\\])*"|'[^']*'|[^\s,;]+)`)
	toolDetailFlag       = regexp.MustCompile(`(?i)(--[\w-]*(?:api[_-]?key|token|secret|password|passwd|private[_-]?key)[ \t]+)("(?:\\.|[^"\\])*"|'[^']*'|[^\s,;]+)`)
	toolDetailBearer     = regexp.MustCompile(`(?i)(\b(?:bearer|basic)[ \t]+)([a-z0-9._~+/=-]+)`)
	toolDetailURL        = regexp.MustCompile(`(?i)(https?://)([^/\s:@]+:[^@\s/]+)@`)
	toolDetailPrivateKey = regexp.MustCompile(`(?s)-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----`)
	toolDetailANSI       = regexp.MustCompile("\x1b\\[[0-?]*[ -/]*[@-~]")
)

// toolDetailRedactor 只在本轮投影期间保存已识别的凭证，以过滤同一工具输出中的回显。
type toolDetailRedactor struct{ secrets []string }

func (r *toolDetailRedactor) hide(value string) string {
	if len(value) >= 4 {
		r.secrets = append(r.secrets, strings.Trim(value, `"'`))
	}
	return "[REDACTED]"
}

func (r *toolDetailRedactor) text(value string) string {
	value = toolDetailPrivateKey.ReplaceAllStringFunc(value, r.hide)
	for _, pattern := range []*regexp.Regexp{toolDetailBearer, toolDetailURL, toolDetailAssignment, toolDetailFlag} {
		value = pattern.ReplaceAllStringFunc(value, func(match string) string {
			parts := pattern.FindStringSubmatch(match)
			masked := parts[1] + r.hide(parts[2])
			if pattern == toolDetailURL {
				masked += "@"
			}
			return masked
		})
	}
	for _, secret := range r.secrets {
		if secret != "" {
			value = strings.ReplaceAll(value, secret, "[REDACTED]")
		}
	}
	value = toolDetailANSI.ReplaceAllString(value, "")
	value = strings.ReplaceAll(value, "\r\n", "\n")
	return strings.Map(func(ch rune) rune {
		if ch == '\r' {
			return '\n'
		}
		if unicode.IsControl(ch) && ch != '\n' && ch != '\t' {
			return -1
		}
		return ch
	}, value)
}

func (r *toolDetailRedactor) structured(value any, depth int) any {
	if depth > 20 {
		return "…（已截断）"
	}
	switch value := value.(type) {
	case map[string]any:
		for key, item := range value {
			if toolDetailSecretKey.MatchString(key) {
				value[key] = "[REDACTED]"
			} else {
				value[key] = r.structured(item, depth+1)
			}
		}
	case []any:
		for i, item := range value {
			value[i] = r.structured(item, depth+1)
		}
	case string:
		return r.text(value)
	}
	return value
}

// collectSecrets 先收集各层敏感字段，再清理普通字符串中的回显，避免依赖 map 遍历顺序。
func (r *toolDetailRedactor) collectSecrets(value any, depth int) {
	if depth > 20 {
		return
	}
	switch value := value.(type) {
	case map[string]any:
		for key, item := range value {
			if text, ok := item.(string); ok && toolDetailSecretKey.MatchString(key) {
				r.hide(text)
			}
			r.collectSecrets(item, depth+1)
		}
	case []any:
		for _, item := range value {
			r.collectSecrets(item, depth+1)
		}
	}
}

func (r *toolDetailRedactor) preview(raw string) string {
	text := raw
	structured := false
	if json.Valid([]byte(raw)) {
		decoder := json.NewDecoder(strings.NewReader(raw))
		decoder.UseNumber()
		var value any
		if decoder.Decode(&value) == nil {
			r.collectSecrets(value, 0)
			encoded, err := json.Marshal(r.structured(value, 0))
			if err == nil {
				text = string(encoded)
				structured = true
			}
		}
	}
	if !structured {
		text = r.text(text)
	}
	if len(text) > maxToolDetailBytes {
		const suffix = "\n…（已截断）"
		end := maxToolDetailBytes - len(suffix)
		for end > 0 && !utf8.RuneStart(text[end]) {
			end--
		}
		text = text[:end] + suffix
	}
	return text
}
