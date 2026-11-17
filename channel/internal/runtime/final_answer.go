// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import "strings"

// selectFinalAnswer 只选择已完成本轮边界过滤后的最后一条非空消息，不合并过程说明。
func selectFinalAnswer(events []Event) (string, bool) {
	for i := len(events) - 1; i >= 0; i-- {
		text, ok := messageText(events[i])
		if !ok {
			return "", false
		}
		if strings.TrimSpace(text) != "" {
			return text, true
		}
	}
	return "", true
}
