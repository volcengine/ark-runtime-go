// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRouteKeyIsolation(t *testing.T) {
	base := Message{BindingID: "b", TenantKey: "tenant", ChatID: "chat", ChatType: "group"}
	cases := []struct {
		name string
		edit func(*Message)
	}{
		{"binding", func(m *Message) { m.BindingID = "other" }},
		{"tenant", func(m *Message) { m.TenantKey = "other" }},
		{"chat", func(m *Message) { m.ChatID = "other" }},
		{"kind", func(m *Message) { m.ChatType = "p2p" }},
		{"thread", func(m *Message) { m.ThreadID = "thread" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			changed := base
			tc.edit(&changed)
			require.NotEqual(t, RouteFor(base), RouteFor(changed))
		})
	}
}
