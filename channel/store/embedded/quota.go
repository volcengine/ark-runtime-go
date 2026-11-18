// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package embedded

import (
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

var bucketQuotas = []byte("account_quotas")

type accountQuota struct {
	Remote map[string]core.RemoteKind `json:"remote,omitempty"`
	Held   map[string]bool            `json:"held"`
}

func (s *store) quota(tx storeTx, turn core.Turn) (string, *accountQuota, error) {
	v, err := getJSON[core.BindingVersion](tx, bucketVersions, key([]byte(turn.BindingID), []byte(turn.BindingVersion)))
	if err != nil {
		return "", nil, err
	}
	if v == nil {
		return "", nil, core.ErrConflict
	}
	owner := v.Content.Owner
	if owner == "" {
		return "", nil, nil
	}
	q, err := getJSON[accountQuota](tx, bucketQuotas, []byte(owner))
	if q == nil {
		q = &accountQuota{Held: map[string]bool{}}
	}
	return owner, q, err
}
