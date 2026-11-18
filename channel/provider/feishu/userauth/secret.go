// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package userauth

import (
	"context"
	"encoding/json"
	"time"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/codec"
	"github.com/volcengine/ark-runtime-go/channel/provider/feishu/oauth"
)

const (
	phasePending       = "pending"
	phasePollIntent    = "poll_intent"
	phaseRefreshIntent = "refresh_intent"
	phaseTokens        = "tokens"
	phaseSync          = "sync"
	phaseSyncIntent    = "sync_intent"
	phaseSynced        = "synced"
	phaseUnknown       = "unknown"
)

type deviceSecret struct {
	Flow       core.AuthFlow
	DeviceCode string
	Phase      string
	Interval   time.Duration
	NextPollAt time.Time
	Tokens     oauth.Tokens
}

func (s deviceSecret) String() string   { return "DeviceSecret{redacted}" }
func (s deviceSecret) GoString() string { return s.String() }
func uncertainPhase(phase string) bool {
	return phase == phasePollIntent || phase == phaseRefreshIntent || phase == phaseSyncIntent || phase == phaseUnknown
}
func secretScope(r core.UserAuthRecord) codec.Scope {
	return codec.Scope{Owner: r.Principal.Owner, Project: r.Principal.Project, Namespace: "user_auth_device_v2", RecordKind: "user_auth_device", RecordID: r.Principal.Key() + ":" + r.FlowID, Field: "device_state", PayloadVersion: 1, LogicalSchema: 1}
}
func (a *Authorizer) decrypt(ctx context.Context, r core.UserAuthRecord) (deviceSecret, error) {
	if r.Secret == nil || r.FlowID == "" {
		return deviceSecret{}, errState
	}
	var envelope codec.Envelope
	if json.Unmarshal(r.Secret.Ciphertext, &envelope) != nil || r.Secret.KeyID != envelope.KeyID || r.Secret.Version != envelope.Version {
		return deviceSecret{}, errState
	}
	plaintext, err := a.config.Codec.Open(ctx, secretScope(r), envelope)
	if err != nil {
		return deviceSecret{}, errState
	}
	var secret deviceSecret
	if json.Unmarshal(plaintext, &secret) != nil || secret.Flow.ID != r.FlowID || !secret.Flow.ExpiresAt.Equal(r.ExpiresAt) {
		return deviceSecret{}, errState
	}
	return secret, nil
}
func (a *Authorizer) checkpoint(r core.UserAuthRecord, secret deviceSecret, state, id string) (core.UserAuthRecord, error) {
	// HTTP 已结束后的证据提交不使用已取消的调用 context；仍必须通过原 lease/revision。
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	plaintext, err := json.Marshal(secret)
	if err != nil {
		return r, errState
	}
	envelope, err := a.config.Codec.Seal(ctx, secretScope(r), plaintext)
	if err != nil {
		return r, errState
	}
	wrapped, err := json.Marshal(envelope)
	if err != nil {
		return r, errState
	}
	deadline := r.ExpiresAt
	if deadline.IsZero() || secret.Flow.ExpiresAt.Before(deadline) {
		deadline = secret.Flow.ExpiresAt
	}
	next, err := a.config.Store.UpdateUserAuth(ctx, core.UserAuthUpdate{Principal: r.Principal, Lease: r.Lease, Revision: r.Revision, State: state, FlowID: r.FlowID, CredentialID: id, ExpiresAt: deadline, Secret: &core.UserAuthSecret{Version: envelope.Version, KeyID: envelope.KeyID, Ciphertext: wrapped}})
	if err != nil {
		return r, errState
	}
	return next, nil
}
