// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Package store provides encryption extension contracts for the built-in stores.
package store

import (
	"context"
	"errors"

	"github.com/volcengine/ark-runtime-go/channel"
	"github.com/volcengine/ark-runtime-go/channel/internal/handles"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/codec"
)

// Codec protects sensitive payloads outside persistence transactions.
type Codec = codec.Codec

// Scope identifies a record for encryption additional authenticated data.
type Scope = codec.Scope

// Envelope contains versioned ciphertext.
type Envelope = codec.Envelope

// ExpandUserAuth explicitly installs optional authorization structures; normal
// startup does not invoke it implicitly.
func ExpandUserAuth(ctx context.Context, handle *channel.Store) error {
	s, ok := handles.StoreValue(handle).(interface{ ExpandUserAuth(context.Context) error })
	if !ok {
		return errors.New("user authorization storage unavailable")
	}
	return s.ExpandUserAuth(ctx)
}
