// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package ma

import (
	"context"
	"time"

	"github.com/volcengine/ark-runtime-go/arkruntime"

	"github.com/volcengine/ark-runtime-go/channel"
	"github.com/volcengine/ark-runtime-go/channel/internal/handles"
)

// Options bounds MA requests, event streams, and file processing.
type Options struct {
	// ModelHeaderSource supplies BYOK headers only for events writes.
	// Returned values are neither cached nor persisted.
	ModelHeaderSource func(context.Context, string) (map[string]string, error)
	RequestTimeout    time.Duration
	StreamTimeout     time.Duration
	UploadTimeout     time.Duration
	FilePollInterval  time.Duration
	FilePollTimeout   time.Duration
}

// New reuses the supplied main SDK Client without discovering global credentials.
func New(client *arkruntime.Client, options Options) (*channel.AgentClient, error) {
	var headers ModelHeaderSource
	if options.ModelHeaderSource != nil {
		headers = func(ctx context.Context, scope RequestScope) (map[string]string, error) {
			return options.ModelHeaderSource(ctx, scope.SessionID)
		}
	}
	c, err := NewClient("https://ark.cn-beijing.volces.com/api/v3", "", ClientOptions{SDKClient: client, ModelHeaderSource: headers, RequestTimeout: options.RequestTimeout, StreamTimeout: options.StreamTimeout, UploadTimeout: options.UploadTimeout, FilePollInterval: options.FilePollInterval, FilePollTimeout: options.FilePollTimeout})
	if err != nil {
		return nil, err
	}
	return handles.NewAgent(c), nil
}
