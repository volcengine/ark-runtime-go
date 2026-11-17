// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package runner

import (
	"context"
	"errors"
	"maps"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/ma"
	"github.com/volcengine/ark-runtime-go/channel/secret"
)

func validateModelHeaderRefs(refs map[string]string) error {
	names := make(map[string]string, len(refs))
	for name, ref := range refs {
		if secret.Validate(ref) != nil {
			return errors.New("invalid model header reference")
		}
		names[name] = "reference"
	}
	return ma.ValidateModelHeaders(names)
}

func newRunnerMA(cfg MAConfig, key string, resolver secret.Resolver) (core.MAClient, error) {
	if err := validateModelHeaderRefs(cfg.ModelHeaderRefs); err != nil {
		return nil, err
	}
	var source ma.ModelHeaderSource
	if len(cfg.ModelHeaderRefs) > 0 {
		refs := maps.Clone(cfg.ModelHeaderRefs)
		source = func(ctx context.Context, _ ma.RequestScope) (map[string]string, error) {
			headers := make(map[string]string, len(refs))
			for name, ref := range refs {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				value, err := resolver.Resolve(ref)
				if err != nil {
					return nil, errors.New("model credential unavailable")
				}
				headers[name] = value
			}
			return headers, nil
		}
	}
	return ma.NewClient(cfg.BaseURL, key, ma.ClientOptions{ModelHeaderSource: source})
}
