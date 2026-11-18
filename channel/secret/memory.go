// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package secret

import (
	"errors"
	"strings"
	"sync"
)

// MemoryResolver holds application-injected in-process secrets and supports
// env/file references.
// Only adapters resolve plaintext; persisted state holds memory: references.
type MemoryResolver struct {
	mu     sync.RWMutex
	values map[string]string
}

// NewMemoryResolver creates an empty in-process secret container.
func NewMemoryResolver() *MemoryResolver { return &MemoryResolver{values: make(map[string]string)} }

// String returns a fixed redacted representation.
func (*MemoryResolver) String() string { return "[secret resolver redacted]" }

// GoString prevents Go-syntax formatting from exposing container contents.
func (*MemoryResolver) GoString() string { return "[secret resolver redacted]" }

// Set creates or replaces an in-process secret without affecting other references.
func (r *MemoryResolver) Set(ref, value string) error {
	if !strings.HasPrefix(ref, "memory:") || Validate(ref) != nil || value == "" || len(value) > maxFileBytes {
		return errInvalidRef
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.values == nil {
		r.values = make(map[string]string)
	}
	r.values[ref] = value
	return nil
}

// Delete removes an in-process secret and is idempotent.
func (r *MemoryResolver) Delete(ref string) error {
	if !strings.HasPrefix(ref, "memory:") || Validate(ref) != nil {
		return errInvalidRef
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.values, ref)
	return nil
}

// Resolve returns plaintext only on demand and excludes references and secrets
// from errors.
func (r *MemoryResolver) Resolve(ref string) (string, error) {
	if Validate(ref) != nil {
		return "", errInvalidRef
	}
	if !strings.HasPrefix(ref, "memory:") {
		return (EnvFileResolver{}).Resolve(ref)
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	value, ok := r.values[ref]
	if !ok {
		return "", errors.New("memory secret unavailable")
	}
	return value, nil
}
