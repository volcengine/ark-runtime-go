// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package arkruntime

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/volcengine/ark-runtime-go/arkruntime/model/memory"
)

const memoryStoresPrefix = "/memory_stores"

// ---- MemoryStore CRUD -----------------------------------------------------

// CreateMemoryStore creates a new MemoryStore.
func (c *Client) CreateMemoryStore(
	ctx context.Context,
	body *memory.CreateMemoryStoreRequest,
	setters ...requestOption,
) (*memory.MemoryStore, error) {
	if body == nil {
		return nil, errors.New("missing required request body")
	}
	opts := append(setters, withBody(body))
	wrap := &memory.MemoryStoreResponse{}
	if err := c.Do(ctx, http.MethodPost, c.fullURL(memoryStoresPrefix), "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.MemoryStore, nil
}

// GetMemoryStore retrieves a MemoryStore by ID.
func (c *Client) GetMemoryStore(
	ctx context.Context,
	memoryStoreID string,
	setters ...requestOption,
) (*memory.MemoryStore, error) {
	if memoryStoreID == "" {
		return nil, errors.New("missing required memory_store_id")
	}
	u := c.fullURL(fmt.Sprintf("%s/%s", memoryStoresPrefix, memory.PathEscape(memoryStoreID)))
	opts := append(setters, withBody(nil))
	wrap := &memory.MemoryStoreResponse{}
	if err := c.Do(ctx, http.MethodGet, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.MemoryStore, nil
}

// ListMemoryStores lists MemoryStores.
func (c *Client) ListMemoryStores(
	ctx context.Context,
	params *memory.MemoryStoresListParams,
	setters ...requestOption,
) (*memory.ListMemoryStoresResponse, error) {
	q, qerr := memory.URLQueryStoresList(params)
	if qerr != nil {
		return nil, qerr
	}
	u := c.fullURL(memoryStoresPrefix)
	if encoded := q.Encode(); encoded != "" {
		u = u + "?" + encoded
	}
	opts := append(setters, withBody(nil))
	wrap := &memory.ListMemoryStoresResponseWrapper{}
	if err := c.Do(ctx, http.MethodGet, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.ListMemoryStoresResponse, nil
}

// UpdateMemoryStore updates a MemoryStore.
func (c *Client) UpdateMemoryStore(
	ctx context.Context,
	memoryStoreID string,
	body *memory.UpdateMemoryStoreRequest,
	setters ...requestOption,
) (*memory.MemoryStore, error) {
	if memoryStoreID == "" {
		return nil, errors.New("missing required memory_store_id")
	}
	if body == nil {
		return nil, errors.New("missing required request body")
	}
	u := c.fullURL(fmt.Sprintf("%s/%s", memoryStoresPrefix, memory.PathEscape(memoryStoreID)))
	opts := append(setters, withBody(body))
	wrap := &memory.MemoryStoreResponse{}
	if err := c.Do(ctx, http.MethodPost, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.MemoryStore, nil
}

// DeleteMemoryStore deletes a MemoryStore (cascades to all memories).
func (c *Client) DeleteMemoryStore(
	ctx context.Context,
	memoryStoreID string,
	setters ...requestOption,
) (*memory.DeleteMemoryStoreResponse, error) {
	if memoryStoreID == "" {
		return nil, errors.New("missing required memory_store_id")
	}
	u := c.fullURL(fmt.Sprintf("%s/%s", memoryStoresPrefix, memory.PathEscape(memoryStoreID)))
	opts := append(setters, withBody(nil))
	wrap := &memory.DeleteMemoryStoreResponseWrapper{}
	if err := c.Do(ctx, http.MethodDelete, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.DeleteMemoryStoreResponse, nil
}

// ---- Memory CRUD (nested under MemoryStore) --------------------------------

func memoriesPrefix(memoryStoreID string) string {
	return fmt.Sprintf("%s/%s/memories", memoryStoresPrefix, memory.PathEscape(memoryStoreID))
}

// CreateMemory creates a new Memory in the given MemoryStore.
func (c *Client) CreateMemory(
	ctx context.Context,
	memoryStoreID string,
	body *memory.CreateMemoryRequest,
	setters ...requestOption,
) (*memory.Memory, error) {
	if memoryStoreID == "" {
		return nil, errors.New("missing required memory_store_id")
	}
	if body == nil {
		return nil, errors.New("missing required request body")
	}
	opts := append(setters, withBody(body))
	wrap := &memory.MemoryResponse{}
	if err := c.Do(ctx, http.MethodPost, c.fullURL(memoriesPrefix(memoryStoreID)), "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.Memory, nil
}

// GetMemory retrieves a single Memory (with content).
func (c *Client) GetMemory(
	ctx context.Context,
	memoryStoreID, memoryID string,
	setters ...requestOption,
) (*memory.Memory, error) {
	if memoryStoreID == "" || memoryID == "" {
		return nil, errors.New("missing required memory_store_id / memory_id")
	}
	u := c.fullURL(fmt.Sprintf("%s/%s", memoriesPrefix(memoryStoreID), memory.PathEscape(memoryID)))
	opts := append(setters, withBody(nil))
	wrap := &memory.MemoryResponse{}
	if err := c.Do(ctx, http.MethodGet, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.Memory, nil
}

// ListMemories lists Memories under a MemoryStore with optional path/depth filter.
func (c *Client) ListMemories(
	ctx context.Context,
	memoryStoreID string,
	params *memory.MemoriesListParams,
	setters ...requestOption,
) (*memory.ListMemoriesResponse, error) {
	if memoryStoreID == "" {
		return nil, errors.New("missing required memory_store_id")
	}
	q, qerr := memory.URLQueryMemoriesList(params)
	if qerr != nil {
		return nil, qerr
	}
	u := c.fullURL(memoriesPrefix(memoryStoreID))
	if encoded := q.Encode(); encoded != "" {
		u = u + "?" + encoded
	}
	opts := append(setters, withBody(nil))
	wrap := &memory.ListMemoriesResponseWrapper{}
	if err := c.Do(ctx, http.MethodGet, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.ListMemoriesResponse, nil
}

// UpdateMemory updates a Memory's content and/or path.
func (c *Client) UpdateMemory(
	ctx context.Context,
	memoryStoreID, memoryID string,
	body *memory.UpdateMemoryRequest,
	setters ...requestOption,
) (*memory.Memory, error) {
	if memoryStoreID == "" || memoryID == "" {
		return nil, errors.New("missing required memory_store_id / memory_id")
	}
	if body == nil {
		return nil, errors.New("missing required request body")
	}
	u := c.fullURL(fmt.Sprintf("%s/%s", memoriesPrefix(memoryStoreID), memory.PathEscape(memoryID)))
	opts := append(setters, withBody(body))
	wrap := &memory.MemoryResponse{}
	if err := c.Do(ctx, http.MethodPost, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.Memory, nil
}

// DeleteMemory deletes a Memory (path is released; internal history retained).
func (c *Client) DeleteMemory(
	ctx context.Context,
	memoryStoreID, memoryID string,
	setters ...requestOption,
) (*memory.DeleteMemoryResponse, error) {
	if memoryStoreID == "" || memoryID == "" {
		return nil, errors.New("missing required memory_store_id / memory_id")
	}
	u := c.fullURL(fmt.Sprintf("%s/%s", memoriesPrefix(memoryStoreID), memory.PathEscape(memoryID)))
	opts := append(setters, withBody(nil))
	wrap := &memory.DeleteMemoryResponseWrapper{}
	if err := c.Do(ctx, http.MethodDelete, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.DeleteMemoryResponse, nil
}
