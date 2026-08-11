// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"net/url"

	"github.com/volcengine/ark-runtime-go/arkruntime/model"
	"github.com/volcengine/ark-runtime-go/arkruntime/pkg/apiquery"
)

// --- MemoryStore ------------------------------------------------------------

type MemoryStoreResponse struct {
	MemoryStore
	model.HttpHeader
}

type ListMemoryStoresResponseWrapper struct {
	ListMemoryStoresResponse
	model.HttpHeader
}

type DeleteMemoryStoreResponseWrapper struct {
	DeleteMemoryStoreResponse
	model.HttpHeader
}

// --- Memory -----------------------------------------------------------------

type MemoryResponse struct {
	Memory
	model.HttpHeader
}

type ListMemoriesResponseWrapper struct {
	ListMemoriesResponse
	model.HttpHeader
}

type DeleteMemoryResponseWrapper struct {
	DeleteMemoryResponse
	model.HttpHeader
}

// --- URL helpers ------------------------------------------------------------

func URLQueryStoresList(req *MemoryStoresListParams) (url.Values, error) {
	if req == nil {
		return url.Values{}, nil
	}
	return apiquery.Marshal(req)
}

func URLQueryMemoriesList(req *MemoriesListParams) (url.Values, error) {
	if req == nil {
		return url.Values{}, nil
	}
	return apiquery.Marshal(req)
}

func PathEscape(s string) string { return url.PathEscape(s) }
