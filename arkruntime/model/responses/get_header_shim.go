// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Code NOT generated. Hand-written auxiliary methods that survive regeneration
// because the Makefile vendor-<api> target excludes *_shim.go files from its
// rsync --delete sweep.
//
// Purpose: satisfy the arkruntime Client pipeline's model.Response interface
// (which requires SetHeader(http.Header) and GetHeader() http.Header) on the
// ogen-generated response types we hand to Client.Do. The generated structs
// do not carry a header field, so we define thin wrapper types here that
// embed the generated type plus a header, and expose the interface methods.
//
// If this file is deleted or renamed without the _shim.go suffix, the build
// will break after the next `make vendor-responses`.
package responses

import "net/http"

// ResponseWithHeader wraps the generated Response (the body of
// POST /responses and GET /responses/{id}) so the arkruntime Client can
// populate and read the HTTP headers associated with the API call.
//
// The embedded Response's generated MarshalJSON / UnmarshalJSON are promoted
// to the wrapper, so json.Decode(wrapper) and json.Marshal(wrapper) behave
// exactly as if the value were the bare generated type.
type ResponseWithHeader struct {
	Response
	header http.Header
}

// SetHeader records the response's HTTP headers on the wrapper.
func (r *ResponseWithHeader) SetHeader(h http.Header) { r.header = h }

// GetHeader returns the HTTP headers recorded via SetHeader.
func (r *ResponseWithHeader) GetHeader() http.Header { return r.header }

// ListInputItemsResponseWithHeader wraps the generated ListInputItemsResponse.
type ListInputItemsResponseWithHeader struct {
	ListInputItemsResponse
	header http.Header
}

// SetHeader records the response's HTTP headers on the wrapper.
func (r *ListInputItemsResponseWithHeader) SetHeader(h http.Header) { r.header = h }

// GetHeader returns the HTTP headers recorded via SetHeader.
func (r *ListInputItemsResponseWithHeader) GetHeader() http.Header { return r.header }

// DeleteResponseResponseWithHeader wraps the generated DeleteResponseResponse.
type DeleteResponseResponseWithHeader struct {
	DeleteResponseResponse
	header http.Header
}

// SetHeader records the response's HTTP headers on the wrapper.
func (r *DeleteResponseResponseWithHeader) SetHeader(h http.Header) { r.header = h }

// GetHeader returns the HTTP headers recorded via SetHeader.
func (r *DeleteResponseResponseWithHeader) GetHeader() http.Header { return r.header }
