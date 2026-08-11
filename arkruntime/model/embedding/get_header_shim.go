// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Code NOT generated. Hand-written auxiliary methods that survive regeneration
// because the Makefile vendor-<api> target excludes *_shim.go files from its
// rsync --delete sweep.
//
// Purpose: satisfy the arkruntime Client pipeline's model.Response interface
// (which requires SetHeader(http.Header) and GetHeader() http.Header) on the
// ogen-generated response types we hand to Client.Do. The generated structs
// do not carry a header field, so we define a thin wrapper type here that
// embeds the generated type plus a header, and expose the interface methods.
//
// If this file is deleted or renamed without the _shim.go suffix, the build
// will break after the next `make vendor-embedding`.
package embedding

import "net/http"

// EmbeddingResponseWithHeader wraps the generated EmbeddingResponse (the body
// of POST /embeddings) so the arkruntime Client can populate and read the
// HTTP headers associated with the API call.
//
// The embedded EmbeddingResponse's generated MarshalJSON / UnmarshalJSON are
// promoted to the wrapper, so json.Decode(wrapper) and json.Marshal(wrapper)
// behave exactly as if the value were the bare generated type.
type EmbeddingResponseWithHeader struct {
	EmbeddingResponse
	header http.Header
}

// SetHeader records the response's HTTP headers on the wrapper.
func (r *EmbeddingResponseWithHeader) SetHeader(h http.Header) { r.header = h }

// GetHeader returns the HTTP headers recorded via SetHeader.
func (r *EmbeddingResponseWithHeader) GetHeader() http.Header { return r.header }
