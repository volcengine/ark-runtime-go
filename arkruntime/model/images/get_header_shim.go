// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Code NOT generated. Hand-written wrappers that survive regeneration because
// the Makefile vendor-<api> target excludes *_shim.go files from its
// rsync --delete sweep.
//
// Purpose: satisfy the arkruntime Client pipeline's model.Response interface
// (which requires SetHeader(http.Header) and GetHeader() http.Header) on the
// ogen-generated response types we hand to Client.Do. The generated structs
// do not carry a header field, so we define a thin wrapper type here that
// embeds the generated type plus a header.
package images

import "net/http"

// ImageGenerationResponseWithHeader wraps the generated ImageGenerationResponse
// so the arkruntime Client can populate and read the HTTP headers associated
// with the API call.
type ImageGenerationResponseWithHeader struct {
	ImageGenerationResponse
	header http.Header
}

func (r *ImageGenerationResponseWithHeader) SetHeader(h http.Header) { r.header = h }
func (r *ImageGenerationResponseWithHeader) GetHeader() http.Header  { return r.header }
