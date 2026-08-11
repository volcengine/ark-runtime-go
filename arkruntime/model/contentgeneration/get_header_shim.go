// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Code NOT generated. Hand-written wrappers that survive regeneration because
// the Makefile vendor-<api> target excludes *_shim.go files from its
// rsync --delete sweep.
//
// Purpose: satisfy the arkruntime Client pipeline's model.Response interface
// (which requires SetHeader(http.Header) and GetHeader() http.Header) on the
// ogen-generated response types we hand to Client.Do. The generated structs
// do not carry a header field, so we define thin wrapper types here that
// embed the generated type plus a header.
package contentgeneration

import "net/http"

// CreateContentGenerationTaskResponseWithHeader wraps the generated
// CreateContentGenerationTaskResponse so the arkruntime Client can populate
// and read the HTTP headers associated with the API call.
type CreateContentGenerationTaskResponseWithHeader struct {
	CreateContentGenerationTaskResponse
	header http.Header
}

func (r *CreateContentGenerationTaskResponseWithHeader) SetHeader(h http.Header) { r.header = h }
func (r *CreateContentGenerationTaskResponseWithHeader) GetHeader() http.Header  { return r.header }

// ContentGenerationTaskWithHeader wraps the task snapshot returned by
// GET /contents/generations/tasks/{id}.
type ContentGenerationTaskWithHeader struct {
	ContentGenerationTask
	header http.Header
}

func (r *ContentGenerationTaskWithHeader) SetHeader(h http.Header) { r.header = h }
func (r *ContentGenerationTaskWithHeader) GetHeader() http.Header  { return r.header }

// ListContentGenerationTasksResponseWithHeader wraps the paged list response
// returned by GET /contents/generations/tasks.
type ListContentGenerationTasksResponseWithHeader struct {
	ListContentGenerationTasksResponse
	header http.Header
}

func (r *ListContentGenerationTasksResponseWithHeader) SetHeader(h http.Header) { r.header = h }
func (r *ListContentGenerationTasksResponseWithHeader) GetHeader() http.Header  { return r.header }
