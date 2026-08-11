// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package file

import (
	"bytes"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"

	"github.com/volcengine/ark-runtime-go/arkruntime/pkg/apiform"
	"github.com/volcengine/ark-runtime-go/arkruntime/pkg/apiquery"
)

// --- model.Response wrappers ------------------------------------------------
// c.Do requires model.Response (SetHeader / GetHeader). The ogen-generated
// types don't embed model.HttpHeader, so we provide thin wrappers here that
// the client methods use internally and then unwrap for callers.

type httpHeader http.Header

func (h *httpHeader) SetHeader(header http.Header) { *h = httpHeader(header) }
func (h *httpHeader) GetHeader() http.Header       { return http.Header(*h) }

// FileObjectResponse wraps FileObject so it satisfies model.Response.
type FileObjectResponse struct {
	FileObject
	httpHeader
}

// FileListResponseWrapper wraps FileListResponse so it satisfies model.Response.
type FileListResponseWrapper struct {
	FileListResponse
	httpHeader
}

// FileDeletedResponse wraps FileDeleted so it satisfies model.Response.
type FileDeletedResponse struct {
	FileDeleted
	httpHeader
}

// --- Multipart upload helper ------------------------------------------------

// UploadForm pairs the JSON-side metadata (FileCreateRequest, generated from
// typespec/file/) with the binary file part. The ark-apis spec intentionally
// omits the `file` part; this shim adds it at multipart build time.
type UploadForm struct {
	// File is the binary content to upload. Required when Request.URL is unset.
	File io.Reader
	// Request carries the JSON-side multipart fields (purpose, expire_at, ...).
	Request FileCreateRequest
}

// MarshalMultipart writes Request as multipart form parts and appends the
// File part as the binary `file` field. Returns the encoded body and the
// matching Content-Type header (with boundary).
//
// The metadata fields (purpose, expire_at, url, tos[...], preprocess_configs[...])
// are encoded generically by apiform.Marshal, which reads the injected `form:`
// struct tags and ogen's Opt* wrappers — so adding a field to FileCreateRequest
// in the spec needs no change here. Only the binary `file` part (intentionally
// absent from the generated struct) is appended explicitly.
func (u *UploadForm) MarshalMultipart() (data []byte, contentType string, err error) {
	buf := bytes.NewBuffer(nil)
	writer := multipart.NewWriter(buf)

	if err = apiform.Marshal(&u.Request, writer); err != nil {
		_ = writer.Close()
		return nil, "", err
	}

	// Binary file part
	if u.File != nil {
		part, perr := writer.CreateFormFile("file", "file")
		if perr != nil {
			_ = writer.Close()
			return nil, "", perr
		}
		if _, err = io.Copy(part, u.File); err != nil {
			_ = writer.Close()
			return nil, "", err
		}
	}

	if err = writer.Close(); err != nil {
		return nil, "", err
	}
	return buf.Bytes(), writer.FormDataContentType(), nil
}

// URLQuery encodes the file-list query parameters as URL query values.
//
// Encoding is generic (apiquery.Marshal reads the `query:` struct tags injected
// at codegen time and ogen's Opt* wrappers), so new query fields in the spec are
// picked up without editing this function.
func URLQuery(req *FilesListParams) (url.Values, error) {
	if req == nil {
		return url.Values{}, nil
	}
	return apiquery.Marshal(req)
}
