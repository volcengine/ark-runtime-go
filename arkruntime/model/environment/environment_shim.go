// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package environment

import (
	"net/http"
	"net/url"

	"github.com/volcengine/ark-runtime-go/arkruntime/model"
	"github.com/volcengine/ark-runtime-go/arkruntime/pkg/apiquery"
)

// --- model.Response wrappers ------------------------------------------------
// c.Do requires model.Response (SetHeader / GetHeader). Ogen-generated types
// don't embed model.HttpHeader, so we provide thin wrappers used by the client
// wrappers and unwrapped for callers.

// EnvironmentResponse wraps Environment so it satisfies model.Response.
type EnvironmentResponse struct {
	Environment
	model.HttpHeader
}

// ListEnvironmentsResponseWrapper wraps ListEnvironmentsResponse.
type ListEnvironmentsResponseWrapper struct {
	ListEnvironmentsResponse
	model.HttpHeader
}

// DeleteEnvironmentResponseWrapper wraps DeleteEnvironmentResponse.
type DeleteEnvironmentResponseWrapper struct {
	DeleteEnvironmentResponse
	model.HttpHeader
}

// URLQuery encodes list-query params to url.Values via `query:` struct tags
// injected by ogen. Add a field to EnvironmentsListParams in typespec and it
// flows through here without shim edits.
func URLQuery(req *EnvironmentsListParams) (url.Values, error) {
	if req == nil {
		return url.Values{}, nil
	}
	return apiquery.Marshal(req)
}

// PathEscape hides url.PathEscape at the shim boundary so wrapper files stay
// import-lean. Kept here for symmetry with other resources whose IDs may
// contain slashes / colons.
func PathEscape(s string) string { return url.PathEscape(s) }

var _ = http.NoBody // keep net/http import when future SSE helpers land here
