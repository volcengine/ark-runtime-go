// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Code NOT generated. Hand-written auxiliary helper that survives regeneration
// because the Makefile vendor-<api> target excludes *_shim.go files from its
// rsync --delete sweep.
//
// Purpose: encode the responses list/get query parameters as URL query values.
// Encoding is generic (apiquery.Marshal reads the `query:` struct tags injected
// at codegen time and ogen's Opt* wrappers), so new query fields in the spec are
// picked up without editing the client methods. This mirrors file.URLQuery.
package responses

import (
	"net/url"

	"github.com/volcengine/ark-runtime-go/arkruntime/pkg/apiquery"
)

// ListInputItemsURLQuery encodes ResponsesListInputItemsParams as URL query
// values: after / before / limit / order as scalars and include[]=... as a
// bracketed repeated array. The ResponseId path field carries no `query:` tag
// and is skipped.
func ListInputItemsURLQuery(req *ResponsesListInputItemsParams) (url.Values, error) {
	if req == nil {
		return url.Values{}, nil
	}
	return apiquery.Marshal(req)
}

// RetrieveURLQuery encodes ResponsesRetrieveParams as URL query values:
// include[]=... as a bracketed repeated array. The ResponseId path field
// carries no `query:` tag and is skipped.
func RetrieveURLQuery(req *ResponsesRetrieveParams) (url.Values, error) {
	if req == nil {
		return url.Values{}, nil
	}
	return apiquery.Marshal(req)
}
