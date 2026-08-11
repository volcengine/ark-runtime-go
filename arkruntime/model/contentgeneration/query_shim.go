// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Code NOT generated. Hand-written auxiliary helper that survives regeneration
// because the Makefile vendor-<api> target excludes *_shim.go files from its
// rsync --delete sweep.
//
// Purpose: encode the content-generation task-list query parameters as URL
// query values. Encoding is generic (apiquery.Marshal reads the `query:` struct
// tags injected at codegen time and ogen's Opt* wrappers), so new query fields
// in the spec are picked up without editing the client method. This mirrors
// file.URLQuery.
package contentgeneration

import (
	"net/url"

	"github.com/volcengine/ark-runtime-go/arkruntime/pkg/apiquery"
)

// ListURLQuery encodes ContentGenerationTasksListParams as URL query values:
// page_num / page_size / filter.status / filter.model / filter.service_tier as
// dotted scalars and filter.task_ids=... as a bare repeated array (the
// `,repeat` tag option suppresses the default `[]` brackets).
func ListURLQuery(req *ContentGenerationTasksListParams) (url.Values, error) {
	if req == nil {
		return url.Values{}, nil
	}
	return apiquery.Marshal(req)
}
