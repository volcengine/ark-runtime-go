// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package arkruntime

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/volcengine/ark-runtime-go/arkruntime/model/contentgeneration"
	"github.com/volcengine/ark-runtime-go/arkruntime/pkg/apiquery"
)

// CreateContentGenerationTask kicks off an async content generation task.
// The returned response carries only the task id; poll GetContentGenerationTask
// (or wait for the configured callback_url) to read the final output.
func (c *Client) CreateContentGenerationTask(
	ctx context.Context,
	request *contentgeneration.CreateContentGenerationTaskRequest,
	setters ...requestOption,
) (res *contentgeneration.CreateContentGenerationTaskResponseWithHeader, err error) {
	if request == nil {
		err = errors.New("missing required request body")
		return
	}
	path := "/contents/generations/tasks"
	setters = append(setters, withBody(request))
	res = &contentgeneration.CreateContentGenerationTaskResponseWithHeader{}
	err = c.Do(ctx, http.MethodPost, c.fullURL(path), resourceTypeEndpoint, request.Model, res, setters...)
	return
}

// GetContentGenerationTask fetches the current snapshot of a previously-created
// task — including lifecycle status, the generated artifact URLs once it has
// succeeded, and any error if it has failed.
func (c *Client) GetContentGenerationTask(
	ctx context.Context,
	taskID string,
	setters ...requestOption,
) (res *contentgeneration.ContentGenerationTaskWithHeader, err error) {
	if taskID == "" {
		err = errors.New("missing required task_id parameter")
		return
	}
	path := fmt.Sprintf("/contents/generations/tasks/%s", taskID)
	res = &contentgeneration.ContentGenerationTaskWithHeader{}
	err = c.Do(ctx, http.MethodGet, c.fullURL(path), "", "", res, setters...)
	return
}

// ListContentGenerationTasksRequest is the query-side shape for
// GET /contents/generations/tasks. All fields are optional. nil pointers /
// empty slices are skipped, so a zero-valued struct lists the first page
// with no filters applied.
//
// The `query:` struct tags drive apiquery.Marshal (the same tag-driven encoder
// the file-list query uses), keeping the wire format spec-consistent. The
// `,repeat` option on TaskIDs emits bare repeated `filter.task_ids=...` pairs
// (no `[]` brackets), as the server expects.
type ListContentGenerationTasksRequest struct {
	PageNum     *int32                        `query:"page_num"`
	PageSize    *int32                        `query:"page_size"`
	Status      *contentgeneration.TaskStatus `query:"filter.status"`
	TaskIDs     []string                      `query:"filter.task_ids,repeat"`
	Model       *string                       `query:"filter.model"`
	ServiceTier *string                       `query:"filter.service_tier"`
}

// ListContentGenerationTasks returns one page of tasks matching the supplied
// filters, plus the total matching count for pagination.
func (c *Client) ListContentGenerationTasks(
	ctx context.Context,
	query *ListContentGenerationTasksRequest,
	setters ...requestOption,
) (res *contentgeneration.ListContentGenerationTasksResponseWithHeader, err error) {
	q, qerr := apiquery.Marshal(query)
	if qerr != nil {
		return nil, qerr
	}
	u := c.fullURL("/contents/generations/tasks")
	if encoded := q.Encode(); encoded != "" {
		u = u + "?" + encoded
	}
	res = &contentgeneration.ListContentGenerationTasksResponseWithHeader{}
	err = c.Do(ctx, http.MethodGet, u, "", "", res, setters...)
	return
}

// DeleteContentGenerationTask removes a task. Server returns 204 with no body.
func (c *Client) DeleteContentGenerationTask(
	ctx context.Context,
	taskID string,
	setters ...requestOption,
) (err error) {
	if taskID == "" {
		err = errors.New("missing required task_id parameter")
		return
	}
	setters = append(setters, WithCustomHeader("Accept", ""))
	path := fmt.Sprintf("/contents/generations/tasks/%s", taskID)
	err = c.Do(ctx, http.MethodDelete, c.fullURL(path), "", "", nil, setters...)
	return
}
