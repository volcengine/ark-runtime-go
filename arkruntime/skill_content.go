// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package arkruntime

import (
	"context"
	"errors"
	"io"
	"net/http"
	"path"
	"strings"

	"github.com/volcengine/ark-runtime-go/arkruntime/model/skill"
)

// SkillContent is a streaming Skill archive download response.
type SkillContent struct {
	Body          io.ReadCloser
	ContentLength int64
	FileName      string
	ContentType   string
}

// OpenSkillContent opens the archive content for a specific Skill version.
func (c *Client) OpenSkillContent(
	ctx context.Context,
	skillID string,
	version string,
	setters ...requestOption,
) (*SkillContent, error) {
	if skillID == "" {
		return nil, errors.New("missing required skill_id")
	}
	if version == "" {
		return nil, errors.New("missing required version")
	}
	u := c.fullURL(
		strings.Join([]string{
			skillsPrefix,
			skill.PathEscape(skillID),
			"versions",
			skill.PathEscape(version),
			"content",
		}, "/"),
	)
	req, reqErr := c.newRequest(ctx, http.MethodGet, u, "", "", append(setters, withBody(nil))...)
	if reqErr != nil {
		return nil, reqErr
	}
	resp, err := c.config.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		defer resp.Body.Close() //nolint:errcheck // response body close errors are non-actionable
		return nil, c.handleErrorResp(resp)
	}
	return &SkillContent{
		Body:          resp.Body,
		ContentLength: resp.ContentLength,
		FileName:      path.Base(resp.Request.URL.Path),
		ContentType:   resp.Header.Get("Content-Type"),
	}, nil
}

// DownloadSkillVersionContent is an alias for OpenSkillContent.
func (c *Client) DownloadSkillVersionContent(
	ctx context.Context,
	skillID string,
	version string,
	setters ...requestOption,
) (*SkillContent, error) {
	return c.OpenSkillContent(ctx, skillID, version, setters...)
}
