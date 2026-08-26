// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package arkruntime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/volcengine/ark-runtime-go/arkruntime/model/skill"
)

const skillsPrefix = "/skills"

// CreateSkillOptions controls optional multipart metadata for CreateSkill.
type CreateSkillOptions struct {
	DisplayTitle      string
	ProtectionEnabled *bool
}

// CreateSkill uploads a zip package as multipart/form-data and creates a Skill.
// `fileReader` supplies the zip bytes; `displayTitle` is optional.
func (c *Client) CreateSkill(
	ctx context.Context,
	fileReader io.Reader,
	fileName, displayTitle string,
	setters ...requestOption,
) (*skill.Skill, error) {
	return c.CreateSkillWithOptions(ctx, fileReader, fileName, CreateSkillOptions{
		DisplayTitle: displayTitle,
	}, setters...)
}

// CreateSkillWithOptions uploads a zip package with optional Skill metadata.
func (c *Client) CreateSkillWithOptions(
	ctx context.Context,
	fileReader io.Reader,
	fileName string,
	options CreateSkillOptions,
	setters ...requestOption,
) (*skill.Skill, error) {
	if fileReader == nil {
		return nil, errors.New("missing required file reader")
	}
	form := &skill.UploadForm{
		File:              fileReader,
		FileName:          fileName,
		DisplayTitle:      options.DisplayTitle,
		ProtectionEnabled: options.ProtectionEnabled,
	}
	body, contentType, merr := form.MarshalMultipart()
	if merr != nil {
		return nil, merr
	}
	opts := append(setters,
		withBody(bytes.NewReader(body)),
		withContentType(contentType),
	)
	wrap := &skill.SkillResponse{}
	if err := c.Do(ctx, http.MethodPost, c.fullURL(skillsPrefix), "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.Skill, nil
}

// GetSkill retrieves a Skill summary by ID.
func (c *Client) GetSkill(
	ctx context.Context,
	skillID string,
	setters ...requestOption,
) (*skill.Skill, error) {
	if skillID == "" {
		return nil, errors.New("missing required skill_id")
	}
	u := c.fullURL(fmt.Sprintf("%s/%s", skillsPrefix, skill.PathEscape(skillID)))
	opts := append(setters, withBody(nil))
	wrap := &skill.SkillResponse{}
	if err := c.Do(ctx, http.MethodGet, u, "", "", wrap, opts...); err != nil {
		return nil, err
	}
	return &wrap.Skill, nil
}
