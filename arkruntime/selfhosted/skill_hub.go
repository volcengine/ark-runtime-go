// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package selfhosted

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const (
	skillTypeSkillHub           = "skill_hub"
	defaultSkillHubBaseURL      = "https://skills.volces.com/v1/skills"
	maxSkillHubMetadataResponse = 1 << 20
)

type skillHubListResponse struct {
	Skills []skillHubSkill `json:"Skills"`
}

type skillHubSkill struct {
	ID   string `json:"Id"`
	Slug string `json:"Slug"`
}

func (a *ClientAPI) openSkillHub(ctx context.Context, skill SkillRef) (*SkillContent, error) {
	skillID := strings.TrimSpace(skill.IDValue())
	if skillID == "" {
		return nil, errors.New("skill id is required")
	}
	version := strings.TrimSpace(skill.Version)
	if version == "" {
		return nil, errors.New("skill version is required")
	}
	slug, err := lookupSkillHubSlug(ctx, a.client.HTTPClient(), skillID)
	if err != nil {
		return nil, err
	}
	downloadURL, err := skillHubDownloadURL(slug, version)
	if err != nil {
		return nil, err
	}
	return openSignedSkillURL(ctx, a.client.HTTPClient(), downloadURL)
}

func lookupSkillHubSlug(ctx context.Context, client *http.Client, skillID string) (string, error) {
	metadataURL, err := url.Parse(defaultSkillHubBaseURL)
	if err != nil {
		return "", fmt.Errorf("parse skill hub base url: %w", err)
	}
	query := metadataURL.Query()
	query.Set("skillIds", skillID)
	metadataURL.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, metadataURL.String(), nil)
	if err != nil {
		return "", fmt.Errorf("build skill hub metadata request: %w", err)
	}
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("lookup skill hub metadata: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck // response body close errors are non-actionable
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return "", skillHubHTTPError(resp)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxSkillHubMetadataResponse+1))
	if err != nil {
		return "", fmt.Errorf("read skill hub metadata: %w", err)
	}
	if len(body) > maxSkillHubMetadataResponse {
		return "", errors.New("skill hub metadata response is too large")
	}
	var metadata skillHubListResponse
	if err := json.Unmarshal(body, &metadata); err != nil {
		return "", fmt.Errorf("decode skill hub metadata: %w", err)
	}
	for _, candidate := range metadata.Skills {
		if strings.TrimSpace(candidate.ID) != skillID {
			continue
		}
		slug := strings.Trim(strings.TrimSpace(candidate.Slug), "/")
		if slug == "" {
			return "", fmt.Errorf("skill hub slug is empty: %s", skillID)
		}
		return slug, nil
	}
	return "", &APIError{StatusCode: http.StatusNotFound, Message: "skill hub skill not found: " + skillID}
}

func skillHubDownloadURL(slug, version string) (string, error) {
	baseURL, err := url.Parse(defaultSkillHubBaseURL)
	if err != nil {
		return "", fmt.Errorf("parse skill hub base url: %w", err)
	}
	segments := strings.Split(strings.Trim(slug, "/"), "/")
	cleanSegments := make([]string, 0, len(segments))
	for _, segment := range segments {
		segment = strings.TrimSpace(segment)
		if segment == "" || segment == "." || segment == ".." {
			return "", fmt.Errorf("invalid skill hub slug: %q", slug)
		}
		cleanSegments = append(cleanSegments, segment)
	}
	baseURL.Path = strings.TrimRight(baseURL.Path, "/") + "/download/" + strings.Join(cleanSegments, "/")
	query := baseURL.Query()
	query.Set("version", version)
	baseURL.RawQuery = query.Encode()
	return baseURL.String(), nil
}

func skillHubHTTPError(resp *http.Response) error {
	message, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if len(message) == 0 {
		message = []byte(http.StatusText(resp.StatusCode))
	}
	return &APIError{
		StatusCode: resp.StatusCode,
		Message:    strings.TrimSpace(string(message)),
		RequestID:  firstNonEmpty(resp.Header.Get("X-Skill-Request-Id"), resp.Header.Get("X-Request-Id")),
	}
}
