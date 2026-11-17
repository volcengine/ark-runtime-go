// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package ma

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	sdkmodel "github.com/volcengine/ark-runtime-go/arkruntime/model"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

const maxSessionFilePages = 100

var (
	_ core.SessionOutputLister   = (*Client)(nil)
	_ core.SessionFileDownloader = (*Client)(nil)
)

func (c *Client) forSession(sessionID string) (*Client, error) {
	if _, err := sessionPath(sessionID); err != nil || sessionID == "*" {
		return nil, errors.New("ma file session invalid")
	}
	bound := *c
	bound.sessionScope = &RequestScope{Kind: ScopeSession, SessionID: sessionID}
	return &bound, nil
}

// ListSessionOutputFiles 分页读取本 Session 已就绪的 outputs 产物；路径取自 description。
func (c *Client) ListSessionOutputFiles(ctx context.Context, sessionID string) ([]core.SessionOutputFile, error) {
	client, err := c.forSession(sessionID)
	if err != nil {
		return nil, err
	}
	query := url.Values{"scope_id": {sessionID}, "purpose": {"agent"}, "limit": {"100"}, "order": {"asc"}}
	seen := make(map[string]bool)
	cursors := make(map[string]bool)
	var result []core.SessionOutputFile
	for range maxSessionFilePages {
		page, err := requestSDK(client, ctx, http.MethodGet, "/files", sdkCallOptions{}, func(ctx context.Context, sdk *arkruntime.Client, options []arkruntime.RequestOption) (*sessionFilesResponse, error) {
			result := &sessionFilesResponse{}
			err := sdk.Do(ctx, http.MethodGet, sdk.BaseURL()+"/files?"+query.Encode(), "", "", result, options...)
			return result, err
		})
		if err != nil {
			return nil, err
		}
		for _, file := range page.Data {
			if file.Scope.ID != sessionID || file.Scope.Type != "session" || file.Status != "active" || !strings.HasPrefix(file.Description, "/mnt/session/outputs/") || path.Clean(file.Description) != file.Description || strings.HasSuffix(file.Description, "/") {
				continue
			}
			if _, err := filePath(file.ID); err != nil || seen[file.ID] {
				continue
			}
			seen[file.ID] = true
			result = append(result, core.SessionOutputFile{FileID: file.ID, Path: file.Description, FileName: file.FileName, MimeType: file.MimeType, Bytes: file.Bytes})
		}
		if !page.HasMore {
			return result, nil
		}
		if page.LastID == "" || cursors[page.LastID] {
			return nil, errors.New("ma session file cursor invalid")
		}
		cursors[page.LastID] = true
		query.Set("after", page.LastID)
	}
	return nil, errors.New("ma session file page limit")
}

// DownloadSessionFile 以 Session 凭据获取文件元数据，再无凭据下载预签名对象。
func (c *Client) DownloadSessionFile(ctx context.Context, sessionID, fileID string, maxBytes int64) (core.FileContent, error) {
	client, err := c.forSession(sessionID)
	if err != nil {
		return core.FileContent{}, err
	}
	return client.DownloadFile(ctx, fileID, maxBytes)
}

// ResolveSessionFileURL 为超限文件获取与原 Session 绑定的临时下载地址。
func (c *Client) ResolveSessionFileURL(ctx context.Context, sessionID, fileID string) (string, error) {
	client, err := c.forSession(sessionID)
	if err != nil {
		return "", err
	}
	return client.ResolveFileURL(ctx, fileID)
}

type sessionFilesResponse struct {
	Data    []wireFileMeta `json:"data"`
	HasMore bool           `json:"has_more"`
	LastID  string         `json:"last_id"`
	sdkmodel.HttpHeader
}
