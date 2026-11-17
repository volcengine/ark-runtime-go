// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package ma

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	sdkmodel "github.com/volcengine/ark-runtime-go/arkruntime/model"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
)

var (
	_ core.FileDownloader   = (*Client)(nil)
	_ core.FileURLResolver  = (*Client)(nil)
	_ core.OutputDownloader = (*Client)(nil)
)

// presignedTTLSeconds 是请求预签名下载地址的有效期；服务端下限为 1 天。
const presignedTTLSeconds = "86400"

type wireFileMeta struct {
	ID          string `json:"id"`
	FileName    string `json:"filename"`
	Bytes       int64  `json:"bytes"`
	MimeType    string `json:"mime_type"`
	Status      string `json:"status"`
	DownloadURL string `json:"download_url"`
	Description string `json:"description"`
	Scope       struct {
		ID   string `json:"id"`
		Type string `json:"type"`
	} `json:"scope"`
}

// fileDownloadMeta 用宿主凭据读取可下载文件的元数据，不在错误信息中包含下载地址。
func (c *Client) fileDownloadMeta(ctx context.Context, fileID string) (wireFileMeta, error) {
	path, err := filePath(fileID)
	if err != nil {
		return wireFileMeta{}, err
	}
	// Scope is not represented by the generated FileObject model yet. Use
	// the existing SDK response decoder with this ownership projection.
	resource, err := requestSDK(c, ctx, http.MethodGet, path, sdkCallOptions{Headers: http.Header{"X-Ark-PreSignedURL-ExpiresAfter": {presignedTTLSeconds}}}, func(ctx context.Context, sdk *arkruntime.Client, options []arkruntime.RequestOption) (*fileMetadataResponse, error) {
		result := &fileMetadataResponse{}
		err := sdk.Do(ctx, http.MethodGet, sdk.BaseURL()+path, "", "", result, options...)
		return result, err
	})
	if err != nil {
		return wireFileMeta{}, err
	}
	meta := resource.wireFileMeta
	if c.sessionScope != nil && (meta.Scope.ID != c.sessionScope.SessionID || meta.Scope.Type != "session") {
		return wireFileMeta{}, errors.New("ma file session mismatch")
	}
	if meta.Status != "" && meta.Status != "active" && meta.Status != "processed" && meta.Status != "uploaded" {
		return wireFileMeta{}, errors.New("ma file not ready")
	}
	if !downloadableURL(meta.DownloadURL) {
		return wireFileMeta{}, errors.New("ma file download url unavailable")
	}
	return meta, nil
}

func downloadableURL(raw string) bool {
	target, err := url.Parse(raw)
	return err == nil && (target.Scheme == "https" || target.Scheme == "http") && target.Host != "" && target.User == nil
}

// ResolveFileURL 用宿主凭据取预签名下载地址，不读取对象内容；调用方须按敏感输入保存链接。
func (c *Client) ResolveFileURL(ctx context.Context, fileID string) (string, error) {
	meta, err := c.fileDownloadMeta(ctx, fileID)
	if err != nil {
		return "", err
	}
	return meta.DownloadURL, nil
}

// DownloadFile 通过公开 Files API 读取元数据，再流式下载文件；对象下载不携带 API Key。
// 超过 maxBytes 时返回 core.ErrFileTooLarge。
func (c *Client) DownloadFile(ctx context.Context, fileID string, maxBytes int64) (core.FileContent, error) {
	meta, err := c.fileDownloadMeta(ctx, fileID)
	if err != nil {
		return core.FileContent{}, err
	}
	if maxBytes > 0 && meta.Bytes > maxBytes {
		return core.FileContent{}, fmt.Errorf("ma file too large: %w", core.ErrFileTooLarge)
	}
	content, err := c.download(ctx, meta.DownloadURL, maxBytes)
	if err != nil {
		return core.FileContent{}, err
	}
	content.FileName = meta.FileName
	if meta.MimeType != "" {
		content.MimeType = meta.MimeType
	}
	if meta.Bytes > 0 {
		content.Size = meta.Bytes
	}
	return content, nil
}

// DownloadURL 流式下载一个已签名的对象地址（例如托管侧渲染的会话产出文件）；不携带 API Key。
// 大小取响应 Content-Length；已知超过 maxBytes 时返回 core.ErrFileTooLarge，未知时边读边限。
func (c *Client) DownloadURL(ctx context.Context, rawURL string, maxBytes int64) (core.FileContent, error) {
	if !downloadableURL(rawURL) {
		return core.FileContent{}, errors.New("ma output url invalid")
	}
	return c.download(ctx, rawURL, maxBytes)
}

func (c *Client) download(ctx context.Context, rawURL string, maxBytes int64) (core.FileContent, error) {
	downloadCtx, downloadCancel := context.WithTimeout(ctx, c.uploadTimeout)
	req, err := http.NewRequestWithContext(downloadCtx, http.MethodGet, rawURL, nil)
	if err != nil {
		downloadCancel()
		return core.FileContent{}, errors.New("ma file download request invalid")
	}
	// 预签名地址自带鉴权，不附加 API Key，避免把密钥发给对象存储域名。
	download, err := c.downloadHTTP.Do(req)
	if err != nil {
		downloadCancel()
		return core.FileContent{}, errors.New("ma file download failed")
	}
	if download.StatusCode != http.StatusOK {
		_ = download.Body.Close()
		downloadCancel()
		return core.FileContent{}, errors.New("ma file download rejected")
	}
	limit := maxBytes
	if limit <= 0 {
		limit = maxUploadBytes
	}
	if download.ContentLength > limit {
		_ = download.Body.Close()
		downloadCancel()
		return core.FileContent{}, fmt.Errorf("ma file too large: %w", core.ErrFileTooLarge)
	}
	body := &limitedBody{reader: io.LimitReader(download.Body, limit+1), closer: download.Body, cancel: downloadCancel, limit: limit}
	size := download.ContentLength
	if size < 0 {
		size = 0
	}
	return core.FileContent{Body: body, MimeType: download.Header.Get("Content-Type"), Size: size}, nil
}

// limitedBody 在超过上限时返回错误而不是静默截断，关闭时释放下载上下文。
type limitedBody struct {
	reader io.Reader
	closer io.Closer
	cancel context.CancelFunc
	limit  int64
	read   int64
}

func (b *limitedBody) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	b.read += int64(n)
	if b.read > b.limit {
		return n, fmt.Errorf("ma file exceeds limit: %w", core.ErrFileTooLarge)
	}
	return n, err
}

func (b *limitedBody) Close() error {
	defer b.cancel()
	return b.closer.Close()
}

// fileMetadataResponse retains the scope required for Session ownership checks.
type fileMetadataResponse struct {
	wireFileMeta
	sdkmodel.HttpHeader
}
