// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// sizedAttachmentSource 生成有限流，避免边界测试在内存中缓存整文件。
type sizedAttachmentSource struct {
	Provider
	size    int64
	unknown bool
}

func (s sizedAttachmentSource) FetchAttachment(context.Context, Message, Attachment) (DownloadedAttachment, error) {
	size := s.size
	if s.unknown {
		size = -1
	}
	return DownloadedAttachment{Body: io.NopCloser(io.LimitReader(attachmentZeroReader{}, s.size)), Size: size, MimeType: "application/pdf"}, nil
}

type attachmentZeroReader struct{}

func (attachmentZeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

type countingAttachmentUpload struct {
	MAClient
	bytes int64
}

func (u *countingAttachmentUpload) UploadFile(_ context.Context, upload FileUpload) (string, error) {
	n, err := io.Copy(io.Discard, upload.Reader)
	u.bytes += n
	return "file-1", err
}

func TestDefaultAttachmentSizeBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name              string
		size              int64
		count             int
		unknown, rejected bool
		limits            AttachmentLimits
	}{
		{name: "below 100 MB", size: 99999999, count: 1},
		{name: "below 100 MB unknown length", size: 99999999, count: 1, unknown: true},
		{name: "100 MB", size: 100000000, count: 1, rejected: true},
		{name: "100 MB unknown length", size: 100000000, count: 1, unknown: true, rejected: true},
		{name: "100 MB plus one", size: 100000001, count: 1, rejected: true},
		{name: "oversize unknown length", size: 100000001, count: 1, unknown: true, rejected: true},
		{name: "five full size files", size: 99999999, count: 5},
		{name: "seven small files", size: 1, count: 7},
		{name: "twenty small files", size: 1, count: 20},
		{name: "twenty one files", size: 1, count: 21, rejected: true},
		{name: "explicit smaller count", size: 1, count: 3, rejected: true, limits: AttachmentLimits{MaxCount: 2}},
		{name: "six full size files exceed total", size: 99999999, count: 6, rejected: true},
		{name: "exact total known", size: 550, count: 2, limits: AttachmentLimits{MaxBytes: 1024, MaxTotalBytes: 1100}},
		{name: "exact total unknown", size: 550, count: 2, unknown: true, limits: AttachmentLimits{MaxBytes: 1024, MaxTotalBytes: 1100}},
		{name: "explicit smaller limit", size: 2048, count: 1, rejected: true, limits: AttachmentLimits{MaxBytes: 1024}},
		{name: "explicit total only", size: 10000000, count: 2, limits: AttachmentLimits{MaxTotalBytes: 50 << 20}},
		{name: "explicit total only rejects overflow", size: 30000000, count: 2, rejected: true, limits: AttachmentLimits{MaxTotalBytes: 50 << 20}},
		{name: "explicit total limit", size: 30000000, count: 2, rejected: true, limits: AttachmentLimits{MaxBytes: 40000000, MaxTotalBytes: 50000000}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			limits, err := tc.limits.normalize()
			require.NoError(t, err)
			r := &Runtime{options: RuntimeOptions{Attachments: limits}}
			j := &job{msg: Message{Attachments: make([]Attachment, tc.count)}}
			uploader := &countingAttachmentUpload{}
			blocks, err := r.prepareAttachments(context.Background(), j, Connection{Provider: sizedAttachmentSource{size: tc.size, unknown: tc.unknown}, MAClient: uploader})
			if tc.rejected {
				require.ErrorIs(t, err, errAttachmentRejected)
				require.Empty(t, blocks)
				return
			}
			require.NoError(t, err)
			require.Len(t, blocks, tc.count)
			require.Equal(t, tc.size*int64(tc.count), uploader.bytes)
		})
	}
}

func TestAttachmentLimitRejectionExplainsLimit(t *testing.T) {
	for _, tc := range []struct {
		name    string
		count   int
		size    int64
		unknown bool
		limits  AttachmentLimits
		want    []string
	}{
		{name: "count before download", count: 21, size: 1, want: []string{"附件数量超出限制", "21", "20"}},
		{name: "custom count", count: 3, size: 1, limits: AttachmentLimits{MaxCount: 2}, want: []string{"附件数量超出限制", "3", "2"}},
		{name: "single file", count: 1, size: 1025, limits: AttachmentLimits{MaxBytes: 1024}, want: []string{"单个附件大小超出限制", "第 1 个", "1025", "1024"}},
		{name: "stream single file", count: 1, size: 1025, unknown: true, limits: AttachmentLimits{MaxBytes: 1024}, want: []string{"单个附件大小超出限制", "至少", "1024"}},
		{name: "total", count: 2, size: 600, limits: AttachmentLimits{MaxBytes: 1024, MaxTotalBytes: 1100}, want: []string{"附件总大小超出限制", "1200", "1100"}},
		{name: "total before last attachment", count: 3, size: 600, limits: AttachmentLimits{MaxBytes: 1024, MaxTotalBytes: 1100}, want: []string{"附件总大小超出限制", "已累计 1200 bytes", "1100"}},
		{name: "stream total", count: 2, size: 600, unknown: true, limits: AttachmentLimits{MaxBytes: 1024, MaxTotalBytes: 1100}, want: []string{"附件总大小超出限制", "至少", "1100"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			limits, err := tc.limits.normalize()
			require.NoError(t, err)
			r := &Runtime{options: RuntimeOptions{Attachments: limits}}
			j := &job{msg: Message{Attachments: make([]Attachment, tc.count)}}
			uploader := &countingAttachmentUpload{}
			blocks, err := r.prepareAttachments(context.Background(), j, Connection{Provider: sizedAttachmentSource{size: tc.size, unknown: tc.unknown}, MAClient: uploader})
			require.ErrorIs(t, err, errAttachmentRejected)
			require.Empty(t, blocks)
			var rejection interface{ reply() string }
			require.ErrorAs(t, err, &rejection)
			for _, want := range tc.want {
				require.Contains(t, rejection.reply(), want)
			}
			if tc.count > limits.MaxCount {
				require.Zero(t, uploader.bytes)
			}
		})
	}
}

func TestAttachmentFailureReplyDoesNotExposeUpstreamError(t *testing.T) {
	require.Equal(t, "当前渠道不支持处理附件，请联系管理员调整配置。", attachmentFailureReply(errAttachmentRejected))
}
