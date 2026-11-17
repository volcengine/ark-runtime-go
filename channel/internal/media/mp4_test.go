// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package media

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func mp4Box(kind string, payload []byte) []byte {
	box := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(box, uint32(len(box)))
	copy(box[4:8], kind)
	copy(box[8:], payload)
	return box
}

func movieHeader(version byte, scale uint32, duration uint64) []byte {
	length := 100
	if version == 1 {
		length = 112
	}
	payload := make([]byte, length)
	payload[0] = version
	if version == 1 {
		binary.BigEndian.PutUint32(payload[20:], scale)
		binary.BigEndian.PutUint64(payload[24:], duration)
	} else {
		binary.BigEndian.PutUint32(payload[12:], scale)
		binary.BigEndian.PutUint32(payload[16:], uint32(duration))
	}
	return mp4Box("mvhd", payload)
}

func TestMP4DurationMS(t *testing.T) {
	for _, tc := range []struct {
		name     string
		version  byte
		scale    uint32
		duration uint64
		want     int
	}{
		{"seconds", 0, 1000, 10000, 10000},
		{"fractional seconds", 0, 48000, 492000, 10250},
		{"64 bit duration", 1, 1000000, 10001250000, 10001250},
		{"integer conversion", 0, 3, 4, 1333},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// mdat 在前，证明时长解析不依赖 moov 出现在文件头。
			data := append(mp4Box("mdat", bytes.Repeat([]byte{7}, 4096)), mp4Box("moov", movieHeader(tc.version, tc.scale, tc.duration))...)
			reader := bytes.NewReader(data)
			_, err := reader.Seek(7, 0)
			require.NoError(t, err)
			got, err := MP4DurationMS(reader)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
			position, err := reader.Seek(0, 1)
			require.NoError(t, err)
			require.EqualValues(t, 7, position, "探测不得改变后续上传的读取位置")
		})
	}
}

func TestMP4DurationSupportsExtendedAndToEndBoxes(t *testing.T) {
	header := movieHeader(0, 1000, 2500)
	moov := make([]byte, 16)
	binary.BigEndian.PutUint32(moov, 1)
	copy(moov[4:], "moov")
	binary.BigEndian.PutUint64(moov[8:], uint64(len(header)+16))
	moov = append(moov, header...)
	got, err := MP4DurationMS(bytes.NewReader(moov))
	require.NoError(t, err)
	require.Equal(t, 2500, got)
	binary.BigEndian.PutUint32(moov, 0)
	moov = append(moov[:8], header...)
	got, err = MP4DurationMS(bytes.NewReader(moov))
	require.NoError(t, err)
	require.Equal(t, 2500, got)
}

func TestMP4DurationRejectsUnknownAndMalformedMetadata(t *testing.T) {
	extended := []byte{0, 0, 0, 1, 'm', 'o', 'o', 'v', 255, 255, 255, 255, 255, 255, 255, 255}
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"missing", mp4Box("mdat", []byte("video"))},
		{"zero timescale", mp4Box("moov", movieHeader(0, 0, 100))},
		{"zero duration", mp4Box("moov", movieHeader(0, 1000, 0))},
		{"unknown v0", mp4Box("moov", movieHeader(0, 1000, math.MaxUint32))},
		{"unknown v1", mp4Box("moov", movieHeader(1, 1000, math.MaxUint64))},
		{"overflow", mp4Box("moov", movieHeader(1, 1, math.MaxUint64-1))},
		{"unsupported version", mp4Box("moov", movieHeader(2, 1000, 1000))},
		{"truncated", []byte{0, 0, 0, 64, 'm', 'o', 'o', 'v'}},
		{"small box", []byte{0, 0, 0, 4, 'm', 'o', 'o', 'v'}},
		{"extended overflow", extended},
		{"too many boxes", bytes.Repeat(mp4Box("free", nil), 4097)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := MP4DurationMS(bytes.NewReader(tc.data))
			require.Error(t, err)
			require.Zero(t, got)
		})
	}
}
