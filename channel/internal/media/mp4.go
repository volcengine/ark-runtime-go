// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Package media 读取上传媒体的容器元数据，不解码视频或访问外部资源。
package media

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
)

var errMP4Duration = errors.New("mp4 duration unavailable")

const maxDurationBoxes = 4096

// MP4DurationMS 读取 moov/mvhd 的正毫秒时长，并恢复输入原始位置。
// 不扫描媒体样本；分片文件没有声明总时长时返回错误，由上传端保留原有发送行为。
func MP4DurationMS(reader io.ReadSeeker) (duration int, err error) {
	position, err := reader.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, errMP4Duration
	}
	defer func() {
		if _, restoreErr := reader.Seek(position, io.SeekStart); restoreErr != nil {
			duration, err = 0, errMP4Duration
		}
	}()
	end, err := reader.Seek(0, io.SeekEnd)
	if err != nil || end < 8 {
		return 0, errMP4Duration
	}
	budget := maxDurationBoxes
	for offset := int64(0); offset < end; {
		box, err := readBox(reader, offset, end, &budget)
		if err != nil {
			return 0, err
		}
		if box.kind == "moov" {
			return movieDuration(reader, box, &budget)
		}
		offset = box.end
	}
	return 0, errMP4Duration
}

type boxRange struct {
	kind         string
	payload, end int64
}

func readBox(reader io.ReadSeeker, offset, end int64, budget *int) (boxRange, error) {
	if *budget <= 0 || offset < 0 || end-offset < 8 {
		return boxRange{}, errMP4Duration
	}
	*budget--
	if _, err := reader.Seek(offset, io.SeekStart); err != nil {
		return boxRange{}, errMP4Duration
	}
	var header [16]byte
	if _, err := io.ReadFull(reader, header[:8]); err != nil {
		return boxRange{}, errMP4Duration
	}
	size, headerSize := uint64(binary.BigEndian.Uint32(header[:4])), int64(8)
	switch size {
	case 0:
		size = uint64(end - offset)
	case 1:
		if end-offset < 16 {
			return boxRange{}, errMP4Duration
		}
		if _, err := io.ReadFull(reader, header[8:]); err != nil {
			return boxRange{}, errMP4Duration
		}
		size, headerSize = binary.BigEndian.Uint64(header[8:]), 16
	}
	if size < uint64(headerSize) || size > uint64(end-offset) {
		return boxRange{}, errMP4Duration
	}
	return boxRange{kind: string(header[4:8]), payload: offset + headerSize, end: offset + int64(size)}, nil
}

func movieDuration(reader io.ReadSeeker, movie boxRange, budget *int) (int, error) {
	for offset := movie.payload; offset < movie.end; {
		box, err := readBox(reader, offset, movie.end, budget)
		if err != nil {
			return 0, err
		}
		if box.kind == "mvhd" {
			return headerDuration(reader, box)
		}
		offset = box.end
	}
	return 0, errMP4Duration
}

func headerDuration(reader io.ReadSeeker, box boxRange) (int, error) {
	if box.end-box.payload < 100 {
		return 0, errMP4Duration
	}
	var header [32]byte
	if _, err := reader.Seek(box.payload, io.SeekStart); err != nil {
		return 0, errMP4Duration
	}
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return 0, errMP4Duration
	}
	var scale, ticks uint64
	switch header[0] {
	case 0:
		scale = uint64(binary.BigEndian.Uint32(header[12:16]))
		ticks = uint64(binary.BigEndian.Uint32(header[16:20]))
		if ticks == math.MaxUint32 {
			return 0, errMP4Duration
		}
	case 1:
		if box.end-box.payload < 112 {
			return 0, errMP4Duration
		}
		scale = uint64(binary.BigEndian.Uint32(header[20:24]))
		ticks = binary.BigEndian.Uint64(header[24:32])
		if ticks == math.MaxUint64 {
			return 0, errMP4Duration
		}
	default:
		return 0, errMP4Duration
	}
	if scale == 0 || ticks/scale > math.MaxInt32/1000 {
		return 0, errMP4Duration
	}
	millis := ticks/scale*1000 + ticks%scale*1000/scale
	if millis == 0 || millis > math.MaxInt32 {
		return 0, errMP4Duration
	}
	return int(millis), nil
}
