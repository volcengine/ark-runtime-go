// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

// Package markdown 保留答案原文，只定位图片引用与不可拆开的代码片段。
package markdown

import (
	"strings"
	"unicode/utf8"
)

// Image 保存图片引用的原文、字节范围及已解除 Markdown 转义的目标。
type Image struct {
	Start, End  int
	Text        string
	Destination string
	backticks   int
}

// WithDestination 保留图片名称，使用上传后得到的 Provider 资源替换目标。
func (image Image) WithDestination(destination string) string {
	end := bracketEnd(image.Text, 1, '[', ']')
	raw := image.Text[end+1 : len(image.Text)-1]
	leading := len(raw) - len(strings.TrimLeft(raw, " \t\r\n"))
	targetEnd := destinationEnd(raw[leading:]) + leading
	return image.Text[:end+1] + raw[:leading] + destination + raw[targetEnd:] + ")"
}

// Code 使用全文中未出现过的反引号长度，避免与前文未闭合的代码分隔符配对。
func (image Image) Code() string {
	delimiter := strings.Repeat("`", image.backticks+1)
	return delimiter + image.Text + delimiter
}

type span struct{ start, end int }

// Images 返回代码示例之外的内联或显式引用式图片；引用式目标留空，由调用方保守降级。
func Images(text string) []Image {
	images, _ := scan(text)
	longest := longestBackticks(text)
	for i := range images {
		images[i].backticks = longest
	}
	return images
}

// ReplaceImages 按原文顺序替换图片，不重排或重新格式化其他 Markdown。
func ReplaceImages(text string, replace func(Image) string) string {
	var out strings.Builder
	start := 0
	for _, image := range Images(text) {
		out.WriteString(text[start:image.Start])
		out.WriteString(replace(image))
		start = image.End
	}
	out.WriteString(text[start:])
	return out.String()
}

func longestBackticks(text string) int {
	longest := 0
	for i := 0; i < len(text); i++ {
		if text[i] == '`' {
			n := run(text, i, '`')
			longest = max(longest, n)
			i += n - 1
		}
	}
	return longest
}

// PrefixEnd 将 rune 前缀终点向后扩展到完整图片或代码边界。
func PrefixEnd(text string, position int) int {
	runes := []rune(text)
	position = min(max(position, 0), len(runes))
	end := len(string(runes[:position]))
	_, atoms := scan(text)
	for _, atom := range atoms {
		if atom.start < end && end < atom.end {
			return utf8.RuneCountInString(text[:atom.end])
		}
	}
	return position
}

func scan(text string) ([]Image, []span) {
	var images []Image
	var atoms []span
	for i := 0; i < len(text); {
		if end := fenceEnd(text, i); end > i {
			atoms = append(atoms, span{i, end})
			i = end
			continue
		}
		if text[i] == '\\' && i+1 < len(text) {
			i += 2
			continue
		}
		if text[i] == '`' {
			n := run(text, i, '`')
			if end := codeEnd(text, i+n, n); end > 0 {
				atoms = append(atoms, span{i, end})
				i = end
				continue
			}
			i += n
			continue
		}
		if strings.HasPrefix(text[i:], "![") {
			if image, ok := imageAt(text, i); ok {
				images = append(images, image)
				atoms = append(atoms, span{i, image.End})
				i = image.End
				continue
			}
		}
		i++
	}
	return images, atoms
}

func run(text string, start int, marker byte) int {
	end := start
	for end < len(text) && text[end] == marker {
		end++
	}
	return end - start
}

func codeEnd(text string, start, count int) int {
	for i := start; i < len(text); i++ {
		if text[i] != '`' {
			continue
		}
		n := run(text, i, '`')
		if n == count {
			return i + n
		}
		i += n - 1
	}
	return 0
}

func fenceEnd(text string, start int) int {
	if start > 0 && text[start-1] != '\n' {
		return 0
	}
	if strings.HasPrefix(text[start:], "    ") || text[start] == '\t' {
		previous := ""
		if start > 0 {
			before := strings.TrimSuffix(text[:start], "\n")
			previous = before[strings.LastIndexByte(before, '\n')+1:]
		}
		if strings.TrimSpace(previous) == "" || strings.HasPrefix(previous, "    ") || strings.HasPrefix(previous, "\t") {
			if end := strings.IndexByte(text[start:], '\n'); end >= 0 {
				return start + end
			}
			return len(text)
		}
	}
	i, quotes := fencePrefix(text, start)
	if i >= len(text) || text[i] != '`' && text[i] != '~' {
		return 0
	}
	marker, count := text[i], run(text, i, text[i])
	if count < 3 {
		return 0
	}
	if marker == '`' {
		info := text[i+count:]
		if end := strings.IndexByte(info, '\n'); end >= 0 {
			info = info[:end]
		}
		if strings.ContainsRune(info, '`') {
			return 0
		}
	}
	for i += count; i < len(text); i++ {
		if text[i-1] != '\n' {
			continue
		}
		line := i
		var closingQuotes int
		i, closingQuotes = fencePrefix(text, line)
		if closingQuotes < quotes {
			return line
		}
		if run(text, i, marker) >= count {
			end := i + run(text, i, marker)
			lineEnd := strings.IndexByte(text[end:], '\n')
			if lineEnd < 0 {
				lineEnd = len(text) - end
			}
			if strings.TrimSpace(text[end:end+lineEnd]) == "" {
				return end + lineEnd
			}
		}
	}
	return len(text)
}

func fencePrefix(text string, start int) (int, int) {
	i, quotes := start, 0
	for {
		for n := 0; n < 3 && i < len(text) && text[i] == ' '; n++ {
			i++
		}
		if i >= len(text) || text[i] != '>' {
			return i, quotes
		}
		quotes++
		i++
		if i < len(text) && text[i] == ' ' {
			i++
		}
	}
}

func bracketEnd(text string, start int, open, close byte) int {
	depth := 1
	for i := start + 1; i < len(text); i++ {
		if text[i] == '\\' && i+1 < len(text) {
			i++
			continue
		}
		if text[i] == open {
			depth++
		}
		if text[i] == close {
			depth--
			if depth == 0 {
				return i + 1
			}
		}
	}
	return 0
}

func imageAt(text string, start int) (Image, bool) {
	return referenceAt(text, start, start+1)
}

func referenceAt(text string, start, labelStart int) (Image, bool) {
	labelEnd := bracketEnd(text, labelStart, '[', ']')
	if labelEnd == 0 {
		return Image{}, false
	}
	end, target := 0, ""
	if labelEnd == len(text) {
		return Image{Start: start, End: labelEnd, Text: text[start:labelEnd]}, true
	}
	switch text[labelEnd] {
	case '(':
		end = linkEnd(text, labelEnd)
		if end > 0 {
			target = destination(text[labelEnd+1 : end-1])
		}
	case '[':
		end = bracketEnd(text, labelEnd, '[', ']')
	default:
		end = labelEnd
	}
	if end == 0 {
		return Image{}, false
	}
	return Image{Start: start, End: end, Text: text[start:end], Destination: target}, true
}

// FileLinks 返回代码之外的图片和文件链接，保留整段引用的原文范围。
func FileLinks(text string) []Image {
	var result []Image
	for i := 0; i < len(text); {
		if end := fenceEnd(text, i); end > i {
			i = end
			continue
		}
		if text[i] == '\\' && i+1 < len(text) {
			i += 2
			continue
		}
		if text[i] == '`' {
			n := run(text, i, '`')
			if end := codeEnd(text, i+n, n); end > 0 {
				i = end
				continue
			}
			i += n
			continue
		}
		label := i
		if strings.HasPrefix(text[i:], "![") {
			label++
		}
		if text[label] == '[' {
			if ref, ok := referenceAt(text, i, label); ok && ref.Destination != "" {
				result = append(result, ref)
				i = ref.End
				continue
			}
		}
		i++
	}
	return result
}

// MaskCodeBlocks 屏蔽代码块并保留字节位置；行内代码仍可表达真实文件引用。
func MaskCodeBlocks(text string) string {
	masked := []byte(text)
	for i := 0; i < len(text); {
		if end := fenceEnd(text, i); end > i {
			for j := i; j < end; j++ {
				if masked[j] != '\n' {
					masked[j] = ' '
				}
			}
			i = end
			continue
		}
		i++
	}
	return string(masked)
}

func linkEnd(text string, start int) int {
	depth, quote, angle := 1, byte(0), false
	for i := start + 1; i < len(text); i++ {
		c := text[i]
		if c == '\\' && i+1 < len(text) {
			i++
			continue
		}
		if angle {
			angle = c != '>'
			continue
		}
		if quote != 0 {
			if c == quote {
				quote = 0
			}
			continue
		}
		if c == '<' {
			angle = true
			continue
		}
		if (c == '"' || c == '\'') && strings.ContainsRune(" \t\r\n", rune(text[i-1])) {
			quote = c
			continue
		}
		if c == '(' {
			depth++
		}
		if c == ')' {
			depth--
			if depth == 0 {
				return i + 1
			}
		}
	}
	return 0
}

func destination(raw string) string {
	raw = strings.TrimSpace(raw)
	end := destinationEnd(raw)
	if strings.HasPrefix(raw, "<") {
		if end > 1 && raw[end-1] == '>' {
			return unescape(raw[1 : end-1])
		}
	}
	return unescape(raw[:end])
}

func destinationEnd(raw string) int {
	if strings.HasPrefix(raw, "<") {
		if end := strings.IndexByte(raw, '>'); end >= 0 {
			return end + 1
		}
	}
	depth := 0
	for i := 0; i < len(raw); i++ {
		if raw[i] == '\\' && i+1 < len(raw) {
			i++
			continue
		}
		if raw[i] == '(' {
			depth++
		}
		if raw[i] == ')' {
			depth--
		}
		if depth == 0 && strings.ContainsRune(" \t\r\n", rune(raw[i])) {
			return i
		}
	}
	return len(raw)
}

func unescape(text string) string {
	var out strings.Builder
	for i := 0; i < len(text); i++ {
		if text[i] == '\\' && i+1 < len(text) && strings.ContainsRune("!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~", rune(text[i+1])) {
			i++
		}
		out.WriteByte(text[i])
	}
	return out.String()
}
