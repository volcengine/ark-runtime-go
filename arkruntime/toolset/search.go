// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0
package toolset

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

const grepOutputContent = "content"

// GlobTool 实现 glob 工具。
type GlobTool struct {
	resolver *Resolver
	limits   Limits
}

// NewGlobTool 创建 glob 工具。
func NewGlobTool(resolver *Resolver, limits Limits) *GlobTool {
	return &GlobTool{resolver: resolver, limits: limits}
}

// Name 返回工具名。
func (t *GlobTool) Name() string { return "glob" }

// Execute 执行 glob。
func (t *GlobTool) Execute(ctx context.Context, input json.RawMessage) Result {
	var req struct {
		Pattern string `json:"pattern"`
		Path    string `json:"path,omitempty"`
	}
	if err := decodeInput(input, &req); err != nil {
		return ErrorResult(err.Error())
	}
	if req.Pattern == "" {
		return ErrorResult("pattern must not be empty")
	}
	root := t.resolver.Root()
	if req.Path != "" {
		resolved, err := t.resolver.ResolveExisting(req.Path)
		if err != nil {
			return ErrorResult(err.Error())
		}
		root = resolved
	}
	re, err := globRegexp(req.Pattern)
	if err != nil {
		return ErrorResult(err.Error())
	}
	type match struct {
		path string
		mod  int64
	}
	var matches []match
	err = filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(t.resolver.Root(), p)
		if err != nil {
			return nil
		}
		rel = filepath.ToSlash(rel)
		name := filepath.ToSlash(d.Name())
		if !re.MatchString(rel) && !re.MatchString(name) {
			return nil
		}
		info, _ := d.Info()
		mod := int64(0)
		if info != nil {
			mod = info.ModTime().UnixNano()
		}
		matches = append(matches, match{path: rel, mod: mod})
		if t.limits.GlobMaxMatches > 0 && len(matches) >= t.limits.GlobMaxMatches {
			return filepath.SkipAll
		}
		return nil
	})
	if err != nil && err != filepath.SkipAll {
		return ErrorResult(err.Error())
	}
	sort.Slice(matches, func(i, j int) bool {
		if matches[i].mod == matches[j].mod {
			return matches[i].path < matches[j].path
		}
		return matches[i].mod > matches[j].mod
	})
	var b strings.Builder
	for _, m := range matches {
		b.WriteString(m.path)
		b.WriteByte('\n')
	}
	if t.limits.GlobMaxMatches > 0 && len(matches) >= t.limits.GlobMaxMatches {
		fmt.Fprintf(&b, "\n[truncated: first %d matches]\n", t.limits.GlobMaxMatches)
	}
	return TextResult(b.String())
}

// GrepTool 实现 grep 工具。
type GrepTool struct {
	resolver *Resolver
	limits   Limits
}

// NewGrepTool 创建 grep 工具。
func NewGrepTool(resolver *Resolver, limits Limits) *GrepTool {
	return &GrepTool{resolver: resolver, limits: limits}
}

// Name 返回工具名。
func (t *GrepTool) Name() string { return "grep" }

// Execute 执行 grep。
func (t *GrepTool) Execute(ctx context.Context, input json.RawMessage) Result {
	var req struct {
		Pattern         string `json:"pattern"`
		Path            string `json:"path,omitempty"`
		GlobFilter      string `json:"glob_filter,omitempty"`
		CaseInsensitive bool   `json:"case_insensitive,omitempty"`
		OutputMode      string `json:"output_mode,omitempty"`
	}
	if err := decodeInput(input, &req); err != nil {
		return ErrorResult(err.Error())
	}
	if req.Pattern == "" {
		return ErrorResult("pattern must not be empty")
	}
	pattern := req.Pattern
	if req.CaseInsensitive {
		pattern = "(?i)" + pattern
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return ErrorResult(err.Error())
	}
	var filter *regexp.Regexp
	if req.GlobFilter != "" {
		filter, err = globRegexp(req.GlobFilter)
		if err != nil {
			return ErrorResult(err.Error())
		}
	}
	root := t.resolver.Root()
	if req.Path != "" {
		root, err = t.resolver.ResolveExisting(req.Path)
		if err != nil {
			return ErrorResult(err.Error())
		}
	}
	mode := req.OutputMode
	if mode == "" {
		mode = grepOutputContent
	}
	var b strings.Builder
	count := 0
	outputTruncated := false
	filesWithMatches := map[string]bool{}
	err = filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(t.resolver.Root(), p)
		if err != nil {
			return nil
		}
		rel = filepath.ToSlash(rel)
		if filter != nil && !filter.MatchString(rel) && !filter.MatchString(filepath.ToSlash(d.Name())) {
			return nil
		}
		host, err := t.resolver.ResolveExisting(rel)
		if err != nil {
			return nil
		}
		remaining := -1
		if t.limits.GrepMaxMatches > 0 {
			remaining = t.limits.GrepMaxMatches - count
		}
		fileCount, truncated, err := grepFile(ctx, host, rel, re, mode, &b, remaining, t.limits.MaxOutputBytes)
		if err != nil {
			return err
		}
		if truncated {
			outputTruncated = true
		}
		if fileCount > 0 {
			filesWithMatches[rel] = true
			count += fileCount
		}
		if outputTruncated || t.limits.GrepMaxMatches > 0 && count >= t.limits.GrepMaxMatches {
			return filepath.SkipAll
		}
		return nil
	})
	if err != nil && err != filepath.SkipAll {
		return ErrorResult(err.Error())
	}
	if mode == "files_with_matches" {
		files := make([]string, 0, len(filesWithMatches))
		for f := range filesWithMatches {
			files = append(files, f)
		}
		sort.Strings(files)
		b.Reset()
		for _, f := range files {
			b.WriteString(f)
			b.WriteByte('\n')
		}
	}
	if mode == "count" {
		b.Reset()
		fmt.Fprintf(&b, "%d\n", count)
	}
	if t.limits.GrepMaxMatches > 0 && count >= t.limits.GrepMaxMatches && mode == grepOutputContent {
		fmt.Fprintf(&b, "\n[truncated: first %d matches]\n", t.limits.GrepMaxMatches)
	}
	if outputTruncated {
		return TextResult(outputWithTruncationMarker(b.String(), t.limits.MaxOutputBytes))
	}
	return TextResult(truncateOutput(b.String(), t.limits.MaxOutputBytes))
}

func grepFile(ctx context.Context, path, rel string, re *regexp.Regexp, mode string, b *strings.Builder, remaining int, maxOutputBytes int64) (int, bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, false, err
	}
	defer func() { _ = f.Close() }()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	lineNo := 0
	count := 0
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return count, false, ctx.Err()
		default:
		}
		lineNo++
		line := scanner.Text()
		if !re.MatchString(line) {
			continue
		}
		count++
		if mode == grepOutputContent && (remaining < 0 || count <= remaining) {
			entry := fmt.Sprintf("%s:%d:%s\n", rel, lineNo, line)
			if !appendWithinLimit(b, entry, maxOutputBytes) {
				return count, true, nil
			}
		}
		if remaining >= 0 && count >= remaining {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return count, false, fmt.Errorf("scan %s: %w", rel, err)
	}
	return count, false, nil
}

func appendWithinLimit(builder *strings.Builder, text string, limit int64) bool {
	if limit <= 0 {
		builder.WriteString(text)
		return true
	}
	remaining := limit - int64(builder.Len())
	if remaining <= 0 {
		return false
	}
	if int64(len(text)) <= remaining {
		builder.WriteString(text)
		return true
	}
	builder.WriteString(text[:remaining])
	return false
}

func outputWithTruncationMarker(output string, limit int64) string {
	const marker = "\n[output truncated]\n"
	if limit <= 0 {
		return output + marker
	}
	if limit <= int64(len(marker)) {
		return marker[:limit]
	}
	keep := limit - int64(len(marker))
	if int64(len(output)) > keep {
		output = output[:keep]
	}
	return output + marker
}

func globRegexp(pattern string) (*regexp.Regexp, error) {
	var b strings.Builder
	b.WriteString("^")
	for i := 0; i < len(pattern); i++ {
		ch := pattern[i]
		switch ch {
		case '*':
			if i+1 < len(pattern) && pattern[i+1] == '*' {
				b.WriteString(".*")
				i++
			} else {
				b.WriteString("[^/]*")
			}
		case '?':
			b.WriteString("[^/]")
		case '.', '+', '(', ')', '|', '^', '$', '{', '}', '[', ']', '\\':
			b.WriteByte('\\')
			b.WriteByte(ch)
		default:
			b.WriteByte(ch)
		}
	}
	b.WriteString("$")
	return regexp.Compile(b.String())
}
