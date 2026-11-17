// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/volcengine/ark-runtime-go/channel/internal/markdown"
)

// SessionOutputFile 是公开 Files API 登记的 Session 产物，不包含临时下载地址。
type SessionOutputFile struct {
	FileID   string
	Path     string
	FileName string
	MimeType string
	Bytes    int64
}

// SessionOutputLister 列出指定 Session 的 outputs 产物，供最终答案解析引用。
type SessionOutputLister interface {
	ListSessionOutputFiles(context.Context, string) ([]SessionOutputFile, error)
}

// SessionFileDownloader 使用文件所属 Session 的凭据下载产物。
type SessionFileDownloader interface {
	DownloadSessionFile(context.Context, string, string, int64) (FileContent, error)
	ResolveSessionFileURL(context.Context, string, string) (string, error)
}

type artifactReference struct {
	Start, End int
	Artifact   Artifact
}

var outputMentionPattern = regexp.MustCompile(`[^\s/` + "`" + `'"<>()\[\]{}|，。；：、！？（）]+\.(?:[a-zA-Z][a-zA-Z0-9]{0,9}|7z)\b`)

var outputDirectoryPattern = regexp.MustCompile("(?:/mnt/session/(?:outputs|storage)|/workspace/outputs)(?:/[^\\s`'\"<>()\\[\\]{}|，。；：、！？（）《》“”‘’]*)?")
var inlineCodeReferencePattern = regexp.MustCompile("`+([^`\\n]+)`+")

type outputDirectoryHint struct {
	position  int
	directory string
}

func answerDirectoryHints(masked string) []outputDirectoryHint {
	var hints []outputDirectoryHint
	add := func(raw string, position int) {
		if !strings.HasSuffix(raw, "/") && path.Ext(raw) != "" {
			return
		}
		if normalized, ok := normalizeOutputPath(strings.TrimSuffix(raw, "/") + "/directory-placeholder"); ok {
			hints = append(hints, outputDirectoryHint{position, path.Dir(normalized) + "/"})
		}
	}
	codes := inlineCodeReferencePattern.FindAllStringSubmatchIndex(masked, -1)
	links := markdown.FileLinks(masked)
	for _, link := range links {
		add(link.Destination, link.Start)
	}
	for _, code := range codes {
		add(masked[code[2]:code[3]], code[0])
	}
	for _, loc := range outputDirectoryPattern.FindAllStringIndex(masked, maxAnswerPaths) {
		insideCode := false
		for _, link := range links {
			if link.Start <= loc[0] && loc[0] < link.End {
				insideCode = true
				break
			}
		}
		for _, code := range codes {
			if code[0] <= loc[0] && loc[0] < code[1] {
				insideCode = true
				break
			}
		}
		if insideCode || loc[0] > 0 && !referenceBoundary(masked[:loc[0]], true) {
			continue
		}
		add(masked[loc[0]:loc[1]], loc[0])
	}
	sort.SliceStable(hints, func(i, j int) bool { return hints[i].position < hints[j].position })
	return hints
}

// collectSessionArtifacts 用公开 Files 清单关联答案引用；不把整份 Session 历史当成本轮附件。
func (r *Runtime) collectSessionArtifacts(ctx context.Context, j *job, answer string, legacy []Artifact) ([]Artifact, error) {
	lister, ok := j.connection.MAClient.(SessionOutputLister)
	if !ok || !j.binding.DeliverArtifacts {
		return legacy, nil
	}
	var toolRefs []artifactReference
	for _, link := range markdown.FileLinks(answer) {
		for _, a := range legacy {
			if a.FileID != "" && a.FileID == link.Destination {
				toolRefs = append(toolRefs, artifactReference{link.Start, link.End, a})
				break
			}
		}
	}
	j.answerReferences = toolRefs
	if !missingOutputReferences(answer, toolRefs) {
		return legacy, nil
	}
	var files []SessionOutputFile
	for attempt := range 3 {
		var err error
		files, err = lister.ListSessionOutputFiles(ctx, j.Session.SessionID)
		if err != nil {
			return legacy, err
		}
		j.answerReferences = matchOutputReferences(answer, files)
		j.answerReferences = append(j.answerReferences, toolRefs...)
		sort.SliceStable(j.answerReferences, func(i, jj int) bool { return j.answerReferences[i].Start < j.answerReferences[jj].Start })
		if !missingOutputReferences(answer, j.answerReferences) || attempt == 2 {
			break
		}
		timer := time.NewTimer(time.Duration(attempt+1) * 100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return legacy, ctx.Err()
		case <-timer.C:
		}
	}
	seen := make(map[string]bool)
	var result []Artifact
	add := func(a Artifact) {
		if !seen[a.key()] {
			seen[a.key()] = true
			result = append(result, a)
		}
	}
	for i := range j.answerReferences {
		if j.answerReferences[i].Artifact.Path != "" {
			j.answerReferences[i].Artifact.SessionID = j.Session.SessionID
		}
		add(j.answerReferences[i].Artifact)
	}
	for _, a := range legacy {
		if a.Path != "" && a.FileID == "" {
			var matches []SessionOutputFile
			for _, file := range files {
				if file.Path == a.Path {
					matches = append(matches, file)
				}
			}
			if len(matches) == 1 {
				file := matches[0]
				a.FileID, a.SessionID, a.MimeType = file.FileID, j.Session.SessionID, file.MimeType
			}
		}
		add(a)
	}
	if missingOutputReferences(answer, j.answerReferences) {
		return result, errors.New("session output references unresolved")
	}
	return result, nil
}

func missingOutputReferences(answer string, refs []artifactReference) bool {
	masked := markdown.MaskCodeBlocks(answer)
	covered := func(start, end int) bool {
		for _, ref := range refs {
			if ref.Start <= start && end <= ref.End {
				return true
			}
		}
		return false
	}
	for _, loc := range outputPathPattern.FindAllStringIndex(masked, maxAnswerPaths) {
		if path.Ext(strings.TrimRight(masked[loc[0]:loc[1]], ".,;:!?*_~")) != "" && !covered(loc[0], loc[1]) && (loc[0] == 0 || referenceBoundary(masked[:loc[0]], true)) {
			return true
		}
	}
	for _, loc := range outputMentionPattern.FindAllStringIndex(masked, maxAnswerPaths) {
		switch strings.ToLower(path.Ext(masked[loc[0]:loc[1]])) {
		case ".com", ".org", ".net", ".cn", ".io", ".dev", ".app", ".edu", ".gov", ".co", ".ai", ".test":
			if loc[0] == 0 || masked[loc[0]-1] != '`' {
				continue
			}
		}
		knownName := false
		for _, ref := range refs {
			if ref.Artifact.FileName == masked[loc[0]:loc[1]] {
				knownName = true
				break
			}
		}
		if !knownName && !covered(loc[0], loc[1]) && (loc[0] == 0 || referenceBoundary(masked[:loc[0]], true)) && (loc[1] == len(masked) || referenceBoundary(masked[loc[1]:], false)) {
			return true
		}
	}
	return false
}

// matchOutputReferences 只关联已登记文件；完整路径、目录加文件名、唯一文件名依次消歧。
func matchOutputReferences(answer string, files []SessionOutputFile) []artifactReference {
	masked := markdown.MaskCodeBlocks(answer)
	directories := answerDirectoryHints(masked)
	byPath := make(map[string][]Artifact)
	byName := make(map[string][]Artifact)
	for _, file := range files {
		p, ok := normalizeOutputPath(file.Path)
		if !ok || !strings.HasPrefix(p, sessionOutputsDir) || !validFileID(file.FileID) {
			continue
		}
		a := Artifact{FileID: file.FileID, Path: p, FileName: path.Base(p), MimeType: file.MimeType, Kind: ArtifactKindByName(p)}
		byPath[p] = append(byPath[p], a)
		byName[path.Base(p)] = append(byName[path.Base(p)], a)
	}
	resolve := func(raw string, position int) (Artifact, bool) {
		if decoded, err := url.PathUnescape(raw); err == nil {
			raw = decoded
		}
		raw = strings.TrimPrefix(raw, "file://")
		if strings.HasPrefix(raw, "/") {
			p, ok := normalizeOutputPath(raw)
			if !ok || len(byPath[p]) != 1 {
				return Artifact{}, false
			}
			return byPath[p][0], true
		}
		if strings.ContainsAny(raw, "/\\:?") {
			return Artifact{}, false
		}
		candidates := byName[raw]
		directory := ""
		for _, hint := range directories {
			if hint.position >= position {
				break
			}
			directory = hint.directory
		}
		if directory != "" {
			if matches := byPath[directory+raw]; len(matches) == 1 {
				return matches[0], true
			}
			return Artifact{}, false
		}
		if len(candidates) != 1 {
			return Artifact{}, false
		}
		return candidates[0], true
	}
	var refs []artifactReference
	links := markdown.FileLinks(masked)
	for _, link := range links {
		if a, ok := resolve(link.Destination, link.Start); ok {
			refs = append(refs, artifactReference{link.Start, link.End, a})
		}
	}
	overlapsLink := func(start, end int) bool {
		for _, link := range links {
			if start < link.End && link.Start < end {
				return true
			}
		}
		return false
	}
	addMatches := func(token string) {
		for offset := 0; offset < len(masked); {
			index := strings.Index(masked[offset:], token)
			if index < 0 {
				break
			}
			start, end := offset+index, offset+index+len(token)
			offset = end
			if overlapsLink(start, end) || start > 0 && !referenceBoundary(masked[:start], true) || end < len(masked) && !referenceBoundary(masked[end:], false) {
				continue
			}
			a, ok := resolve(token, start)
			if !ok {
				continue
			}
			if start > 0 && end < len(masked) && masked[start-1] == '`' && masked[end] == '`' {
				start--
				end++
			}
			refs = append(refs, artifactReference{start, end, a})
		}
	}
	for p := range byPath {
		addMatches(p)
		addMatches("file://" + p)
		addMatches("/workspace/outputs/" + strings.TrimPrefix(p, sessionOutputsDir))
	}
	for name := range byName {
		addMatches(name)
	}
	sort.SliceStable(refs, func(i, j int) bool {
		if refs[i].Start == refs[j].Start {
			return refs[i].End > refs[j].End
		}
		return refs[i].Start < refs[j].Start
	})
	var result []artifactReference
	for _, ref := range refs {
		if len(result) == 0 || ref.Start >= result[len(result)-1].End {
			result = append(result, ref)
		}
	}
	var selected []artifactReference
	for _, ref := range result {
		if !strings.Contains(answer[ref.Start:ref.End], "/") {
			duplicate := false
			for _, other := range result {
				if other.Artifact.key() != ref.Artifact.key() || !strings.Contains(answer[other.Start:other.End], "/") {
					continue
				}
				if other.Start < ref.End {
					continue
				}
				separator := strings.TrimSpace(answer[ref.End:other.Start])
				if separator == "→" || separator == "->" || separator == ":" || separator == "：" {
					duplicate = true
					break
				}
			}
			if duplicate {
				continue
			}
		}
		selected = append(selected, ref)
	}
	return selected
}

func referenceBoundary(text string, before bool) bool {
	r, _ := utf8.DecodeRuneInString(text)
	if before {
		r, _ = utf8.DecodeLastRuneInString(text)
	}
	return !unicode.IsLetter(r) && !unicode.IsDigit(r) && !strings.ContainsRune("/\\._-:%?=&", r)
}
