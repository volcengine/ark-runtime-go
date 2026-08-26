// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0
package toolset

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var errPathEscape = errors.New("path escapes workdir")

// Resolver 负责把工具输入路径解析到 workdir 内的真实路径。
type Resolver struct {
	root         string
	rootReal     string
	unrestricted bool
}

// NewResolver 创建 workdir 路径解析器。
func NewResolver(root string) (*Resolver, error) {
	return NewResolverWithOptions(root, false)
}

// NewResolverWithOptions 创建 workdir 路径解析器。
func NewResolverWithOptions(root string, unrestricted bool) (*Resolver, error) {
	if root == "" {
		return nil, errors.New("workdir must not be empty")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return nil, err
	}
	real, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return nil, err
	}
	return &Resolver{root: abs, rootReal: real, unrestricted: unrestricted}, nil
}

// Root 返回 workdir 绝对路径。
func (r *Resolver) Root() string {
	return r.root
}

// ResolveExisting 解析一个必须已存在的路径。
func (r *Resolver) ResolveExisting(p string) (string, error) {
	clean, err := r.clean(p)
	if err != nil {
		return "", err
	}
	real, err := filepath.EvalSymlinks(clean)
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("not found: %s", p)
		}
		return "", err
	}
	if !r.unrestricted && !withinRoot(r.rootReal, real) {
		return "", errPathEscape
	}
	return real, nil
}

// ResolveForWrite 解析写入目标，允许目标尚不存在。
func (r *Resolver) ResolveForWrite(p string) (string, error) {
	clean, err := r.clean(p)
	if err != nil {
		return "", err
	}
	dir, base := filepath.Split(clean)
	realDir, err := r.resolveLongest(filepath.Clean(dir))
	if err != nil {
		return "", err
	}
	if !r.unrestricted && !withinRoot(r.rootReal, realDir) {
		return "", errPathEscape
	}
	target := filepath.Join(realDir, base)
	if real, err := filepath.EvalSymlinks(target); err == nil {
		if !r.unrestricted && !withinRoot(r.rootReal, real) {
			return "", errPathEscape
		}
		return real, nil
	} else if !os.IsNotExist(err) {
		return "", err
	}
	if info, err := os.Lstat(target); err == nil && info.Mode()&os.ModeSymlink != 0 && !r.unrestricted {
		return "", fmt.Errorf("%w: unresolved symbolic link", errPathEscape)
	}
	return target, nil
}

func (r *Resolver) clean(p string) (string, error) {
	if p == "" {
		return "", errors.New("path must not be empty")
	}
	switch {
	case p == "~":
		p = r.root
	case strings.HasPrefix(p, "~/"):
		p = filepath.Join(r.root, p[2:])
	case !filepath.IsAbs(p):
		p = filepath.Join(r.root, p)
	}
	return filepath.Clean(p), nil
}

func (r *Resolver) resolveLongest(clean string) (string, error) {
	cur := clean
	var rest []string
	for {
		real, err := filepath.EvalSymlinks(cur)
		if err == nil {
			return filepath.Join(append([]string{real}, rest...)...), nil
		}
		if !os.IsNotExist(err) {
			return "", err
		}
		if info, lstatErr := os.Lstat(cur); lstatErr == nil && info.Mode()&os.ModeSymlink != 0 && !r.unrestricted {
			return "", fmt.Errorf("%w: unresolved symbolic link", errPathEscape)
		} else if lstatErr != nil && !os.IsNotExist(lstatErr) {
			return "", lstatErr
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			return clean, nil
		}
		rest = append([]string{filepath.Base(cur)}, rest...)
		cur = parent
	}
}

func withinRoot(root, target string) bool {
	rel, err := filepath.Rel(root, target)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)) && !filepath.IsAbs(rel))
}
