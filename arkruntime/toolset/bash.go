// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0
package toolset

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// BashTool 实现持久 bash 工具。
type BashTool struct {
	session *BashSession
	timeout time.Duration
}

// NewBashTool 创建 bash 工具。
func NewBashTool(opts Options) (*BashTool, error) {
	if opts.Limits == (Limits{}) {
		opts.Limits = DefaultLimits()
	}
	if opts.ToolTimeout <= 0 {
		opts.ToolTimeout = 120 * time.Second
	}
	session, err := NewBashSession(opts)
	if err != nil {
		return nil, err
	}
	return &BashTool{session: session, timeout: opts.ToolTimeout}, nil
}

// Name 返回工具名。
func (t *BashTool) Name() string { return "bash" }

// Execute 执行 bash 命令。
func (t *BashTool) Execute(ctx context.Context, input json.RawMessage) Result {
	var req struct {
		Command   string `json:"command"`
		Cmd       string `json:"cmd,omitempty"`
		TimeoutMS int    `json:"timeout_ms,omitempty"`
		Restart   bool   `json:"restart,omitempty"`
	}
	if err := decodeInput(input, &req); err != nil {
		return ErrorResult(err.Error())
	}
	if req.Restart {
		if err := t.session.Restart(); err != nil {
			return ErrorResult(err.Error())
		}
		return TextResult("bash session restarted")
	}
	command := req.Command
	if command == "" {
		command = req.Cmd
	}
	if command == "" {
		return ErrorResult("command must not be empty")
	}
	timeout := t.timeout
	if req.TimeoutMS > 0 {
		timeout = time.Duration(req.TimeoutMS) * time.Millisecond
	}
	out, exit, timedOut, err := t.session.Run(ctx, command, timeout)
	if err != nil {
		return ErrorResult(err.Error())
	}
	if timedOut {
		return ErrorResult(fmt.Sprintf("command timed out after %s\n%s", timeout, out))
	}
	return TextResult(fmt.Sprintf("exit_code: %d\n%s", exit, out))
}

// Close 关闭 bash 会话。
func (t *BashTool) Close() error {
	return t.session.Close()
}

// BashSession 是 per-worker 的持久 bash 子进程。
type BashSession struct {
	mu         sync.Mutex
	cond       *sync.Cond
	cmd        *exec.Cmd
	stdin      *os.File
	outR       *os.File
	ctrlR      *os.File
	done       chan struct{}
	buf        bytes.Buffer
	ctrlBuf    bytes.Buffer
	dead       bool
	gen        int64
	workdir    string
	privateDir string
	env        map[string]string
	bashPath   string
	limits     Limits
}

// NewBashSession 创建持久 bash 会话。
func NewBashSession(opts Options) (*BashSession, error) {
	if opts.BashPath == "" {
		opts.BashPath = "/bin/bash"
	}
	if opts.Limits == (Limits{}) {
		opts.Limits = DefaultLimits()
	}
	if opts.Workdir == "" {
		return nil, errors.New("workdir must not be empty")
	}
	if err := os.MkdirAll(opts.Workdir, 0o755); err != nil {
		return nil, err
	}
	s := &BashSession{
		workdir:  opts.Workdir,
		env:      opts.Env,
		bashPath: opts.BashPath,
		limits:   opts.Limits,
	}
	s.cond = sync.NewCond(&s.mu)
	if err := s.startLocked(); err != nil {
		_ = os.RemoveAll(s.privateDir)
		return nil, err
	}
	return s, nil
}

// Restart 重建持久 bash。
func (s *BashSession) Restart() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeLocked()
	return s.startLocked()
}

// Run 在持久 bash 中执行命令。
func (s *BashSession) Run(ctx context.Context, command string, timeout time.Duration) (string, int, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dead {
		if err := s.startLocked(); err != nil {
			return "", -1, false, err
		}
	}
	cmdID := nonce()
	controlID := nonce()
	info, err := os.Lstat(s.privateDir)
	if err != nil {
		return "", -1, false, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", -1, false, errors.New("bash private directory is not a directory")
	}
	cmdPath := filepath.Join(s.privateDir, cmdID+".sh")
	statePath := filepath.Join(s.privateDir, cmdID+".state")
	if err := os.WriteFile(cmdPath, []byte(command), 0o600); err != nil {
		return "", -1, false, err
	}
	defer func() { _ = os.Remove(cmdPath) }()
	defer func() { _ = os.Remove(statePath) }()

	s.buf.Reset()
	s.ctrlBuf.Reset()
	frame := s.execFrame(cmdPath, statePath, controlID)
	if _, err := io.WriteString(s.stdin, frame); err != nil {
		s.dead = true
		return "", -1, false, fmt.Errorf("write bash stdin: %w", err)
	}

	deadline := time.Now().Add(timeout)
	wakeDone := make(chan struct{})
	go func() {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case <-ctx.Done():
		case <-timer.C:
		case <-wakeDone:
			return
		}
		s.mu.Lock()
		s.cond.Broadcast()
		s.mu.Unlock()
	}()
	defer close(wakeDone)

	for {
		if _, exit, consumed, ok := parseControl(s.ctrlBuf.Bytes(), controlID); ok {
			remaining := append([]byte(nil), s.ctrlBuf.Bytes()[consumed:]...)
			s.ctrlBuf.Reset()
			_, _ = s.ctrlBuf.Write(remaining)
			return truncateOutput(s.buf.String(), s.limits.MaxOutputBytes), exit, false, nil
		}
		if s.dead {
			return "", -1, false, errors.New("persistent bash terminated before command completed")
		}
		if err := ctx.Err(); err != nil {
			s.closeLocked()
			return "", -1, true, err
		}
		if timeout > 0 && time.Now().After(deadline) {
			partial := truncateOutput(s.buf.String(), s.limits.MaxOutputBytes)
			s.closeLocked()
			return partial, -1, true, nil
		}
		s.cond.Wait()
	}
}

// Close 关闭持久 bash。
func (s *BashSession) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeLocked()
	err := os.RemoveAll(s.privateDir)
	s.privateDir = ""
	return err
}

func (s *BashSession) startLocked() error {
	if s.privateDir == "" {
		privateDir, err := os.MkdirTemp("", "ark-self-host-shell-*")
		if err != nil {
			return err
		}
		s.privateDir = privateDir
	}
	cmd := exec.Command(s.bashPath, "--noprofile", "--norc")
	cmd.Dir = s.workdir
	cmd.Env = scrubbedEnv(s.env)
	setProcessGroup(cmd)

	inR, inW, err := os.Pipe()
	if err != nil {
		return err
	}
	outR, outW, err := os.Pipe()
	if err != nil {
		_ = inR.Close()
		_ = inW.Close()
		return err
	}
	ctrlR, ctrlW, err := os.Pipe()
	if err != nil {
		_ = inR.Close()
		_ = inW.Close()
		_ = outR.Close()
		_ = outW.Close()
		return err
	}
	cmd.Stdin = inR
	cmd.Stdout = outW
	cmd.Stderr = outW
	cmd.ExtraFiles = []*os.File{ctrlW}
	if err := cmd.Start(); err != nil {
		_ = inR.Close()
		_ = inW.Close()
		_ = outR.Close()
		_ = outW.Close()
		_ = ctrlR.Close()
		_ = ctrlW.Close()
		return err
	}
	_ = inR.Close()
	_ = outW.Close()
	_ = ctrlW.Close()
	s.cmd = cmd
	s.stdin = inW
	s.outR = outR
	s.ctrlR = ctrlR
	s.done = make(chan struct{})
	s.buf.Reset()
	s.ctrlBuf.Reset()
	s.dead = false
	s.gen++
	gen := s.gen
	go s.readOutputLoop(gen, outR)
	go s.readControlLoop(gen, ctrlR)
	go s.waitLoop(gen, cmd, s.done)
	return nil
}

func (s *BashSession) closeLocked() {
	if s.stdin != nil {
		_ = s.stdin.Close()
		s.stdin = nil
	}
	if s.outR != nil {
		_ = s.outR.Close()
		s.outR = nil
	}
	if s.ctrlR != nil {
		_ = s.ctrlR.Close()
		s.ctrlR = nil
	}
	killCommandGroup(s.cmd)
	s.cmd = nil
	s.dead = true
	s.buf.Reset()
	s.ctrlBuf.Reset()
	s.cond.Broadcast()
}

func (s *BashSession) readOutputLoop(gen int64, r *os.File) {
	buf := make([]byte, 4096)
	for {
		n, err := r.Read(buf)
		s.mu.Lock()
		current := gen == s.gen
		if current && n > 0 {
			_, _ = s.buf.Write(buf[:n])
			if s.limits.MaxOutputBytes > 0 && int64(s.buf.Len()) > s.limits.MaxOutputBytes*4 {
				data := s.buf.Bytes()
				keep := int(s.limits.MaxOutputBytes * 2)
				if keep < len(data) {
					s.buf.Reset()
					_, _ = s.buf.Write(data[len(data)-keep:])
				}
			}
		}
		if current && err != nil {
			s.dead = true
		}
		if current {
			s.cond.Broadcast()
		}
		s.mu.Unlock()
		if err != nil {
			return
		}
	}
}

func (s *BashSession) readControlLoop(gen int64, r *os.File) {
	buf := make([]byte, 256)
	for {
		n, err := r.Read(buf)
		s.mu.Lock()
		current := gen == s.gen
		if current && n > 0 {
			_, _ = s.ctrlBuf.Write(buf[:n])
		}
		if current && err != nil {
			s.dead = true
		}
		if current {
			s.cond.Broadcast()
		}
		s.mu.Unlock()
		if err != nil {
			return
		}
	}
}

func (s *BashSession) waitLoop(gen int64, cmd *exec.Cmd, done chan struct{}) {
	_ = cmd.Wait()
	close(done)
	s.mu.Lock()
	if gen == s.gen {
		s.dead = true
		s.cond.Broadcast()
	}
	s.mu.Unlock()
}

func (s *BashSession) execFrame(cmdPath, statePath, nonce string) string {
	return fmt.Sprintf(
		"builtin set +e; builtin set +o pipefail\n"+
			"__ark_st=%s\n"+
			"( builtin trap '__ark_c=$?; { builtin printf \"builtin cd %%q\\n\" \"$PWD\"; builtin set +o | command grep -vE \" (errexit|pipefail|xtrace)$\"; ( builtin unset __ark_st __ark_c __ark_ec; builtin declare -p ); builtin declare -f; } > \"$__ark_st\" 2>/dev/null; builtin exit $__ark_c' EXIT\n"+
			"  . %s ) 3>&-\n"+
			"__ark_ec=$?\n"+
			"{ builtin set +e; . \"$__ark_st\"; } >/dev/null 2>&1 3>&-\n"+
			"command rm -f %s \"$__ark_st\"\n"+
			"builtin printf '__MA_WORKER_DONE_%s:%%d\\n' \"$__ark_ec\" >&3\n",
		shellQuote(statePath), shellQuote(cmdPath), shellQuote(cmdPath), nonce,
	)
}

func parseControl(data []byte, nonce string) ([]byte, int, int, bool) {
	prefix := []byte("__MA_WORKER_DONE_" + nonce + ":")
	idx := bytes.Index(data, prefix)
	if idx < 0 {
		return nil, 0, 0, false
	}
	start := idx + len(prefix)
	endRel := bytes.IndexByte(data[start:], '\n')
	if endRel < 0 {
		return nil, 0, 0, false
	}
	end := start + endRel
	exit, err := strconv.Atoi(string(data[start:end]))
	if err != nil {
		return nil, 0, 0, false
	}
	output := append([]byte(nil), data[:idx]...)
	return output, exit, end + 1, true
}

func scrubbedEnv(extra map[string]string) []string {
	if extra != nil {
		env := make([]string, 0, len(extra))
		for key, value := range extra {
			if !isSensitiveEnvKey(key) {
				env = append(env, key+"="+value)
			}
		}
		return env
	}
	env := make([]string, 0, len(os.Environ()))
	for _, item := range os.Environ() {
		key, _, _ := strings.Cut(item, "=")
		if !isSensitiveEnvKey(key) {
			env = append(env, item)
		}
	}
	return env
}

func isSensitiveEnvKey(key string) bool {
	upper := strings.ToUpper(strings.TrimSpace(key))
	for _, prefix := range []string{"ARK_", "MA_", "ANTHROPIC_", "OPENAI_", "AWS_", "AZURE_", "GOOGLE_"} {
		if strings.HasPrefix(upper, prefix) {
			return true
		}
	}
	for _, exact := range []string{
		"VOLC_ACCESSKEY", "VOLC_SECRETKEY", "BYTEPLUS_ACCESSKEY", "BYTEPLUS_SECRETKEY",
		"TOKEN", "SECRET", "PASSWORD", "PASSWD", "PRIVATE_KEY", "API_KEY", "ACCESS_KEY", "SECRET_KEY",
	} {
		if upper == exact {
			return true
		}
	}
	for _, suffix := range []string{"_TOKEN", "_SECRET", "_PASSWORD", "_PASSWD", "_PRIVATE_KEY", "_API_KEY", "_ACCESS_KEY", "_SECRET_KEY"} {
		if strings.HasSuffix(upper, suffix) {
			return true
		}
	}
	return false
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

func nonce() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

func truncateOutput(output string, limit int64) string {
	if limit <= 0 || int64(len(output)) <= limit {
		return output
	}
	const marker = "\n[output truncated]\n"
	if limit <= int64(len(marker)) {
		return marker[:limit]
	}
	return output[:limit-int64(len(marker))] + marker
}
