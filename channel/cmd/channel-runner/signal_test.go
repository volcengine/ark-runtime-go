// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAdminSignalUsesCancellationCleanup(t *testing.T) {
	if path := os.Getenv("CHANNEL_RUNNER_SIGNAL_HELPER"); path != "" {
		code := execute([]string{"prune", "--config", path}, os.Stdout, os.Stderr)
		fmt.Printf("admin returned code=%d\n", code)
		return
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { require.NoError(t, listener.Close()) }()
	cfg := strings.Replace(fixtureConfig, `"database_path":"state.db",`, `"store":{"driver":"mysql","dsn_ref":"env:SIGNAL_TEST_DSN"},`, 1)
	path := filepath.Join(t.TempDir(), "config.json")
	require.NoError(t, os.WriteFile(path, []byte(cfg), 0600))
	child := exec.Command(os.Args[0], "-test.run=^TestAdminSignalUsesCancellationCleanup$", "-test.count=1")
	child.Env = append(os.Environ(), "CHANNEL_RUNNER_SIGNAL_HELPER="+path, "SIGNAL_TEST_DSN=user:secret@tcp("+listener.Addr().String()+")/channel?timeout=5s&readTimeout=5s")
	var output bytes.Buffer
	child.Stdout, child.Stderr = &output, &output
	require.NoError(t, child.Start())
	defer func() { _ = child.Process.Kill() }()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	var conn net.Conn
	select {
	case conn = <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("child did not begin database initialization")
	}
	defer func() { _ = conn.Close() }()
	require.NoError(t, child.Process.Signal(os.Interrupt))
	done := make(chan error, 1)
	go func() { done <- child.Wait() }()
	select {
	case err := <-done:
		require.NoError(t, err, output.String())
	case <-time.After(2 * time.Second):
		_ = child.Process.Kill()
		<-done
		t.Fatal("admin did not cancel promptly after SIGINT")
	}
	require.Contains(t, output.String(), "admin returned code=1", "SIGINT should return through CLI cleanup instead of default OS termination")
}
