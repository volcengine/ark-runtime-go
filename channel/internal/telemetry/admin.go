// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package observability

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

// StatusFunc 返回可公开的运行状态快照（只含 ID 与状态，不含正文或 Secret）。
type StatusFunc func(context.Context) (any, error)

// AdminServer 提供 /healthz、/readyz、/metrics 与 /status。只允许监听回环地址：远程访问需
// 由部署方自行加认证代理。
type AdminServer struct {
	Addr    string
	Metrics *Metrics
	Status  StatusFunc
	ready   atomic.Bool
}

// SetReady 标记 Runtime 已完成装配并开始处理消息。
func (s *AdminServer) SetReady(ready bool) { s.ready.Store(ready) }

// ValidateLoopback 校验地址只绑定回环接口。
func ValidateLoopback(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || port == "" {
		return errors.New("admin address must be host:port")
	}
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return errors.New("admin address must bind a loopback interface")
	}
	return nil
}

// Handler 返回管理路由；只接受 GET。
func (s *AdminServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if !s.ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not ready\n"))
			return
		}
		_, _ = w.Write([]byte("ready\n"))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || s.Metrics == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_ = s.Metrics.WriteText(w)
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || s.Status == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		status, err := s.Status(ctx)
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(status)
	})
	return mux
}

// Serve 监听并服务到 ctx 取消；监听失败立即返回错误。
func (s *AdminServer) Serve(ctx context.Context) error {
	if err := ValidateLoopback(s.Addr); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return errors.New("admin listen failed")
	}
	server := &http.Server{Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
		<-done
		return nil
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return errors.New("admin server stopped")
	}
}
