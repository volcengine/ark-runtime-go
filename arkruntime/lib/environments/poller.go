// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0
package environments

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/internal/selfhostedlog"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/environment"
	selfhosted "github.com/volcengine/ark-runtime-go/arkruntime/selfhosted"
)

const (
	defaultPollBlockMS = 999
	pollBackoffCap     = 60 * time.Second
	stopTimeout        = 10 * time.Second
)

// WorkPollerOptions 配置 WorkPoller。
type WorkPollerOptions struct {
	EnvironmentID      string
	WorkerID           string
	BlockMS            int
	ReclaimOlderThanMS int
	Drain              bool
	Logger             *log.Logger
}

// WorkPoller 负责从 environment work queue 里 poll 并 ack work。
type WorkPoller struct {
	ctx         context.Context
	api         selfhosted.API
	opts        WorkPollerOptions
	logger      *selfhostedlog.Logger
	current     *selfhosted.WorkItem
	err         error
	pendingStop func()
	failures    int
	discards    int
	closed      bool
}

// NewWorkPoller 创建 work poller。
func NewWorkPoller(ctx context.Context, api selfhosted.API, opts WorkPollerOptions) *WorkPoller {
	logger := selfhostedlog.New(opts.Logger)
	if opts.WorkerID == "" {
		opts.WorkerID = defaultWorkerID()
	}
	p := &WorkPoller{
		ctx:    ctx,
		api:    api,
		opts:   opts,
		logger: logger.With("component", "work-poller", "environment_id", opts.EnvironmentID),
	}
	if opts.EnvironmentID == "" {
		p.err = errors.New("environments: EnvironmentID is required")
	}
	if api == nil {
		p.err = errors.New("environments: API is required")
	}
	return p
}

// NewWorkPollerForClient 创建绑定到 arkruntime.Client 的 work poller。
func NewWorkPollerForClient(ctx context.Context, client *arkruntime.Client, opts WorkPollerOptions) *WorkPoller {
	return NewWorkPoller(ctx, selfhosted.NewClientAPI(client), opts)
}

// Next 推进到下一条已 ack 的 work。
func (p *WorkPoller) Next() bool {
	p.runPendingStop()
	if p.err != nil || p.closed {
		return false
	}
	for {
		if p.ctx.Err() != nil {
			return false
		}
		req := selfhosted.PollWorkRequest{
			EnvironmentID:      p.opts.EnvironmentID,
			WorkerID:           p.opts.WorkerID,
			MaxItems:           1,
			ReclaimOlderThanMS: p.opts.ReclaimOlderThanMS,
		}
		if p.opts.BlockMS >= 0 {
			req.BlockMS = defaultPollBlockMS
			if p.opts.BlockMS > 0 {
				req.BlockMS = p.opts.BlockMS
			}
		}
		item, err := p.api.PollWork(p.ctx, req)
		if err != nil {
			if p.ctx.Err() != nil {
				return false
			}
			if isFatal4xx(err) {
				p.err = fmt.Errorf("environments: poll work: %w", err)
				return false
			}
			p.failures++
			sleepFor := jitter(backoff(p.failures)/2, backoff(p.failures))
			p.logger.Warn("poll work failed", "err", err, "sleep", sleepFor)
			sleep(p.ctx, sleepFor)
			continue
		}
		p.failures = 0
		if item == nil || item.ID == "" {
			if p.opts.Drain {
				return false
			}
			sleep(p.ctx, jitter(time.Second, 3*time.Second))
			continue
		}
		if item.EnvironmentID == "" {
			item.EnvironmentID = p.opts.EnvironmentID
		}
		if item.SessionIDValue() == "" {
			p.discardInvalidWork(*item, "work item does not contain session id")
			continue
		}
		if err := p.api.AckWork(p.ctx, selfhosted.AckWorkRequest{
			EnvironmentID: item.EnvironmentID,
			WorkID:        item.ID,
			WorkerID:      environment.NewOptString(p.opts.WorkerID),
		}); err != nil {
			p.logger.Warn("ack work failed", "work_id", item.ID, "err", err)
			// ACK 是 queued -> starting 的竞争操作。失败时无法证明当前
			// worker 拥有该 work，不能 force stop，否则可能终止竞争胜者。
			if isResolvedStatus(err) {
				continue
			}
			if isFatal4xx(err) {
				p.err = fmt.Errorf("environments: ack work: %w", err)
				return false
			}
			p.backoffDiscard(*item)
			continue
		}
		p.current = item
		p.pendingStop = p.makeStopClosure(*item)
		p.discards = 0
		p.logger.Info("claimed work", "work_id", item.ID, "session_id", item.SessionIDValue())
		return true
	}
}

// Current 返回最近一次 Next claim 到的 work。
func (p *WorkPoller) Current() *selfhosted.WorkItem {
	return p.current
}

// Err 返回最近一次 Next 的错误；不可恢复错误会阻止后续 Next。
func (p *WorkPoller) Err() error {
	return p.err
}

// Close 停止 poller，并对最后 yield 的 work 发送 StopWork。
func (p *WorkPoller) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true
	p.runPendingStop()
	return nil
}

func (p *WorkPoller) runPendingStop() {
	if p.pendingStop == nil {
		return
	}
	stop := p.pendingStop
	p.pendingStop = nil
	p.current = nil
	stop()
}

func (p *WorkPoller) makeStopClosure(item selfhosted.WorkItem) func() {
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), stopTimeout)
		defer cancel()
		if err := p.api.StopWork(ctx, selfhosted.StopWorkRequest{
			EnvironmentID: item.EnvironmentID,
			WorkID:        item.ID,
		}); err != nil && !isResolvedStatus(err) {
			p.logger.Warn("stop work failed", "work_id", item.ID, "err", err)
		}
	}
}

func (p *WorkPoller) discardInvalidWork(item selfhosted.WorkItem, _ string) {
	if item.ID == "" {
		return
	}
	if err := p.api.AckWork(p.ctx, selfhosted.AckWorkRequest{
		EnvironmentID: item.EnvironmentID,
		WorkID:        item.ID,
		WorkerID:      environment.NewOptString(p.opts.WorkerID),
	}); err != nil {
		p.logger.Warn("ack invalid work failed", "work_id", item.ID, "err", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.api.StopWork(ctx, selfhosted.StopWorkRequest{
		EnvironmentID: item.EnvironmentID,
		WorkID:        item.ID,
		Force:         environment.NewOptBool(true),
	}); err != nil && !isResolvedStatus(err) {
		p.logger.Warn("stop invalid work failed", "work_id", item.ID, "err", err)
	}
	p.backoffDiscard(item)
}

func (p *WorkPoller) backoffDiscard(item selfhosted.WorkItem) {
	p.discards++
	sleepFor := jitter(backoff(p.discards)/2, backoff(p.discards))
	p.logger.Warn("backing off after unprocessable work item", "work_id", item.ID, "sleep", sleepFor)
	sleep(p.ctx, sleepFor)
}

func isResolvedStatus(err error) bool {
	return selfhosted.IsStatus(err, http.StatusNotFound) ||
		selfhosted.IsStatus(err, http.StatusConflict) ||
		selfhosted.IsStatus(err, http.StatusPreconditionFailed)
}

func sleep(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func isFatal4xx(err error) bool {
	var apiErr *selfhosted.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.StatusCode >= 400 && apiErr.StatusCode < 500 &&
		apiErr.StatusCode != http.StatusRequestTimeout &&
		apiErr.StatusCode != http.StatusConflict &&
		apiErr.StatusCode != http.StatusTooManyRequests
}

func backoff(n int) time.Duration {
	if n <= 0 {
		return time.Second
	}
	if n > 6 {
		return pollBackoffCap
	}
	if d := time.Duration(1<<n) * time.Second; d <= pollBackoffCap {
		return d
	}
	return pollBackoffCap
}

func jitter(low, high time.Duration) time.Duration {
	if high <= low {
		return low
	}
	return low + time.Duration(rand.Int63n(int64(high-low)))
}

func defaultWorkerID() string {
	var b [8]byte
	if _, err := crand.Read(b[:]); err != nil {
		return fmt.Sprintf("ark-self-hosted-worker-%d", time.Now().UnixNano())
	}
	suffix := hex.EncodeToString(b[:])
	host, err := os.Hostname()
	if err != nil || host == "" {
		return "ark-self-hosted-worker-" + suffix
	}
	return host + "-" + suffix
}

var _ io.Closer = (*WorkPoller)(nil)
