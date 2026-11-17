// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/ma"
	"github.com/volcengine/ark-runtime-go/channel/store/embedded"
)

func TestRuntimeCreateKeepsKnownRejectionAfterErrorBodyDeadline(t *testing.T) {
	for _, status := range []int{400, 403, 500} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var creates, lists atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/v3/sessions" {
					t.Errorf("unexpected path %s", r.URL.Path)
					w.WriteHeader(404)
					return
				}
				if r.Method == http.MethodGet {
					lists.Add(1)
					_, _ = io.WriteString(w, `{"data":[]}`)
					return
				}
				creates.Add(1)
				w.WriteHeader(status)
				w.(http.Flusher).Flush()
				<-r.Context().Done()
			}))
			t.Cleanup(server.Close)
			store, err := openEmbeddedFixture(filepath.Join(t.TempDir(), "state.db"), embedded.Options{NoSync: true})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			// fixture 冲刷响应头后永久阻塞，响应体读取必然超时；请求期限还需覆盖慢 runner 上的连接与发送，
			// 否则请求在到达 fixture 前就超时。
			client, err := newTestMA(server.URL, "fixture-key", ma.ClientOptions{RequestTimeout: 500 * time.Millisecond})
			require.NoError(t, err)
			// 被测期限是响应体读取与未知写宽限；单轮与总时限只需让 5xx 查账有限结束，
			// 不能在慢机器上先于首次创建到期。
			options := deadlineRuntimeOptions()
			options.UnknownWriteGrace = 100 * time.Millisecond
			options.TurnTimeout, options.LeaseTTL, options.MessageDeadline = 2*time.Second, 4*time.Second, 5*time.Second
			runtime := mustRuntime(t, store, runtimeBinding(), client, &runtimeProvider{}, options)
			harness := &rtHarness{t: t, runtime: runtime, store: store}
			harness.enqueue(runtimeMessage())
			harness.run()
			state := harness.waitSettled(core.TurnFailed, core.DeliveryDone, 15*time.Second)
			require.Equal(t, int32(1), creates.Load(), "已知拒绝和未知5xx都不能盲目重发创建")
			if status < 500 {
				require.Equal(t, "create_rejected", state.turn.Outcome.ErrorKind)
				require.Zero(t, lists.Load(), "已知拒绝不应进入未知创建查账")
			} else {
				require.Equal(t, "create_unconfirmed", state.turn.Outcome.ErrorKind)
				require.Positive(t, lists.Load(), "5xx继续沿用unknown查账策略")
			}
		})
	}
}
