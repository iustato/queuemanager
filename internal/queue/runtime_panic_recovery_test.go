package queue

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
)

// runner that panics once for msgID="panic"
type panicOnceRunner struct {
	panicked atomic.Bool
}

func (r *panicOnceRunner) Run(ctx context.Context, cmd []string, scriptPath string, job Job) Result {
	if job.MessageGUID == "panic" && !r.panicked.Swap(true) {
		panic("boom")
	}

	return Result{
		ExitCode:   0,
		Err:        nil,
		DurationMs: 1,
	}
}

func TestRuntime_Worker_PanicRecovery_ContinuesProcessing(t *testing.T) {
	ms := newMockStore()

	rt := &Runtime{}
	rt.Cfg.Name = "q1"
	rt.Cfg.MaxRetries = 1
	rt.Cfg.RetryDelayMs = 0

	rt.Store = ms
	rt.Command = []string{"dummy"}
	rt.ScriptPath = "dummy.php"

	rt.st = &runtimeState{
		jobs:    make(chan Job, 16),
		quit:    make(chan struct{}),
		log:     zap.NewNop(),
		timeout: 200 * time.Millisecond,
	}

	now := time.Now().UnixMilli()
	exp := time.Now().Add(time.Hour).UnixMilli()

	// seed store so worker can MarkProcessing
	_, _, _, _ = ms.PutNewMessage(context.Background(), "q1", "panic", []byte(`{"x":1}`), now, exp)
	_, _, _, _ = ms.PutNewMessage(context.Background(), "q1", "ok", []byte(`{"x":2}`), now, exp)

	runner := &panicOnceRunner{}

	// start worker
	rt.st.wg.Add(1)
	go rt.workerLoop(1, runner)

	// enqueue panic job then normal job
	if err := rt.Enqueue(context.Background(), Job{Queue: "q1", MessageGUID: "panic", Attempt: 1}); err != nil {
		t.Fatalf("enqueue panic: %v", err)
	}

	if err := rt.Enqueue(context.Background(), Job{Queue: "q1", MessageGUID: "ok", Attempt: 1}); err != nil {
		t.Fatalf("enqueue ok: %v", err)
	}

	// wait until "ok" job processed
	deadline := time.Now().Add(1 * time.Second)

	for time.Now().Before(deadline) {
		ms.mu.Lock()

		seenOK := false
		for _, c := range ms.markProcessingCalls {
			if c.GUID == "ok" && c.Attempt == 1 {
				seenOK = true
				break
			}
		}

		ms.mu.Unlock()

		if seenOK {
			rt.Stop()
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	rt.Stop()
	t.Fatalf("worker died after panic, expected it to continue processing next job")
}

// compile-time check
var _ Runner = (*panicOnceRunner)(nil)