package queue

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
)

type panicAfterProcessingRunner struct {
	panicked atomic.Bool
}

func (r *panicAfterProcessingRunner) Run(ctx context.Context, cmd []string, scriptPath string, job Job) Result {
	if !r.panicked.Swap(true) {
		panic("panic after processing")
	}

	return Result{
		ExitCode:   0,
		Err:        nil,
		DurationMs: 1,
	}
}

func TestRuntime_PanicAfterProcessing_MarksFailed(t *testing.T) {
	ms := newMockStore()

	rt := &Runtime{}
	rt.Cfg.Name = "q1"
	rt.Cfg.MaxRetries = 0

	rt.Store = ms
	rt.Command = []string{"dummy"}
	rt.ScriptPath = "dummy.php"

	rt.st = &runtimeState{
		jobs:    make(chan Job, 8),
		quit:    make(chan struct{}),
		log:     zap.NewNop(),
		timeout: 200 * time.Millisecond,
	}

	now := time.Now().UnixMilli()
	exp := time.Now().Add(time.Hour).UnixMilli()

	msgID := "panic-msg"

	_, _, _ = ms.PutNewMessage(
		context.Background(),
		"q1",
		msgID,
		[]byte(`{"x":1}`),
		"idem",
		now,
		exp,
	)

	runner := &panicAfterProcessingRunner{}

	rt.st.wg.Add(1)
	go rt.workerLoop(1, runner)

	if err := rt.Enqueue(context.Background(), Job{
		Queue:   "q1",
		MsgID:   msgID,
		Attempt: 1,
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	deadline := time.Now().Add(1 * time.Second)

	for time.Now().Before(deadline) {
		ms.mu.Lock()

		failed := false

		for _, c := range ms.markDoneCalls {
			if c.MsgID == msgID {
				failed = true
				break
			}
		}

		ms.mu.Unlock()

		if failed {
			rt.Stop()
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	rt.Stop()
	t.Fatalf("panic did not trigger MarkDone, message stuck in processing")
}

var _ Runner = (*panicAfterProcessingRunner)(nil)