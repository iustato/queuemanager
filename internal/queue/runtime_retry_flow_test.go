package queue

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
)

// fake runner: fails first time for a given message_guid, succeeds on second run.
type failOnceRunner struct {
	seen map[string]*int64
}

func newFailOnceRunner() *failOnceRunner {
	return &failOnceRunner{seen: make(map[string]*int64)}
}

func (r *failOnceRunner) Run(ctx context.Context, cmd []string, scriptPath string, job Job) Result {
	ptr, ok := r.seen[job.MessageGUID]
	if !ok {
		var x int64
		ptr = &x
		r.seen[job.MessageGUID] = ptr
	}
	n := atomic.AddInt64(ptr, 1)

	if n == 1 {
		return Result{
			ExitCode:   1,
			Err:        errors.New("fail once"),
			Stdout:     nil,
			Stderr:     []byte("boom"),
			DurationMs: 1,
		}
	}
	return Result{
		ExitCode:   0,
		Err:        nil,
		Stdout:     []byte("ok"),
		Stderr:     nil,
		DurationMs: 1,
	}
}

func TestRuntime_RetryFlow_PersistsAndReenqueues(t *testing.T) {
	ms := newMockStore()

	rt := &Runtime{}
	rt.Cfg.Name = "q1"
	rt.Cfg.MaxRetries = 2
	rt.Cfg.RetryDelayMs = 10

	rt.Store = ms
	rt.Command = []string{"dummy"}
	rt.ScriptPath = "dummy.php"

	rt.st = &runtimeState{
		jobs:    make(chan Job, 16),
		quit:    make(chan struct{}),
		log:     zap.NewNop(),
		timeout: 200 * time.Millisecond,
	}

	msgID := "m1"
	_, _, _, _ = ms.PutNewMessage(
		context.Background(),
		"q1",
		msgID,
		[]byte(`{"x":1}`),
		time.Now().UnixMilli(),
		time.Now().Add(time.Hour).UnixMilli(),
	)

	// start 1 worker
	runner := newFailOnceRunner()
	rt.st.wg.Add(1)
	go rt.workerLoop(1, runner)

	// enqueue first attempt
	if err := rt.Enqueue(context.Background(), Job{Queue: "q1", MessageGUID: msgID, Attempt: 1}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// wait until:
	// - RequeueForRetry called
	// - MarkProcessing called with attempt=2 (meaning re-enqueue happened and worker picked it up)
	deadline := time.Now().Add(1 * time.Second)

	for time.Now().Before(deadline) {
		ms.mu.Lock()

		requeued := len(ms.requeueForRetryCalls) > 0

		seenAttempt2 := false
		for _, c := range ms.markProcessingCalls {
			if c.GUID == msgID && c.Attempt >= 2 {
				seenAttempt2 = true
				break
			}
		}

		ms.mu.Unlock()

		if requeued && seenAttempt2 {
			rt.Stop()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	t.Fatalf("retry flow not observed in time: requeueCalls=%d markProcessingCalls=%d",
		len(ms.requeueForRetryCalls), len(ms.markProcessingCalls))
}