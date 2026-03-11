package queue

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
	"errors"

	"go.uber.org/zap"
)

func TestRuntime_Stop_Graceful_DrainsJobs(t *testing.T) {
	rt := &Runtime{}
	rt.Cfg.Name = "q1"
	rt.st = &runtimeState{
		jobs: make(chan Job, 10),
		quit: make(chan struct{}),
		log:  zap.NewNop(),
	}

	// ВОРКЕР: range jobs (ключ к graceful shutdown)
	var processed int64
	rt.st.wg.Add(1)
	go func() {
		defer rt.st.wg.Done()
		for range rt.st.jobs {
			atomic.AddInt64(&processed, 1)
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// Кладём 2 задачи
	if err := rt.Enqueue(context.Background(), Job{Queue: "q1", MessageGUID: "m1", Attempt: 1}); err != nil {
		t.Fatalf("enqueue1: %v", err)
	}
	if err := rt.Enqueue(context.Background(), Job{Queue: "q1", MessageGUID: "m2", Attempt: 1}); err != nil {
		t.Fatalf("enqueue2: %v", err)
	}

	// Stop() должен завершиться за разумное время (иначе jobs не закрывается / воркер не выходит)
	done := make(chan struct{})
	go func() {
		rt.Stop()
		close(done)
	}()

	select {
	case <-done:
		// ok
	case <-time.After(300 * time.Millisecond):
		t.Fatalf("Stop() hung: jobs channel likely not closed, worker range cannot finish")
	}

	if got := atomic.LoadInt64(&processed); got != 2 {
		t.Fatalf("expected processed=2, got %d", got)
	}
}

func TestRuntime_Enqueue_AfterStop_ReturnsQueueStopped(t *testing.T) {
	rt := &Runtime{}
	rt.Cfg.Name = "q1"
	rt.st = &runtimeState{
		jobs: make(chan Job, 1),
		quit: make(chan struct{}),
		log:  zap.NewNop(),
	}

	rt.Stop()

	err := rt.Enqueue(context.Background(), Job{Queue: "q1", MessageGUID: "m1", Attempt: 1})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !errors.Is(err, ErrQueueStopped) {
		t.Fatalf("expected ErrQueueStopped, got %T: %v", err, err)
	}
}