package queue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestRuntime_Stop_ConcurrentEnqueue_NoPanic(t *testing.T) {
	rt := &Runtime{}
	rt.Cfg.Name = "q1"
	rt.st = &runtimeState{
		jobs: make(chan Job, 64),
		quit: make(chan struct{}),
		log:  zap.NewNop(),
	}

	// worker drains jobs until closed
	rt.st.wg.Add(1)
	go func() {
		defer rt.st.wg.Done()
		for range rt.st.jobs {
			time.Sleep(1 * time.Millisecond)
		}
	}()

	var panics int64
	var ok int64
	var stopped int64
	var busy int64
	var otherErr int64

	// Make enqueue storm
	const goroutines = 20
	const perG = 200

	var wg sync.WaitGroup
	wg.Add(goroutines)

	start := make(chan struct{})
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					atomic.AddInt64(&panics, 1)
				}
			}()

			<-start

			for i := 0; i < perG; i++ {
				err := rt.Enqueue(context.Background(), Job{
					Queue:   "q1",
					MessageGUID:   "m",
					Attempt: 1,
				})
				if err == nil {
					atomic.AddInt64(&ok, 1)
					continue
				}
				switch {
				case errors.Is(err, ErrQueueStopped):
					atomic.AddInt64(&stopped, 1)
				case errors.Is(err, ErrQueueBusy):
					atomic.AddInt64(&busy, 1)
				default:
					atomic.AddInt64(&otherErr, 1)
				}
			}
		}(g)
	}

	// start them
	close(start)

	// Stop while they are enqueuing
	time.Sleep(5 * time.Millisecond)
	rt.Stop()

	wg.Wait()

	if atomic.LoadInt64(&panics) != 0 {
		t.Fatalf("unexpected panics=%d", panics)
	}
	if atomic.LoadInt64(&otherErr) != 0 {
		t.Fatalf("unexpected otherErr=%d", otherErr)
	}

	// sanity: should have either ok or stopped/busy
	if atomic.LoadInt64(&ok)+atomic.LoadInt64(&stopped)+atomic.LoadInt64(&busy) == 0 {
		t.Fatalf("no enqueues happened? ok=%d stopped=%d busy=%d", ok, stopped, busy)
	}
}