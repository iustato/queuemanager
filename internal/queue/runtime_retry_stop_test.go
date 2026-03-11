package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/zap"
)

// runner that always fails (to trigger retry scheduling)
type alwaysFailRunner struct{}

func (r *alwaysFailRunner) Run(ctx context.Context, cmd []string, scriptPath string, job Job) Result {
	return Result{
		ExitCode:   1,
		Err:        errors.New("fail"),
		DurationMs: 1,
	}
}

func TestRuntime_RetryGoroutine_AfterStop_DoesNotReenqueue(t *testing.T) {
	ms := newMockStore()

	rt := &Runtime{}
	rt.Cfg.Name = "q1"
	rt.Cfg.MaxRetries = 2
	rt.Cfg.RetryDelayMs = 200 // успеваем вызвать Stop() до ретрая

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
	msgID := "m1"

	_, _, _, _ = ms.PutNewMessage(context.Background(), "q1", msgID, []byte(`{"x":1}`), now, exp)

	// стартуем 1 воркер
	runner := &alwaysFailRunner{}
	rt.st.wg.Add(1)
	go rt.workerLoop(1, runner)

	// enqueue first attempt
	if err := rt.Enqueue(context.Background(), Job{Queue: "q1", MessageGUID: msgID, Attempt: 1}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// дождаться, что attempt=1 реально начался
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		ms.mu.Lock()
		seen1 := false
		for _, c := range ms.markProcessingCalls {
			if c.GUID == msgID && c.Attempt == 1 {
				seen1 = true
				break
			}
		}
		ms.mu.Unlock()

		if seen1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// стопаем ДО истечения retry delay
	rt.Stop()

	// ждём дольше retry delay
	time.Sleep(350 * time.Millisecond)

	// attempt=2 не должен начаться после stop
	ms.mu.Lock()
	defer ms.mu.Unlock()

	for _, c := range ms.markProcessingCalls {
		if c.GUID == msgID && c.Attempt >= 2 {
			t.Fatalf("retry attempt started after Stop(): attempt=%d", c.Attempt)
		}
	}
}

var _ Runner = (*alwaysFailRunner)(nil)