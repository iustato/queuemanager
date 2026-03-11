package queue

import (
	"context"
	"os"
	"testing"
	"time"

	"go-web-server/internal/config"
	"go-web-server/internal/storage"

	"go.uber.org/zap"
)

func TestRuntime_Integration_HappyPath_MarksSucceeded(t *testing.T) {
	dbPath := "test_runtime_happy.db"
	_ = os.Remove(dbPath)

	st, err := storage.Open(storage.OpenOptions{FilePath: dbPath})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		_ = st.Close()
		_ = os.Remove(dbPath)
	}()

	cfg := config.QueueConfig{
		Name:        "q1",
		Workers:     1,
		MaxQueue:    10,
		MaxRetries:  0,
		RetryDelayMs: 0,
		TimeoutSec:  1,
	}

	tr := NewTestRunner(ModeOK)

	rt, err := NewRuntime(
		cfg,
		st,            // Store implements QueueStore
		zap.NewNop(),  // logger
		nil,           // schema not needed here
		[]string{"test"},
		"/dev/null",
		"", "",
		WithRunnerFactory(func() (Runner, error) { return tr, nil }),
	)
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	defer rt.Stop()

	msgID := "msg_1"

	_, created, _, err := st.PutNewMessage(context.Background(), cfg.Name, msgID, []byte(`{"x":1}`), 0, 0)
	if err != nil {
		t.Fatalf("put new message: %v", err)
	}
	if !created {
		t.Fatalf("expected created=true")
	}

	// Enqueue should succeed.
	err = rt.Enqueue(context.Background(), Job{
		Queue:   cfg.Name,
		MessageGUID:   msgID,
		Body:    []byte(`{"x":1}`),
		Attempt: 1,
		Method:  "POST",
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	stFinal, res := waitTerminalStatus(t, st, msgID, 2*time.Second)

	if stFinal != storage.StatusSucceeded {
		t.Fatalf("expected succeeded, got %s (exit=%d err=%v)", stFinal, res.ExitCode, res.Err)
	}
	if res.ExitCode != 0 {
		t.Fatalf("expected exit=0, got %d", res.ExitCode)
	}
	if res.FinishedAt == 0 {
		t.Fatalf("expected FinishedAt to be set")
	}
	if tr.Calls() != 1 {
		t.Fatalf("expected runner calls=1, got %d", tr.Calls())
	}
}