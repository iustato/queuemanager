package queue

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"go-web-server/internal/config"
	"go-web-server/internal/storage"

	"go.uber.org/zap"
)

func TestRuntime_Integration_Panic_MarksFailedWithPanicRecovered(t *testing.T) {
	dbPath := "test_runtime_panic.db"
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
		Name:       "q1",
		Workers:    1,
		MaxQueue:   10,
		MaxRetries: 0,
		TimeoutSec: 1,
	}

	tr := NewTestRunner(ModePanic)

	rt, err := NewRuntime(
		cfg,
		st,
		zap.NewNop(),
		nil,
		[]string{"test"},
		"/dev/null",
		"", "",
		WithRunnerFactory(func() (Runner, error) { return tr, nil }),
	)
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	defer rt.Stop()

	msgID := "msg_panic"

	_, _, err = st.PutNewMessage(context.Background(), cfg.Name, msgID, []byte(`{}`), "", 0, 0)
	if err != nil {
		t.Fatalf("put new message: %v", err)
	}

	err = rt.Enqueue(context.Background(), Job{
		Queue:   cfg.Name,
		MsgID:   msgID,
		Body:    []byte(`{}`),
		Attempt: 1,
		Method:  "POST",
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	stFinal, res := waitTerminalStatus(t, st, msgID, 2*time.Second)

	if stFinal != storage.StatusFailed {
		t.Fatalf("expected failed, got %s", stFinal)
	}
	if res.ExitCode != -1 {
		t.Fatalf("expected exit=-1 for panic recovered, got %d", res.ExitCode)
	}
	if !strings.Contains(strings.ToLower(res.Err), "panic recovered") {
		t.Fatalf("expected err to contain 'panic recovered', got %q", res.Err)
	}
}