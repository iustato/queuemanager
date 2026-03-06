package storage

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestRequeueStuck_ShouldMoveExpiredProcessingToEnqueueFailed(t *testing.T) {
	dbPath := "test_requeue_stuck.db"
	_ = os.Remove(dbPath)

	store, err := Open(OpenOptions{
		FilePath: dbPath,
		// чтобы lease был короткий (не обязательно, но удобно)
		ProcessingTimeoutMs: 50,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		_ = store.Close()
		_ = os.Remove(dbPath)
	}()

	ctx := context.Background()
	msgID := "stuck_msg_1"

	_, _, err = store.PutNewMessage(ctx, "queue1", msgID, []byte(`{"text":"x"}`), "idem_stuck_1", 0, 0)
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	// MarkProcessing ставит leaseUntil = now + processingTimeoutMs и пишет индекс bProc
	if err := store.MarkProcessing(msgID, 1); err != nil {
		t.Fatalf("mark processing: %v", err)
	}

	// Ждём пока lease истечёт
	time.Sleep(80 * time.Millisecond)

	nowMs := time.Now().UnixMilli()
	n, err := store.RequeueStuck(nowMs, 100)
	if err != nil {
		t.Fatalf("requeue stuck: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected requeued=1, got %d", n)
	}

	meta, _, err := store.GetByMsgID(msgID)
	if err != nil {
		t.Fatalf("get meta: %v", err)
	}

	if meta.Status != StatusEnqueueFailed {
		t.Fatalf("expected status=%s, got %s", StatusEnqueueFailed, meta.Status)
	}
	if meta.EnqueueFailedAtMs == 0 {
		t.Fatalf("expected EnqueueFailedAtMs to be set")
	}
	if meta.EnqueueError == "" {
		t.Fatalf("expected EnqueueError to be set")
	}
	if meta.LastRequeueAtMs == 0 {
		t.Fatalf("expected LastRequeueAtMs to be set")
	}
}