package storage

import (
	"context"
	"os"
	"testing"
	"time"
)

func openTempStore2(t *testing.T) *Store {
	t.Helper()
	dbPath := "test_store_requeue.db"
	_ = os.Remove(dbPath)

	st, err := Open(OpenOptions{FilePath: dbPath})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		_ = st.Close()
		_ = os.Remove(dbPath)
	})
	return st
}

func TestRequeueStuck_IndexesEnqueueFailed(t *testing.T) {
	st := openTempStore2(t)
	ctx := context.Background()

	queue := "q1"
	msgID := newUUIDv7ForTest(t)
	body := []byte(`{"text":"hello"}`)

	_, created, _, err := st.PutNewMessage(ctx, queue, msgID, body, time.Now().UnixMilli(), 0)
	if err != nil {
		t.Fatalf("PutNewMessage: %v", err)
	}
	if !created {
		t.Fatalf("expected created")
	}

	if err := st.MarkProcessing(msgID, 1); err != nil {
		t.Fatalf("MarkProcessing: %v", err)
	}

	nowMs := time.Now().Add(10 * time.Minute).UnixMilli()
	requeued, err := st.RequeueStuck(nowMs, 10)
	if err != nil {
		t.Fatalf("RequeueStuck: %v", err)
	}
	if requeued < 1 {
		t.Fatalf("expected requeued>=1, got %d", requeued)
	}

	// Проверяем, что попало в enq_fail индекс
	if !bucketHasMsgID(t, st.db, bEnqFail, msgID) {
		t.Fatalf("expected msgID in enq_fail index after RequeueStuck")
	}
}