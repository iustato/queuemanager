package storage

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestGC_ShouldDeleteExpiredMessages(t *testing.T) {
	dbPath := "test_gc_expired.db"
	_ = os.Remove(dbPath)

	store, err := Open(OpenOptions{FilePath: dbPath})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		_ = store.Close()
		_ = os.Remove(dbPath)
	}()

	ctx := context.Background()

	msgID := "expired_msg_1"
	expiresAt := time.Now().Add(-1 * time.Minute).UnixMilli()

	_, _, err = store.PutNewMessage(
		ctx,
		"queue1",
		msgID,
		[]byte(`{"text":"old"}`),
		"idem_gc_1",
		0,
		expiresAt,
	)
	if err != nil {
		t.Fatalf("put message: %v", err)
	}

	deleted, err := store.GC(time.Now().UnixMilli(), 100)
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if len(deleted) < 1 {
		t.Fatalf("expected at least 1 deleted message, got %d", len(deleted))
	}

	_, _, err = store.GetByMsgID(msgID)
	if err == nil {
		t.Fatalf("expected message to be deleted")
	}
}