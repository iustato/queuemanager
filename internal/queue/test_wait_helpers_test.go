package queue

import (
	"testing"
	"time"

	"go-web-server/internal/storage"
)

func waitTerminalStatus(t *testing.T, store QueueStore, msgID string, timeout time.Duration) (storage.Status, storage.Result) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		st, res, err := store.GetStatusAndResult(msgID)
		if err == nil && (st == storage.StatusSucceeded || st == storage.StatusFailed) {
			return st, res
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting terminal status for msg_id=%s", msgID)
	return "", storage.Result{}
}