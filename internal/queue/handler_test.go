package queue

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go-web-server/internal/config"
	"go-web-server/internal/storage"

	"go.uber.org/zap"
)

func TestHandleNewMessage_Rejected_BodyTooLarge(t *testing.T) {
	m := NewManager(zap.NewNop(), t.TempDir(), storage.OpenOptions{})

	// Чтобы limit был маленький: создаём runtime с MaxSize=2
	m.queues["q1"] = &Runtime{
		Cfg: config.QueueConfig{
			Name:    "q1",
			MaxSize: 2,
		},
	}

	req := httptest.NewRequest(http.MethodPost, "http://x", strings.NewReader("abcd")) // 4 bytes > 2
	w := httptest.NewRecorder()

	m.HandleNewMessage("q1", w, req)

	if w.Code == http.StatusAccepted {
		t.Fatalf("expected non-202 status, got %d body=%q", w.Code, w.Body.String())
	}
	if w.Body.Len() == 0 {
		t.Fatalf("expected error body")
	}
}

func TestHandleNewMessage_Rejected_ByPushError(t *testing.T) {
	m := NewManager(zap.NewNop(), t.TempDir(), storage.OpenOptions{})

	// GET почти наверняка будет отвергнут Push-валидацией (а readLimitedBody пройдёт)
	req := httptest.NewRequest(http.MethodGet, "http://x?x=1", strings.NewReader("ok"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	m.HandleNewMessage("q1", w, req)

	if w.Code == http.StatusAccepted {
		t.Fatalf("expected non-202 status, got %d body=%q", w.Code, w.Body.String())
	}
	if w.Body.Len() == 0 {
		t.Fatalf("expected error body")
	}
}