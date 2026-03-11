package queue

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go-web-server/internal/config"
	"go-web-server/internal/storage"

	"go.uber.org/zap"
)

func TestHandleNewMessage_Rejected_BodyTooLarge(t *testing.T) {
	m := NewManager(zap.NewNop(), t.TempDir(), storage.OpenOptions{}, "", false, "")

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
	m := NewManager(zap.NewNop(), t.TempDir(), storage.OpenOptions{}, "", false, "")

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

func TestHandleGetSchema_OK(t *testing.T) {
	m := NewManager(zap.NewNop(), t.TempDir(), storage.OpenOptions{}, "", false, "")

	rawSchema := json.RawMessage(`{"type":"object","required":["text"],"properties":{"text":{"type":"string"}}}`)
	m.queues["q1"] = &Runtime{
		Cfg:        config.QueueConfig{Name: "q1"},
		SchemaJSON: rawSchema,
	}

	req := httptest.NewRequest(http.MethodGet, "/q1/schema", nil)
	w := httptest.NewRecorder()

	m.HandleGetSchema("q1", w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%q", w.Code, w.Body.String())
	}

	var resp struct {
		OK   bool `json:"ok"`
		Data struct {
			Queue  string          `json:"queue"`
			Schema json.RawMessage `json:"schema"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !resp.OK {
		t.Fatalf("expected ok=true")
	}
	if resp.Data.Queue != "q1" {
		t.Fatalf("expected queue=q1, got %q", resp.Data.Queue)
	}
	if len(resp.Data.Schema) == 0 {
		t.Fatalf("expected non-empty schema")
	}

	var schemaMap map[string]any
	if err := json.Unmarshal(resp.Data.Schema, &schemaMap); err != nil {
		t.Fatalf("schema is not valid JSON: %v", err)
	}
	if schemaMap["type"] != "object" {
		t.Fatalf("expected schema type=object, got %v", schemaMap["type"])
	}
}

func TestHandleGetSchema_QueueNotFound(t *testing.T) {
	m := NewManager(zap.NewNop(), t.TempDir(), storage.OpenOptions{}, "", false, "")

	req := httptest.NewRequest(http.MethodGet, "/unknown/schema", nil)
	w := httptest.NewRecorder()

	m.HandleGetSchema("unknown", w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestHandleGetSchema_NoSchema(t *testing.T) {
	m := NewManager(zap.NewNop(), t.TempDir(), storage.OpenOptions{}, "", false, "")

	m.queues["q1"] = &Runtime{
		Cfg: config.QueueConfig{Name: "q1"},
	}

	req := httptest.NewRequest(http.MethodGet, "/q1/schema", nil)
	w := httptest.NewRecorder()

	m.HandleGetSchema("q1", w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}