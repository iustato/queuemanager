package queue

import (
	"encoding/json"
	"testing"
)

func TestParseWorkerStdout_Empty(t *testing.T) {
	ws := parseWorkerStdout(nil)
	if ws.Output != "" {
		t.Fatalf("expected empty Output, got %q", ws.Output)
	}
	if ws.Body != nil {
		t.Fatalf("expected nil Body, got %s", ws.Body)
	}
}

func TestParseWorkerStdout_ValidJSON_OutputOnly(t *testing.T) {
	ws := parseWorkerStdout([]byte(`{"output":"hello world"}`))
	if ws.Output != "hello world" {
		t.Fatalf("expected Output=%q, got %q", "hello world", ws.Output)
	}
	if ws.Body != nil {
		t.Fatalf("expected nil Body, got %s", ws.Body)
	}
}

func TestParseWorkerStdout_ValidJSON_BodyOnly(t *testing.T) {
	ws := parseWorkerStdout([]byte(`{"body":{"order_id":42}}`))
	if ws.Output != "" {
		t.Fatalf("expected empty Output, got %q", ws.Output)
	}
	if len(ws.Body) == 0 {
		t.Fatalf("expected non-nil Body")
	}
	var m map[string]any
	if err := json.Unmarshal(ws.Body, &m); err != nil {
		t.Fatalf("body is not valid JSON: %v", err)
	}
	if m["order_id"].(float64) != 42 {
		t.Fatalf("expected order_id=42, got %v", m["order_id"])
	}
}

func TestParseWorkerStdout_ValidJSON_BothFields(t *testing.T) {
	ws := parseWorkerStdout([]byte(`{"output":"ok","body":{"x":1}}`))
	if ws.Output != "ok" {
		t.Fatalf("expected Output=%q, got %q", "ok", ws.Output)
	}
	if len(ws.Body) == 0 {
		t.Fatalf("expected non-nil Body")
	}
}

func TestParseWorkerStdout_NonJSON_FallbackRawText(t *testing.T) {
	raw := "not json output"
	ws := parseWorkerStdout([]byte(raw))
	if ws.Output != raw {
		t.Fatalf("expected Output=%q, got %q", raw, ws.Output)
	}
	if ws.Body != nil {
		t.Fatalf("expected nil Body, got %s", ws.Body)
	}
}

func TestParseWorkerStdout_EmptyJSON(t *testing.T) {
	ws := parseWorkerStdout([]byte(`{}`))
	if ws.Output != "" {
		t.Fatalf("expected empty Output, got %q", ws.Output)
	}
	if ws.Body != nil {
		t.Fatalf("expected nil Body, got %s", ws.Body)
	}
}
