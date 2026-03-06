package validate

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeTempSchema(t *testing.T, dir string, schema string) string {
	t.Helper()
	p := filepath.Join(dir, "schema.json")
	if err := os.WriteFile(p, []byte(schema), 0o600); err != nil {
		t.Fatalf("write schema: %v", err)
	}
	return p
}

func TestValidateBytes_InvalidJSON(t *testing.T) {
	dir := t.TempDir()

	schemaPath := writeTempSchema(t, dir, `{
	  "$schema":"https://json-schema.org/draft/2020-12/schema",
	  "type":"object",
	  "properties":{"text":{"type":"string"}},
	  "required":["text"]
	}`)

	cs, err := LoadSchemaFromFile(schemaPath)
	if err != nil {
		t.Fatalf("LoadSchemaFromFile: %v", err)
	}

	// битый JSON
	body := []byte(`{"text":`)

	err = cs.ValidateBytes(body)
	if err == nil {
		t.Fatalf("expected error")
	}
	if !strings.Contains(err.Error(), "invalid json") && !strings.Contains(err.Error(), "InvalidJSON") {
		// твой ErrInvalidJSON оборачивается, поэтому достаточно наличия маркера
		t.Fatalf("expected invalid json error, got: %v", err)
	}
}

func TestValidateBytes_SchemaViolation(t *testing.T) {
	dir := t.TempDir()

	schemaPath := writeTempSchema(t, dir, `{
	  "$schema":"https://json-schema.org/draft/2020-12/schema",
	  "type":"object",
	  "properties":{"text":{"type":"string"}},
	  "required":["text"]
	}`)

	cs, err := LoadSchemaFromFile(schemaPath)
	if err != nil {
		t.Fatalf("LoadSchemaFromFile: %v", err)
	}

	// валидный JSON, но не проходит schema (text должен быть string)
	body := []byte(`{"text": 123}`)

	err = cs.ValidateBytes(body)
	if err == nil {
		t.Fatalf("expected schema error")
	}

	// важно: это НЕ invalid json
	if strings.Contains(err.Error(), "invalid json") {
		t.Fatalf("expected schema validation error, got invalid json: %v", err)
	}
}

func TestValidateBytes_OK(t *testing.T) {
	dir := t.TempDir()

	schemaPath := writeTempSchema(t, dir, `{
	  "$schema":"https://json-schema.org/draft/2020-12/schema",
	  "type":"object",
	  "properties":{"text":{"type":"string"}},
	  "required":["text"]
	}`)

	cs, err := LoadSchemaFromFile(schemaPath)
	if err != nil {
		t.Fatalf("LoadSchemaFromFile: %v", err)
	}

	body := []byte(`{"text":"hello"}`)
	if err := cs.ValidateBytes(body); err != nil {
		t.Fatalf("expected ok, got: %v", err)
	}
}