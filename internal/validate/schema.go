package validate

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

type CompiledSchema struct {
	schema *jsonschema.Schema
}

var ErrInvalidJSON = errors.New("invalid json")

func LoadSchemaFromFile(path string) (*CompiledSchema, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve schema path: %w", err)
	}

	data, err := os.ReadFile(absPath)
	if err != nil {
		return nil, fmt.Errorf("read schema file %s: %w", absPath, err)
	}

	compiler := jsonschema.NewCompiler()
	compiler.Draft = jsonschema.Draft2020

	schemaURL := "file://" + filepath.ToSlash(absPath)

	if err := compiler.AddResource(schemaURL, strings.NewReader(string(data))); err != nil {
		return nil, fmt.Errorf("add schema resource %s: %w", absPath, err)
	}

	s, err := compiler.Compile(schemaURL)
	if err != nil {
		return nil, fmt.Errorf("compile schema %s: %w", absPath, err)
	}

	return &CompiledSchema{schema: s}, nil
}

// CompileSchemaFromBytes compiles a JSON Schema from raw bytes (for admin API).
func CompileSchemaFromBytes(data []byte) (*CompiledSchema, error) {
	compiler := jsonschema.NewCompiler()
	compiler.Draft = jsonschema.Draft2020

	const schemaURL = "mem://inline-schema.json"
	if err := compiler.AddResource(schemaURL, strings.NewReader(string(data))); err != nil {
		return nil, fmt.Errorf("add inline schema resource: %w", err)
	}

	s, err := compiler.Compile(schemaURL)
	if err != nil {
		return nil, fmt.Errorf("compile inline schema: %w", err)
	}

	return &CompiledSchema{schema: s}, nil
}

func (cs *CompiledSchema) ValidateJSON(payload any) error {
	if cs == nil || cs.schema == nil {
		return fmt.Errorf("schema is not initialized")
	}
	return cs.schema.Validate(payload)
}

// ValidateBytes: минимизируем аллокации насколько возможно в рамках jsonschema/v5
func (cs *CompiledSchema) ValidateBytes(body []byte) error {
	if cs == nil || cs.schema == nil {
		return fmt.Errorf("schema is not initialized")
	}

	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()

	var v any
	if err := dec.Decode(&v); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidJSON, err)
	}

	// защита от "хвоста" после JSON
	if dec.More() {
		return fmt.Errorf("%w: trailing data", ErrInvalidJSON)
	}
	// ещё более строгая проверка хвоста
	if tok, err := dec.Token(); err == nil || tok != nil {
		// если Token() вернул что-то — значит есть хвост
		// (на практике можно упростить, но так безопаснее)
		return fmt.Errorf("%w: trailing data", ErrInvalidJSON)
	}

	return cs.schema.Validate(v)
}