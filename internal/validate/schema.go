package validate

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

// CompiledSchema — компилируется ОДИН раз при старте

type CompiledSchema struct {
	schema *jsonschema.Schema
}

// LoadSchemaFromFile загружает и компилирует JSON Schema из файла.
//
//   - чтение файла
//   - парсинг JSON Schema
//   - компиляция (проверка ссылок, draft и т.д.)
//
// Если здесь ошибка — сервис НЕ должен стартовать.
func LoadSchemaFromFile(path string) (*CompiledSchema, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve schema path: %w", err)
	}

	// Читаем файл схемы целиком
	data, err := os.ReadFile(absPath)
	if err != nil {
		return nil, fmt.Errorf("read schema file %s: %w", absPath, err)
	}

	// компилятор JSON Schema
	compiler := jsonschema.NewCompiler()
	compiler.Draft = jsonschema.Draft2020

	// jsonschema.Compiler работает с ресурсами по URL

	schemaURL := "file://" + filepath.ToSlash(absPath)

	if err := compiler.AddResource(schemaURL, strings.NewReader(string(data))); err != nil {
		return nil, fmt.Errorf("add schema resource %s: %w", absPath, err)
	}

	// Компиляция схемы:
	//  - проверка синтаксиса
	//  - проверка ссылок ($ref)
	//  - подготовка валидатора
	s, err := compiler.Compile(schemaURL)
	if err != nil {
		return nil, fmt.Errorf("compile schema %s: %w", absPath, err)
	}

	return &CompiledSchema{schema: s}, nil
}

// ValidateJSON валидирует произвольные данные (decoded JSON)
// по уже скомпилированной JSON Schema.
//
// payload должен быть результатом json.Unmarshal (map[string]any, []any и т.п.).
func (cs *CompiledSchema) ValidateJSON(payload any) error {
	if cs == nil || cs.schema == nil {
		return fmt.Errorf("schema is not initialized")
	}

	// Validate возвращает детализированную ошибку,
	// которую можно логировать или отдавать клиенту (осторожно).
	return cs.schema.Validate(payload)
}
