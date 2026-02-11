package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// LoadQueueConfigs загружает конфигурации очередей из директории.
//
// Принцип:
//   - читаем ВСЕ *.yaml / *.yml файлы
//   - каждая конфигурация должна быть валидна
//   - при любой ошибке сервис не стартует (fail-fast)
func LoadQueueConfigs(dir string) ([]QueueConfig, error) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("abs config dir: %w", err)
	}
	dir = absDir

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read config dir: %w", err)
	}

	var result []QueueConfig //возвращаем срез всех валидных конфигов

	// seen используется для защиты от двух файлов,
	seen := make(map[string]struct{})

	for _, e := range entries {
		if e.IsDir() || !isYAML(e.Name()) {
			continue
		}

		path := filepath.Join(dir, e.Name())

		// 1. Читаем и парсим YAML в строгую структуру
		cfg, err := parseConfig(path)
		if err != nil {
			return nil, err
		}

		// 2. Нормализуем конфиг:
		//    - имя очереди (если не задано)
		//    - пути к файлам (schema)
		if err := normalizeConfig(&cfg, e.Name(), dir); err != nil {
			return nil, err
		}

		// 3. Валидируем сам YAML-конфиг
		if err := validateConfig(cfg); err != nil {
			return nil, err
		}

		// 4. Проверяем, что имя очереди уникально
		if _, ok := seen[cfg.Name]; ok {
			return nil, fmt.Errorf("duplicate queue name %q", cfg.Name)
		}
		seen[cfg.Name] = struct{}{}

		result = append(result, cfg)
	}

	// Если конфигов нет — это почти всегда ошибка деплоя
	if len(result) == 0 {
		return nil, fmt.Errorf("no queue configs found in %s", dir)
	}

	return result, nil
}

// isYAML фильтрует только поддерживаемые форматы конфигов.
// Расширения проверяются намеренно строго.
func isYAML(name string) bool {
	ext := strings.ToLower(filepath.Ext(name))
	return ext == ".yaml" || ext == ".yml"
}

// parseConfig отвечает ТОЛЬКО за чтение файла и YAML → struct.
// Здесь нет логики нормализации и валидации — принцип single responsibility.
func parseConfig(path string) (QueueConfig, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return QueueConfig{}, fmt.Errorf("read %s: %w", path, err)
	}

	var cfg QueueConfig
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return QueueConfig{}, fmt.Errorf("parse %s: %w", path, err)
	}

	return cfg, nil
}

// normalizeConfig приводит конфигурацию к каноническому виду.

func normalizeConfig(cfg *QueueConfig, fileName, baseDir string) error {
	// Если имя очереди не задано явно —
	// используем имя файла без расширения.
	if cfg.Name == "" {
		cfg.Name = strings.TrimSuffix(fileName, filepath.Ext(fileName))
	}

	// Пути в конфиге разрешаем относительно директории конфигов,
	// чтобы сервис можно было запускать из любого cwd.
	if !filepath.IsAbs(cfg.SchemaFile) {
		cfg.SchemaFile = filepath.Join(baseDir, cfg.SchemaFile)
	}

	if !filepath.IsAbs(cfg.Script) {
		cfg.Script = filepath.Join(baseDir, cfg.Script)
	}

		// ---- defaults for idempotency/storage ----
	if strings.TrimSpace(cfg.Idempotency.AcceptMaxAge) == "" {
		cfg.Idempotency.AcceptMaxAge = "30d"
	}
	if strings.TrimSpace(cfg.Idempotency.RetentionMin) == "" {
		cfg.Idempotency.RetentionMin = cfg.Idempotency.AcceptMaxAge
	}

	// если не forever и retention не задан — храним минимум окно дедупа
	if !cfg.Storage.Forever && strings.TrimSpace(cfg.Storage.Retention) == "" {
		cfg.Storage.Retention = cfg.Idempotency.RetentionMin
	}

	// дефолты GC (можно считать 0 => дефолт)
	if cfg.Storage.GCIntervalSec == 0 {
		cfg.Storage.GCIntervalSec = 60
	}
	if cfg.Storage.GCMaxDeletes == 0 {
		cfg.Storage.GCMaxDeletes = 1000
	}

	return nil
}

// validateConfig валидирует бизнес-инварианты YAML-конфига.
//
// Это НЕ проверка JSON Schema сообщений,
// а защита от некорректной конфигурации сервиса.
func validateConfig(cfg QueueConfig) error {
	if cfg.Name == "" {
		return fmt.Errorf("queue name is empty")
	}
	if cfg.SchemaFile == "" {
		return fmt.Errorf("schema_file is required")
	}
	if cfg.Workers <= 0 {
		return fmt.Errorf("workers must be > 0")
	}

	// durations (после normalizeConfig они уже не пустые)
	accept, err := ParseDurationExt(cfg.Idempotency.AcceptMaxAge)
	if err != nil {
		return fmt.Errorf("idempotency.accept_max_age: %w", err)
	}

	retMin, err := ParseDurationExt(cfg.Idempotency.RetentionMin)
	if err != nil {
		return fmt.Errorf("idempotency.retention_min: %w", err)
	}

	var ret time.Duration
	if !cfg.Storage.Forever {
		ret, err = ParseDurationExt(cfg.Storage.Retention)
		if err != nil {
			return fmt.Errorf("storage.retention: %w", err)
		}
	}

	// sanity-checks
	if cfg.Storage.GCIntervalSec < 0 || cfg.Storage.GCMaxDeletes < 0 {
		return fmt.Errorf("storage gc settings must be >= 0")
	}

	// если не forever — retention должен быть >= max(retMin, accept)
	if !cfg.Storage.Forever {
		minNeed := retMin
		if accept > minNeed {
			minNeed = accept
		}
		if ret != 0 && ret < minNeed {
			return fmt.Errorf("storage.retention must be >= idempotency window (need at least %s)", minNeed)
		}
	}

	return nil
}
