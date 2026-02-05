package config

// QueueConfig описывает конфигурацию ОДНОЙ очереди
type QueueConfig struct {
	// Name — логическое имя очереди.
	// Если не задано явно в YAML — будет выведено из имени файла.
	Name string `yaml:"name"`

	// SchemaFile — путь к JSON Schema файла (loader приводит к абсолютному виду).
	SchemaFile string `yaml:"schema_file"`

	// MaxSize — максимальный размер входного тела запроса (bytes).
	MaxSize int `yaml:"max_size"`

	// Workers — количество воркеров (goroutines) для очереди.
	Workers int `yaml:"workers"`

	// Runtime — чем запускать обработчик:
	// примеры: "php-cgi", "python3"
	Runtime string `yaml:"runtime"`

	// Script — путь к скрипту обработчика (php/python).
	// Лучше относительный от корня проекта или абсолютный (loader может привести к abs).
	Script string `yaml:"script"`

	// TimeoutSec — таймаут выполнения одного job (сек).
	// 0 => дефолт (например 10s).
	TimeoutSec int `yaml:"timeout_sec"`

	// MaxQueue — размер буфера очереди (сколько job может ждать).
	// 0 => дефолт (например 100).
	MaxQueue int `yaml:"max_queue"`
}

// AppConfig (опционально) — конфигурация всего приложения.
type AppConfig struct {
	QueuesDir string
}
