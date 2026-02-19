package config

type QueueConfig struct {
	Name       string `yaml:"name"`
	SchemaFile string `yaml:"schema_file"`

	MaxSize  int    `yaml:"max_size"`
	Workers  int    `yaml:"workers"`
	Runtime  string `yaml:"runtime"`
	Script   string `yaml:"script"`
	TimeoutSec int  `yaml:"timeout_sec"`
	MaxQueue int    `yaml:"max_queue"`

	// --- new ---
	Idempotency IdempotencyConfig `yaml:"idempotency"`
	Storage     StorageConfig     `yaml:"storage"`
	
	MaxRetries   int `yaml:"max_retries"`
	RetryDelayMs int `yaml:"retry_delay_ms"`
}

type IdempotencyConfig struct {
	// UUIDv7 старше этого возраста — отклоняем (400)
	AcceptMaxAge string `yaml:"accept_max_age"` // пример: "30d" или "720h"
	// минимум хранения для дедупа; если пусто => = AcceptMaxAge
	RetentionMin string `yaml:"retention_min"` // пример: "30d"
}

type StorageConfig struct {
	// default retention, если Forever=false
	Retention string `yaml:"retention"` // пример: "90d"
	Forever   bool   `yaml:"forever"`

	GCIntervalSec int `yaml:"gc_interval_sec"` // пример: 60
	GCMaxDeletes  int `yaml:"gc_max_deletes"`  // пример: 500
}
