package config

type QueueConfig struct {
	Name       string `yaml:"name"`
	SchemaFile string `yaml:"schema_file"`

	MaxSize    int    `yaml:"max_size"`
	Workers    int    `yaml:"workers"`
	Runtime    string `yaml:"runtime"`
	Script     string `yaml:"script"`
	TimeoutSec int    `yaml:"timeout_sec"`
	MaxQueue   int    `yaml:"max_queue"`
	Command []string `json:"command,omitempty" yaml:"command,omitempty"`

	// php-fpm options
	FPMNetwork       string `yaml:"fpm_network"`
	FPMAddress       string `yaml:"fpm_address"`
	FPMDialTimeoutMs int    `yaml:"fpm_dial_timeout_ms"` // default: 3000
	FPMServerName    string `yaml:"fpm_server_name"`     // default: "queue-service"
	FPMServerPort    string `yaml:"fpm_server_port"`     // default: "80"

	// --- new ---
	Idempotency IdempotencyConfig `yaml:"idempotency"`
	Storage     StorageConfig     `yaml:"storage"`

	MaxRetries   int `yaml:"max_retries"`
	RetryDelayMs int `yaml:"retry_delay_ms"`

	// logging
	LogDir string `yaml:"log_dir"` // per-queue log directory (default: "configs/scripts/logs")

	// TTLs
	ResultTTL     string `yaml:"result_ttl"`     // TTL for result after job done (default: "10m")
	MessageExpiry string `yaml:"message_expiry"` // message expiration time (default: "30d")

	// HTTP handler
	PushTimeoutSec int `yaml:"push_timeout_sec"` // push handler context timeout (default: 5)
	EnqueueWaitMs  int `yaml:"enqueue_wait_ms"`  // backpressure wait when queue is full (default: 100)

	// runner buffer limits
	MaxStdoutBytes   int  `yaml:"max_stdout_bytes"`   // max stdout capture (default: 4194304 = 4 MiB)
	MaxStderrBytes   int  `yaml:"max_stderr_bytes"`   // max stderr capture (default: 1048576 = 1 MiB)
	MaxResponseBytes int  `yaml:"max_response_bytes"` // FastCGI max response (default: 4194304 = 4 MiB)
	TruncateOnLimit  *bool `yaml:"truncate_on_limit"`  // FastCGI truncate on limit (default: false)

	// maintenance
	RequeueStuckIntervalSec int `yaml:"requeue_stuck_interval_sec"` // default: 15
	RequeueStuckBatchLimit  int `yaml:"requeue_stuck_batch_limit"`  // default: 200
	GCBatchLimit            int `yaml:"gc_batch_limit"`             // default: 500
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
