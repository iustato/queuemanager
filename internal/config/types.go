package config

type QueueConfig struct {
	Name       string `yaml:"name" json:"name"`
	SchemaFile string `yaml:"schema_file" json:"schema_file,omitempty"`

	MaxSize    int    `yaml:"max_size" json:"max_size,omitempty"`
	Workers    int    `yaml:"workers" json:"workers"`
	Runtime    string `yaml:"runtime" json:"runtime"`
	Script     string `yaml:"script" json:"script,omitempty"`
	TimeoutSec int    `yaml:"timeout_sec" json:"timeout_sec,omitempty"`
	MaxQueue   int    `yaml:"max_queue" json:"max_queue,omitempty"`
	Command []string `yaml:"command,omitempty" json:"command,omitempty"`

	// php-fpm options
	FPMNetwork       string `yaml:"fpm_network" json:"fpm_network,omitempty"`
	FPMAddress       string `yaml:"fpm_address" json:"fpm_address,omitempty"`
	FPMDialTimeoutMs int    `yaml:"fpm_dial_timeout_ms" json:"fpm_dial_timeout_ms,omitempty"`
	FPMServerName    string `yaml:"fpm_server_name" json:"fpm_server_name,omitempty"`
	FPMServerPort    string `yaml:"fpm_server_port" json:"fpm_server_port,omitempty"`

	// --- new ---
	Idempotency IdempotencyConfig `yaml:"idempotency" json:"idempotency,omitempty"`
	Storage     StorageConfig     `yaml:"storage" json:"storage,omitempty"`

	MaxRetries   int `yaml:"max_retries" json:"max_retries,omitempty"`
	RetryDelayMs int `yaml:"retry_delay_ms" json:"retry_delay_ms,omitempty"`

	// logging
	LogDir string `yaml:"log_dir" json:"log_dir,omitempty"`

	// TTLs
	ResultTTL     string `yaml:"result_ttl" json:"result_ttl,omitempty"`
	MessageExpiry string `yaml:"message_expiry" json:"message_expiry,omitempty"`

	// HTTP handler
	PushTimeoutSec int `yaml:"push_timeout_sec" json:"push_timeout_sec,omitempty"`
	EnqueueWaitMs  int `yaml:"enqueue_wait_ms" json:"enqueue_wait_ms,omitempty"`

	// runner buffer limits
	MaxStdoutBytes   int  `yaml:"max_stdout_bytes" json:"max_stdout_bytes,omitempty"`
	MaxStderrBytes   int  `yaml:"max_stderr_bytes" json:"max_stderr_bytes,omitempty"`
	MaxResponseBytes int  `yaml:"max_response_bytes" json:"max_response_bytes,omitempty"`
	TruncateOnLimit  *bool `yaml:"truncate_on_limit" json:"truncate_on_limit,omitempty"`

	// maintenance
	RequeueStuckIntervalSec int `yaml:"requeue_stuck_interval_sec" json:"requeue_stuck_interval_sec,omitempty"`
	RequeueStuckBatchLimit  int `yaml:"requeue_stuck_batch_limit" json:"requeue_stuck_batch_limit,omitempty"`
	GCBatchLimit            int `yaml:"gc_batch_limit" json:"gc_batch_limit,omitempty"`
}
type IdempotencyConfig struct {
	AcceptMaxAge string `yaml:"accept_max_age" json:"accept_max_age,omitempty"`
	RetentionMin string `yaml:"retention_min" json:"retention_min,omitempty"`
}

type StorageConfig struct {
	Retention string `yaml:"retention" json:"retention,omitempty"`
	Forever   bool   `yaml:"forever" json:"forever,omitempty"`

	GCIntervalSec int `yaml:"gc_interval_sec" json:"gc_interval_sec,omitempty"`
	GCMaxDeletes  int `yaml:"gc_max_deletes" json:"gc_max_deletes,omitempty"`
}
