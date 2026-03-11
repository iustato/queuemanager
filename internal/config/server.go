package config

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// ServerConfig contains all server-level settings.
// Priority: .env file > OS env vars > defaults.
type ServerConfig struct {
	PublicAddr  string
	InternalAddr string

	QueuesConfigDir string
	QueueStorageDir string
	StaticDir       string

	PhpCgiBin string

	WorkerToken   string
	AllowAutoGUID bool

	StorageOpenTimeoutMs int
	ProcessingTimeoutMs  int64
	GCProcessingGraceMs  int64

	ReadHeaderTimeoutSec int
	ShutdownTimeoutSec   int
	StopAllTimeoutSec    int

	DisableAccessLog bool
}

// LoadServerConfig loads server configuration.
// Priority: .env file values > OS environment variables > hardcoded defaults.
// Missing .env file is not an error (all values fall back to env/defaults).
func LoadServerConfig(envFile string) ServerConfig {
	fileVars := parseEnvFile(envFile)

	get := func(key string) string {
		// .env file has priority over OS env
		if v, ok := fileVars[key]; ok {
			return v
		}
		return os.Getenv(key)
	}

	getStr := func(key, def string) string {
		v := get(key)
		if v == "" {
			return def
		}
		return v
	}

	getInt := func(key string, def int) int {
		v := get(key)
		if v == "" {
			return def
		}
		n, err := strconv.Atoi(v)
		if err != nil {
			return def
		}
		return n
	}

	getInt64 := func(key string, def int64) int64 {
		v := get(key)
		if v == "" {
			return def
		}
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return def
		}
		return n
	}

	getBool := func(key string) bool {
		v := get(key)
		return v == "true" || v == "1"
	}

	return ServerConfig{
		PublicAddr:   getStr("PUBLIC_ADDR", "0.0.0.0:8080"),
		InternalAddr: getStr("INTERNAL_ADDR", "127.0.0.1:8081"),

		QueuesConfigDir: getStr("QUEUES_CONFIG_DIR", "./configs"),
		QueueStorageDir: getStr("QUEUE_STORAGE_DIR", "./data"),
		StaticDir:       getStr("STATIC_DIR", "web/static"),

		PhpCgiBin: getStr("PHP_CGI", "php-cgi"),

		WorkerToken:   getStr("WORKER_TOKEN", ""),
		AllowAutoGUID: getBool("ALLOW_AUTO_GUID"),

		StorageOpenTimeoutMs: getInt("STORAGE_OPEN_TIMEOUT_MS", 2000),
		ProcessingTimeoutMs:  getInt64("PROCESSING_TIMEOUT_MS", 120_000),
		GCProcessingGraceMs:  getInt64("GC_PROCESSING_GRACE_MS", 120_000),

		ReadHeaderTimeoutSec: getInt("READ_HEADER_TIMEOUT_SEC", 5),
		ShutdownTimeoutSec:   getInt("SHUTDOWN_TIMEOUT_SEC", 10),
		StopAllTimeoutSec:    getInt("STOP_ALL_TIMEOUT_SEC", 30),

		DisableAccessLog: getBool("DISABLE_ACCESS_LOG"),
	}
}

// parseEnvFile reads a .env file and returns key-value pairs.
// Returns empty map if file doesn't exist or can't be read.
// Format: KEY=VALUE, # comments, empty lines, optional quoting.
func parseEnvFile(path string) map[string]string {
	result := make(map[string]string)

	f, err := os.Open(path)
	if err != nil {
		return result
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// split on first '='
		idx := strings.IndexByte(line, '=')
		if idx < 0 {
			continue
		}

		key := strings.TrimSpace(line[:idx])
		val := strings.TrimSpace(line[idx+1:])

		if key == "" {
			continue
		}

		// strip surrounding quotes (single or double)
		val = unquote(val)

		result[key] = val
	}

	return result
}

// unquote removes matching surrounding quotes from a value.
func unquote(s string) string {
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') ||
			(s[0] == '\'' && s[len(s)-1] == '\'') {
			return s[1 : len(s)-1]
		}
	}
	return s
}

// FormatEnvFile creates a .env file content string from a ServerConfig.
// Useful for generating the example file or debugging.
func FormatEnvFile(cfg ServerConfig) string {
	var b strings.Builder
	w := func(format string, args ...any) {
		fmt.Fprintf(&b, format, args...)
	}

	w("# Queue Service — server configuration\n")
	w("# Priority: this file > OS environment variables > defaults\n\n")

	w("# ---- Network ----\n")
	w("PUBLIC_ADDR=%s\n", cfg.PublicAddr)
	w("INTERNAL_ADDR=%s\n\n", cfg.InternalAddr)

	w("# ---- Paths ----\n")
	w("QUEUES_CONFIG_DIR=%s\n", cfg.QueuesConfigDir)
	w("QUEUE_STORAGE_DIR=%s\n", cfg.QueueStorageDir)
	w("STATIC_DIR=%s\n\n", cfg.StaticDir)

	w("# ---- PHP ----\n")
	w("PHP_CGI=%s\n\n", cfg.PhpCgiBin)

	w("# ---- Auth ----\n")
	w("WORKER_TOKEN=%s\n", cfg.WorkerToken)
	w("ALLOW_AUTO_GUID=%v\n\n", cfg.AllowAutoGUID)

	w("# ---- Storage ----\n")
	w("STORAGE_OPEN_TIMEOUT_MS=%d\n", cfg.StorageOpenTimeoutMs)
	w("PROCESSING_TIMEOUT_MS=%d\n", cfg.ProcessingTimeoutMs)
	w("GC_PROCESSING_GRACE_MS=%d\n\n", cfg.GCProcessingGraceMs)

	w("# ---- HTTP ----\n")
	w("READ_HEADER_TIMEOUT_SEC=%d\n\n", cfg.ReadHeaderTimeoutSec)

	w("# ---- Shutdown ----\n")
	w("SHUTDOWN_TIMEOUT_SEC=%d\n", cfg.ShutdownTimeoutSec)
	w("STOP_ALL_TIMEOUT_SEC=%d\n\n", cfg.StopAllTimeoutSec)

	w("# ---- Logging ----\n")
	w("DISABLE_ACCESS_LOG=%v\n", cfg.DisableAccessLog)

	return b.String()
}
