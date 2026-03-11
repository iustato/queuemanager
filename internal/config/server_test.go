package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTempEnv(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatalf("write .env: %v", err)
	}
	return p
}

// ---------- LoadServerConfig ----------

func TestLoadServerConfig_Defaults(t *testing.T) {
	// No file, no env → all defaults.
	cfg := LoadServerConfig(filepath.Join(t.TempDir(), "nonexistent.env"))

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"PublicAddr", cfg.PublicAddr, "0.0.0.0:8080"},
		{"InternalAddr", cfg.InternalAddr, "127.0.0.1:8081"},
		{"QueuesConfigDir", cfg.QueuesConfigDir, "./configs"},
		{"QueueStorageDir", cfg.QueueStorageDir, "./data"},
		{"StaticDir", cfg.StaticDir, "web/static"},
		{"PhpCgiBin", cfg.PhpCgiBin, "php-cgi"},
		{"WorkerToken", cfg.WorkerToken, ""},
		{"AllowAutoGUID", cfg.AllowAutoGUID, false},
		{"StorageOpenTimeoutMs", cfg.StorageOpenTimeoutMs, 2000},
		{"ProcessingTimeoutMs", cfg.ProcessingTimeoutMs, int64(120_000)},
		{"GCProcessingGraceMs", cfg.GCProcessingGraceMs, int64(120_000)},
		{"ReadHeaderTimeoutSec", cfg.ReadHeaderTimeoutSec, 5},
		{"ShutdownTimeoutSec", cfg.ShutdownTimeoutSec, 10},
		{"StopAllTimeoutSec", cfg.StopAllTimeoutSec, 30},
		{"DisableAccessLog", cfg.DisableAccessLog, false},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, c.got, c.want)
		}
	}
}

func TestLoadServerConfig_FromFile(t *testing.T) {
	env := writeTempEnv(t, `
PUBLIC_ADDR=0.0.0.0:9090
PHP_CGI=/usr/bin/php-cgi8.3
ALLOW_AUTO_GUID=true
SHUTDOWN_TIMEOUT_SEC=20
PROCESSING_TIMEOUT_MS=60000
DISABLE_ACCESS_LOG=1
`)
	cfg := LoadServerConfig(env)

	// overridden
	if cfg.PublicAddr != "0.0.0.0:9090" {
		t.Errorf("PublicAddr: got %q", cfg.PublicAddr)
	}
	if cfg.PhpCgiBin != "/usr/bin/php-cgi8.3" {
		t.Errorf("PhpCgiBin: got %q", cfg.PhpCgiBin)
	}
	if !cfg.AllowAutoGUID {
		t.Errorf("AllowAutoGUID: got false, want true")
	}
	if cfg.ShutdownTimeoutSec != 20 {
		t.Errorf("ShutdownTimeoutSec: got %d, want 20", cfg.ShutdownTimeoutSec)
	}
	if cfg.ProcessingTimeoutMs != 60_000 {
		t.Errorf("ProcessingTimeoutMs: got %d, want 60000", cfg.ProcessingTimeoutMs)
	}
	if !cfg.DisableAccessLog {
		t.Errorf("DisableAccessLog: got false, want true")
	}

	// non-overridden → defaults
	if cfg.InternalAddr != "127.0.0.1:8081" {
		t.Errorf("InternalAddr should be default, got %q", cfg.InternalAddr)
	}
	if cfg.StorageOpenTimeoutMs != 2000 {
		t.Errorf("StorageOpenTimeoutMs should be default, got %d", cfg.StorageOpenTimeoutMs)
	}
}

func TestLoadServerConfig_EnvFallback(t *testing.T) {
	t.Setenv("PUBLIC_ADDR", "1.2.3.4:3000")
	t.Setenv("STORAGE_OPEN_TIMEOUT_MS", "5000")

	cfg := LoadServerConfig(filepath.Join(t.TempDir(), "nonexistent.env"))

	if cfg.PublicAddr != "1.2.3.4:3000" {
		t.Errorf("PublicAddr: got %q, want 1.2.3.4:3000", cfg.PublicAddr)
	}
	if cfg.StorageOpenTimeoutMs != 5000 {
		t.Errorf("StorageOpenTimeoutMs: got %d, want 5000", cfg.StorageOpenTimeoutMs)
	}
}

func TestLoadServerConfig_FileTakesPriority(t *testing.T) {
	t.Setenv("PUBLIC_ADDR", "from-env:1111")

	env := writeTempEnv(t, "PUBLIC_ADDR=from-file:2222\n")
	cfg := LoadServerConfig(env)

	if cfg.PublicAddr != "from-file:2222" {
		t.Errorf("expected file to win over env, got %q", cfg.PublicAddr)
	}
}

func TestLoadServerConfig_InvalidInt(t *testing.T) {
	env := writeTempEnv(t, "SHUTDOWN_TIMEOUT_SEC=abc\nPROCESSING_TIMEOUT_MS=xyz\n")
	cfg := LoadServerConfig(env)

	if cfg.ShutdownTimeoutSec != 10 {
		t.Errorf("ShutdownTimeoutSec: invalid int should fallback to default 10, got %d", cfg.ShutdownTimeoutSec)
	}
	if cfg.ProcessingTimeoutMs != 120_000 {
		t.Errorf("ProcessingTimeoutMs: invalid int64 should fallback to default 120000, got %d", cfg.ProcessingTimeoutMs)
	}
}

func TestLoadServerConfig_BoolVariants(t *testing.T) {
	// "true" → true
	env := writeTempEnv(t, "ALLOW_AUTO_GUID=true\n")
	if cfg := LoadServerConfig(env); !cfg.AllowAutoGUID {
		t.Errorf("ALLOW_AUTO_GUID=true should be true")
	}

	// "1" → true
	env = writeTempEnv(t, "DISABLE_ACCESS_LOG=1\n")
	if cfg := LoadServerConfig(env); !cfg.DisableAccessLog {
		t.Errorf("DISABLE_ACCESS_LOG=1 should be true")
	}

	// "false" → false
	env = writeTempEnv(t, "ALLOW_AUTO_GUID=false\n")
	if cfg := LoadServerConfig(env); cfg.AllowAutoGUID {
		t.Errorf("ALLOW_AUTO_GUID=false should be false")
	}

	// "0" → false
	env = writeTempEnv(t, "ALLOW_AUTO_GUID=0\n")
	if cfg := LoadServerConfig(env); cfg.AllowAutoGUID {
		t.Errorf("ALLOW_AUTO_GUID=0 should be false")
	}
}

// ---------- parseEnvFile ----------

func TestParseEnvFile_CommentsAndBlanks(t *testing.T) {
	p := writeTempEnv(t, "# comment\n\nKEY1=val1\n  # indented comment\nKEY2=val2\n")
	m := parseEnvFile(p)

	if len(m) != 2 {
		t.Fatalf("expected 2 keys, got %d: %v", len(m), m)
	}
	if m["KEY1"] != "val1" {
		t.Errorf("KEY1: got %q", m["KEY1"])
	}
	if m["KEY2"] != "val2" {
		t.Errorf("KEY2: got %q", m["KEY2"])
	}
}

func TestParseEnvFile_Quotes(t *testing.T) {
	p := writeTempEnv(t, `KEY1="double quoted"
KEY2='single quoted'
KEY3=no quotes
`)
	m := parseEnvFile(p)

	if m["KEY1"] != "double quoted" {
		t.Errorf("KEY1: got %q, want %q", m["KEY1"], "double quoted")
	}
	if m["KEY2"] != "single quoted" {
		t.Errorf("KEY2: got %q, want %q", m["KEY2"], "single quoted")
	}
	if m["KEY3"] != "no quotes" {
		t.Errorf("KEY3: got %q, want %q", m["KEY3"], "no quotes")
	}
}

func TestParseEnvFile_EdgeCases(t *testing.T) {
	p := writeTempEnv(t, "EMPTY=\nEQUALS=a=b=c\nNOSEPARATOR\n=value\n  KEY  =  val  \n")
	m := parseEnvFile(p)

	// empty value
	if v, ok := m["EMPTY"]; !ok || v != "" {
		t.Errorf("EMPTY: got %q ok=%v, want empty string", v, ok)
	}

	// value with '='
	if m["EQUALS"] != "a=b=c" {
		t.Errorf("EQUALS: got %q, want %q", m["EQUALS"], "a=b=c")
	}

	// no separator → skipped
	if _, ok := m["NOSEPARATOR"]; ok {
		t.Errorf("NOSEPARATOR should be skipped")
	}

	// empty key → skipped
	if _, ok := m[""]; ok {
		t.Errorf("empty key should be skipped")
	}

	// whitespace trimmed
	if m["KEY"] != "val" {
		t.Errorf("KEY: got %q, want %q", m["KEY"], "val")
	}
}

func TestParseEnvFile_Missing(t *testing.T) {
	m := parseEnvFile(filepath.Join(t.TempDir(), "no-such-file"))
	if len(m) != 0 {
		t.Errorf("missing file should return empty map, got %v", m)
	}
}
