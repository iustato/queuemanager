package queue

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestBuildPhpEnvMap_DefaultsAndOverrides(t *testing.T) {
	scriptPath := filepath.Join("some", "dir", "job.php")
	job := Job{
		Queue:   "q1",
		MsgID:   "m1",
		Attempt: 3,
		Body:    []byte(`{"x":1}`),

		Method:      "PUT",
		QueryString: "a=1&b=2",
		RemoteAddr:  "127.0.0.1:9000",
	}

	env := buildPhpEnvMap(scriptPath, job)

	// defaults
	if env["GATEWAY_INTERFACE"] != "CGI/1.1" {
		t.Fatalf("GATEWAY_INTERFACE: got %q", env["GATEWAY_INTERFACE"])
	}
	if env["SERVER_PROTOCOL"] != "HTTP/1.1" {
		t.Fatalf("SERVER_PROTOCOL: got %q", env["SERVER_PROTOCOL"])
	}
	if env["REDIRECT_STATUS"] != "200" {
		t.Fatalf("REDIRECT_STATUS: got %q", env["REDIRECT_STATUS"])
	}

	// script
	if env["SCRIPT_FILENAME"] != scriptPath {
		t.Fatalf("SCRIPT_FILENAME: got %q want %q", env["SCRIPT_FILENAME"], scriptPath)
	}
	if env["SCRIPT_NAME"] != "job.php" {
		t.Fatalf("SCRIPT_NAME: got %q want %q", env["SCRIPT_NAME"], "job.php")
	}

	// body derived
	if env["CONTENT_TYPE"] != "application/json" {
		t.Fatalf("CONTENT_TYPE: got %q", env["CONTENT_TYPE"])
	}
	if env["CONTENT_LENGTH"] != "7" { // len(`{"x":1}`) == 7
		t.Fatalf("CONTENT_LENGTH: got %q want %q", env["CONTENT_LENGTH"], "7")
	}

	// job fields
	if env["MSG_ID"] != "m1" {
		t.Fatalf("MSG_ID: got %q", env["MSG_ID"])
	}
	if env["QUEUE"] != "q1" {
		t.Fatalf("QUEUE: got %q", env["QUEUE"])
	}
	if env["ATTEMPT"] != "3" {
		t.Fatalf("ATTEMPT: got %q", env["ATTEMPT"])
	}

	// overrides
	if env["REQUEST_METHOD"] != "PUT" {
		t.Fatalf("REQUEST_METHOD: got %q want %q", env["REQUEST_METHOD"], "PUT")
	}
	if env["QUERY_STRING"] != "a=1&b=2" {
		t.Fatalf("QUERY_STRING: got %q want %q", env["QUERY_STRING"], "a=1&b=2")
	}
	if env["REMOTE_ADDR"] != "127.0.0.1" { // stripPort()
		t.Fatalf("REMOTE_ADDR: got %q want %q", env["REMOTE_ADDR"], "127.0.0.1")
	}
}

func TestBuildPhpEnvStandalone_ConvertsToSlice(t *testing.T) {
	scriptPath := "job.php"
	job := Job{
		Queue:   "q1",
		MsgID:   "m1",
		Attempt: 1,
		Body:    []byte("abc"),
	}

	envSlice, err := buildPhpEnvStandalone(scriptPath, job)
	if err != nil {
		t.Fatalf("buildPhpEnvStandalone: %v", err)
	}
	if len(envSlice) == 0 {
		t.Fatalf("expected non-empty env slice")
	}

	// проверим, что нужные пары точно есть
	wantContains := []string{
		"SCRIPT_FILENAME=" + scriptPath,
		"MSG_ID=m1",
		"QUEUE=q1",
		"ATTEMPT=1",
		"CONTENT_LENGTH=3",
	}
	for _, w := range wantContains {
		found := false
		for _, s := range envSlice {
			if s == w || strings.HasPrefix(s, w) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected envSlice to contain %q, got %#v", w, envSlice)
		}
	}
}