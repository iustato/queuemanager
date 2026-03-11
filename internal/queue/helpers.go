package queue

import (
	"bytes"
	"net"
	"path/filepath"
	"strconv"
)

// buildPhpEnvMap — Единый источник правды для всех переменных
func buildPhpEnvMap(scriptPath string, job Job) map[string]string {
	m := map[string]string{
		"GATEWAY_INTERFACE": "CGI/1.1",
		"SERVER_PROTOCOL":    "HTTP/1.1",
		"REDIRECT_STATUS":    "200",
		"SCRIPT_FILENAME":   scriptPath,
		"SCRIPT_NAME":       filepath.Base(scriptPath),
		"REQUEST_METHOD":    "POST",
		"CONTENT_TYPE":      "application/json",
		"CONTENT_LENGTH":    strconv.Itoa(len(job.Body)),
		"MESSAGE_GUID":       job.MessageGUID,
		"QUEUE":             job.Queue,
		"ATTEMPT":           strconv.Itoa(job.Attempt),
	}

	if job.Method != "" { m["REQUEST_METHOD"] = job.Method }
	if job.QueryString != "" { m["QUERY_STRING"] = job.QueryString }
	if job.RemoteAddr != "" { m["REMOTE_ADDR"] = stripPort(job.RemoteAddr) }
	
	return m
}

// buildPhpEnvStandalone — Обертка для PHPCGI и Exec (конвертирует map в []string)
func buildPhpEnvStandalone(scriptPath string, job Job) ([]string, error) {
	envMap := buildPhpEnvMap(scriptPath, job)
	envSlice := make([]string, 0, len(envMap))
	for k, v := range envMap {
		envSlice = append(envSlice, k+"="+v)
	}
	return envSlice, nil
}

func stripPort(addr string) string {
	if addr == "" { return "" }
	host, _, err := net.SplitHostPort(addr)
	if err == nil { return host }
	return addr
}

const (
	cgiSepLong  = "\r\n\r\n"
	cgiSepShort = "\n\n"
)

func splitCGIBodyIndex(out []byte) int {
	if i := bytes.Index(out, []byte(cgiSepLong)); i >= 0 {
		return i + len(cgiSepLong)
	}
	if i := bytes.Index(out, []byte(cgiSepShort)); i >= 0 {
		return i + len(cgiSepShort)
	}
	return 0
}