package queue

import (
	"bytes"
	"net"
	"path/filepath"
	"strconv"
	"strings"
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
		"MSG_ID":            job.MsgID,
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

func splitCGI(out []byte) (map[string]string, []byte) {
	h := map[string]string{}

	sep := []byte("\r\n\r\n")
	i := bytes.Index(out, sep)
	if i < 0 {
		sep = []byte("\n\n")
		i = bytes.Index(out, sep)
	}
	if i < 0 {
		return h, out
	}

	headerPart := out[:i]
	body := out[i+len(sep):]

	lines := bytes.Split(headerPart, []byte("\n"))
	for _, ln := range lines {
		ln = bytes.TrimSpace(bytes.TrimRight(ln, "\r"))
		if len(ln) == 0 {
			continue
		}
		col := bytes.IndexByte(ln, ':')
		if col <= 0 {
			continue
		}
		k := strings.ToLower(strings.TrimSpace(string(ln[:col])))
		v := strings.TrimSpace(string(ln[col+1:]))
		h[k] = v
	}

	return h, body
}

func safeCmd(cmd []string) string {
	switch len(cmd) {
	case 0:
		return "none"
	case 1:
		return cmd[0]
	default:
		return strings.Join(cmd[:2], " ")
	}
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