package queue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"strconv"

	"go-web-server/internal/config"
	"go-web-server/internal/httpserver"
	"go-web-server/internal/storage"
	"go-web-server/internal/validate"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

const testSchemaJSON = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["text"],
  "properties": {
    "text": {
      "type": "string",
      "minLength": 1
    }
  },
  "additionalProperties": false
}`

func newTestHTTPServer(m *Manager, queueName string) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/"+queueName+"/newmessage", func(w http.ResponseWriter, r *http.Request) {
		m.HandleNewMessage(queueName, w, r)
	})
	return httptest.NewServer(mux)
}

func mustUUIDv7String(t *testing.T) string {
	t.Helper()
	u, err := uuid.NewV7()
	if err != nil {
		t.Fatalf("uuid.NewV7: %v", err)
	}
	return u.String()
}

func writeTempSchemaFile(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "schema.json")
	if err := os.WriteFile(path, []byte(testSchemaJSON), 0o644); err != nil {
		t.Fatalf("write schema file: %v", err)
	}
	return path
}

func doPostNewMessage(t *testing.T, baseURL, queueName string, body []byte, idemKey string) (status int, msgID string, respBody string, respHeaders http.Header) {
	t.Helper()

	req, _ := http.NewRequest(http.MethodPost, baseURL+"/"+queueName+"/newmessage", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()

	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header.Get("X-Message-Id"), string(b), resp.Header.Clone()
}

// --- API contract helpers ---
func mustCheckErrorResp(t *testing.T, ar apiResp, body []byte, wantCode string) {
	t.Helper()

	// для ошибок ok должно быть false
	if ar.OK {
		t.Fatalf("expected ok=false, got ok=true; body=%s", string(body))
	}
	if ar.Error == nil {
		t.Fatalf("expected error object, got nil; body=%s", string(body))
	}
	if wantCode != "" && ar.Error.Code != wantCode {
		t.Fatalf("expected error.code=%q, got %q; body=%s", wantCode, ar.Error.Code, string(body))
	}
	if strings.TrimSpace(ar.Error.Message) == "" {
		t.Fatalf("expected error.message non-empty; body=%s", string(body))
	}
}

func httpGetAPIAlwaysJSON(t *testing.T, url string) (status int, body []byte, ar apiResp, hdr http.Header) {
	t.Helper()

	req, _ := http.NewRequest(http.MethodGet, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()

	hdr = resp.Header.Clone()
	body, _ = io.ReadAll(resp.Body)

	// В ЭТИХ ТЕСТАХ мы ожидаем JSON контракт даже на ошибках.
	// Поэтому анмаршалим всегда.
	if err := json.Unmarshal(body, &ar); err != nil {
		t.Fatalf("GET %s: unmarshal apiResp: %v; body=%s", url, err, string(body))
	}
	return resp.StatusCode, body, ar, hdr
}

type apiResp struct {
	OK   bool            `json:"ok"`
	Data json.RawMessage `json:"data,omitempty"`
	Error *struct {
		Code    string          `json:"code"`
		Message string          `json:"message"`
		Details json.RawMessage `json:"details,omitempty"`
	} `json:"error,omitempty"`
	RequestID string `json:"request_id,omitempty"`
}

func httpGetAPI(t *testing.T, url string) (status int, body []byte, ar apiResp) {
	t.Helper()

	req, _ := http.NewRequest(http.MethodGet, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()

	body, _ = io.ReadAll(resp.Body)

	// для 2xx мы ожидаем JSON контракт
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if err := json.Unmarshal(body, &ar); err != nil {
			t.Fatalf("GET %s: unmarshal apiResp: %v; body=%s", url, err, string(body))
		}
	}

	return resp.StatusCode, body, ar
}

func httpGetAPIWithHeaders(t *testing.T, url string) (status int, body []byte, ar apiResp, hdr http.Header) {
	t.Helper()

	req, _ := http.NewRequest(http.MethodGet, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()

	hdr = resp.Header.Clone()
	body, _ = io.ReadAll(resp.Body)

	// для 2xx мы ожидаем JSON контракт
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if err := json.Unmarshal(body, &ar); err != nil {
			t.Fatalf("GET %s: unmarshal apiResp: %v; body=%s", url, err, string(body))
		}
	}

	return resp.StatusCode, body, ar, hdr
}

func mustUnmarshalData[T any](t *testing.T, ar apiResp, body []byte) T {
	t.Helper()

	if !ar.OK {
		t.Fatalf("expected ok=true, got ok=false err=%+v body=%s", ar.Error, string(body))
	}

	var out T
	if err := json.Unmarshal(ar.Data, &out); err != nil {
		t.Fatalf("unmarshal apiResp.data: %v; body=%s", err, string(body))
	}
	return out
}

func mustCheckJSONHeaders(t *testing.T, hdr http.Header, body []byte, where string) {
	t.Helper()

	ct := hdr.Get("Content-Type")
	if !strings.HasPrefix(strings.ToLower(ct), "application/json") {
		t.Fatalf("%s: expected Content-Type application/json, got %q; body=%s", where, ct, string(body))
	}
	if rid := hdr.Get("X-Request-Id"); strings.TrimSpace(rid) == "" {
		t.Fatalf("%s: expected X-Request-Id header; body=%s", where, string(body))
	}
}

// --- legacy helper (can stay) ---

func httpGetJSON(t *testing.T, url string, out any) (status int, body string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()

	b, _ := io.ReadAll(resp.Body)
	body = string(b)

	if out != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if err := json.Unmarshal(b, out); err != nil {
			t.Fatalf("GET %s: unmarshal: %v; body=%s", url, err, body)
		}
	}
	return resp.StatusCode, body
}

func keys(m map[string]storage.QueueInfo) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// --- E2E server wiring ---

type e2eServerOpts struct{}

func newE2EServer(t *testing.T, _ e2eServerOpts) (ts *httptest.Server, qm *Manager, cleanup func()) {
	t.Helper()

	_ = os.Setenv("ALLOW_AUTO_IDEMPOTENCY", "true") // чтобы можно было без Idempotency-Key
	logger := zap.NewNop()

	// temp directory for per-queue bolt files
	storageDir := filepath.Join(t.TempDir(), "storage")

	qm = NewManager(logger, storageDir, storage.OpenOptions{
		Timeout: 2 * time.Second,
	})

	// temp schema file
	schemaPath := filepath.Join(t.TempDir(), "schema.json")
	if err := os.WriteFile(schemaPath, []byte(testSchemaJSON), 0o644); err != nil {
		t.Fatalf("write schema: %v", err)
	}
	sch, err := validate.LoadSchemaFromFile(schemaPath)
	if err != nil {
		t.Fatalf("load schema: %v", err)
	}

	// queue config
	cfg := config.QueueConfig{
		Name:         "queue1",
		Workers:      1,
		MaxQueue:     100,
		MaxRetries:   3,
		RetryDelayMs: 10,
		TimeoutSec:   1,

		// Важно: твой Runtime выбирает runner по Command[0]
		// Берём /bin/true — он всегда exit 0 и игнорирует аргументы.
		Runtime: "/bin/true",
		Script:  "/dev/null",
	}

	if err := qm.AddQueue(cfg, sch); err != nil {
		t.Fatalf("add queue: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		path := strings.Trim(r.URL.Path, "/")
		parts := strings.Split(path, "/")

		if len(parts) == 1 && parts[0] == "info" && r.Method == http.MethodGet {
			qm.HandleGetInfoAll(w, r)
			return
		}
		if len(parts) == 2 && parts[1] == "newmessage" && r.Method == http.MethodPost {
			qm.HandleNewMessage(parts[0], w, r)
			return
		}
		if len(parts) == 2 && parts[1] == "info" && r.Method == http.MethodGet {
			qm.HandleGetInfo(parts[0], w, r)
			return
		}
		if len(parts) == 3 && parts[1] == "status" && r.Method == http.MethodGet {
			qm.HandleGetStatus(parts[0], parts[2], w, r)
			return
		}
		if len(parts) == 3 && parts[1] == "result" && r.Method == http.MethodGet {
			qm.HandleGetResult(parts[0], parts[2], w, r)
			return
		}

		http.NotFound(w, r)
	})

	// подключаем RequestID middleware (чтобы появился X-Request-Id)
	ts = httptest.NewServer(httpserver.RequestID(mux))

	cleanup = func() {
		ts.Close()
		qm.StopAll()
	}

	return ts, qm, cleanup
}

func postNewMessage(t *testing.T, baseURL, queue, bodyJSON, idemKey string) (msgID string, idem string) {
	t.Helper()

	req, err := http.NewRequest(http.MethodPost, baseURL+"/"+queue+"/newmessage", strings.NewReader(bodyJSON))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// если явно дали idemKey — используем, иначе пусть auto-idem сгенерит
	if strings.TrimSpace(idemKey) != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST newmessage: %v", err)
	}
	defer resp.Body.Close()

	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202, got %d; body=%q; headers: X-Message-Id=%q Idempotency-Key=%q",
			resp.StatusCode, string(b),
			resp.Header.Get("X-Message-Id"),
			resp.Header.Get("Idempotency-Key"),
		)
	}

	return resp.Header.Get("X-Message-Id"), resp.Header.Get("Idempotency-Key")
}

// --- Tests ---

func TestE2E_HTTP_HappyPath(t *testing.T) {
	dbPath := "test_e2e_http_happy.db"
	_ = os.Remove(dbPath)

	st, err := storage.Open(storage.OpenOptions{FilePath: dbPath})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		_ = st.Close()
		_ = os.Remove(dbPath)
	}()

	tmpDir := t.TempDir()
	schemaPath := writeTempSchemaFile(t, tmpDir)

	schema, err := validate.LoadSchemaFromFile(schemaPath)
	if err != nil {
		t.Fatalf("LoadSchemaFromFile: %v", err)
	}

	mgr := NewManager(zap.NewNop(), filepath.Dir(dbPath), storage.OpenOptions{})

	queueName := "queue1"
	cfg := config.QueueConfig{
		Name:       queueName,
		Workers:    1,
		MaxQueue:   10,
		MaxRetries: 0,
		TimeoutSec: 1,
	}

	tr := NewTestRunner(ModeOK)

	rt, err := NewRuntime(
		cfg,
		st,
		zap.NewNop(),
		schema,
		[]string{"test"},
		"/dev/null",
		"", "",
		WithRunnerFactory(func() (Runner, error) { return tr, nil }),
	)
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	defer rt.Stop()

	mgr.mu.Lock()
	mgr.queues[queueName] = rt
	mgr.mu.Unlock()

	srv := newTestHTTPServer(mgr, queueName)
	defer srv.Close()

	idem := mustUUIDv7String(t)
	payload := []byte(`{"text":"hello"}`)

	status, msgID, bodyStr, hdr := doPostNewMessage(t, srv.URL, queueName, payload, idem)
	if status != http.StatusAccepted {
		t.Fatalf("expected 202, got %d; body=%q; headers: X-Message-Id=%q Idempotency-Key=%q",
			status, bodyStr, hdr.Get("X-Message-Id"), hdr.Get("Idempotency-Key"))
	}
	if msgID == "" {
		t.Fatalf("missing X-Message-Id; body=%q", bodyStr)
	}

	stFinal, res := waitTerminalStatus(t, st, msgID, 2*time.Second)
	if stFinal != storage.StatusSucceeded {
		t.Fatalf("expected succeeded, got %s (exit=%d err=%q)", stFinal, res.ExitCode, res.Err)
	}
	if tr.Calls() != 1 {
		t.Fatalf("expected runner calls=1, got %d", tr.Calls())
	}
}

func TestE2E_HTTP_Dedup_IdempotencyKey(t *testing.T) {
	dbPath := "test_e2e_http_dedup.db"
	_ = os.Remove(dbPath)

	st, err := storage.Open(storage.OpenOptions{FilePath: dbPath})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		_ = st.Close()
		_ = os.Remove(dbPath)
	}()

	tmpDir := t.TempDir()
	schemaPath := writeTempSchemaFile(t, tmpDir)

	schema, err := validate.LoadSchemaFromFile(schemaPath)
	if err != nil {
		t.Fatalf("LoadSchemaFromFile: %v", err)
	}

	mgr := NewManager(zap.NewNop(), filepath.Dir(dbPath), storage.OpenOptions{})

	queueName := "queue1"
	cfg := config.QueueConfig{
		Name:       queueName,
		Workers:    1,
		MaxQueue:   10,
		MaxRetries: 0,
		TimeoutSec: 1,
	}

	tr := NewTestRunner(ModeOK)

	rt, err := NewRuntime(
		cfg,
		st,
		zap.NewNop(),
		schema,
		[]string{"test"},
		"/dev/null",
		"", "",
		WithRunnerFactory(func() (Runner, error) { return tr, nil }),
	)
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	defer rt.Stop()

	mgr.mu.Lock()
	mgr.queues[queueName] = rt
	mgr.mu.Unlock()

	srv := newTestHTTPServer(mgr, queueName)
	defer srv.Close()

	idem := mustUUIDv7String(t)
	payload := []byte(`{"text":"hello"}`)

	code1, msg1, body1, hdr1 := doPostNewMessage(t, srv.URL, queueName, payload, idem)
	code2, msg2, body2, hdr2 := doPostNewMessage(t, srv.URL, queueName, payload, idem)

	if code1 != http.StatusAccepted {
		t.Fatalf("first request: expected 202, got %d; body=%q; headers: X-Message-Id=%q Idempotency-Key=%q",
			code1, body1, hdr1.Get("X-Message-Id"), hdr1.Get("Idempotency-Key"))
	}
	if code2 != http.StatusAccepted {
		t.Fatalf("second request: expected 202, got %d; body=%q; headers: X-Message-Id=%q Idempotency-Key=%q",
			code2, body2, hdr2.Get("X-Message-Id"), hdr2.Get("Idempotency-Key"))
	}

	if msg1 == "" || msg2 == "" {
		t.Fatalf("missing msg ids: %q %q (bodies=%q / %q)", msg1, msg2, body1, body2)
	}
	if msg1 != msg2 {
		t.Fatalf("expected same msg id for dedup, got %q and %q", msg1, msg2)
	}

	_, _ = waitTerminalStatus(t, st, msg1, 2*time.Second)

	if tr.Calls() != 1 {
		t.Fatalf("expected runner calls=1 (dedup), got %d", tr.Calls())
	}
}

func TestE2E_HTTP_QueueBusy_WhenOverCapacity(t *testing.T) {
	dbPath := "test_e2e_http_busy.db"
	_ = os.Remove(dbPath)

	st, err := storage.Open(storage.OpenOptions{FilePath: dbPath})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		_ = st.Close()
		_ = os.Remove(dbPath)
	}()

	tmpDir := t.TempDir()
	schemaPath := writeTempSchemaFile(t, tmpDir)
	schema, err := validate.LoadSchemaFromFile(schemaPath)
	if err != nil {
		t.Fatalf("LoadSchemaFromFile: %v", err)
	}

	mgr := NewManager(zap.NewNop(), filepath.Dir(dbPath), storage.OpenOptions{})

	queueName := "queue1"
	cfg := config.QueueConfig{
		Name:       queueName,
		Workers:    1,
		MaxQueue:   1,
		MaxRetries: 0,
		TimeoutSec: 2,
	}

	tr := NewTestRunner(ModeBlock)

	rt, err := NewRuntime(
		cfg,
		st,
		zap.NewNop(),
		schema,
		[]string{"test"},
		"/dev/null",
		"", "",
		WithRunnerFactory(func() (Runner, error) { return tr, nil }),
	)
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	defer rt.Stop()

	mgr.mu.Lock()
	mgr.queues[queueName] = rt
	mgr.mu.Unlock()

	srv := newTestHTTPServer(mgr, queueName)
	defer srv.Close()

	payload := []byte(`{"text":"hello"}`)

	type out struct {
		code int
		body string
	}
	ch := make(chan out, 3)

	for i := 0; i < 3; i++ {
		go func() {
			idem := mustUUIDv7String(t)
			code, _, body, _ := doPostNewMessage(t, srv.URL, queueName, payload, idem)
			ch <- out{code: code, body: body}
		}()
	}

	var accepted int
	var busyBodies []string
	for i := 0; i < 3; i++ {
		r := <-ch
		if r.code == http.StatusAccepted {
			accepted++
			continue
		}
		busyBodies = append(busyBodies, r.body)
	}

	if accepted == 3 {
		t.Fatalf("expected at least one request to be rejected as queue busy, but all 3 were accepted")
	}

	foundBusy := false
	for _, b := range busyBodies {
		if strings.Contains(strings.ToLower(b), "busy") {
			foundBusy = true
			break
		}
	}
	if !foundBusy {
		t.Fatalf("expected non-202 response to mention busy; bodies=%q", busyBodies)
	}
}

func TestE2E_HTTP_RetryFlow_FailThenSuccess(t *testing.T) {
	dbPath := "test_e2e_http_retry.db"
	_ = os.Remove(dbPath)

	st, err := storage.Open(storage.OpenOptions{FilePath: dbPath})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		_ = st.Close()
		_ = os.Remove(dbPath)
	}()

	tmpDir := t.TempDir()
	schemaPath := writeTempSchemaFile(t, tmpDir)
	schema, err := validate.LoadSchemaFromFile(schemaPath)
	if err != nil {
		t.Fatalf("LoadSchemaFromFile: %v", err)
	}

	mgr := NewManager(zap.NewNop(), filepath.Dir(dbPath), storage.OpenOptions{})

	queueName := "queue1"
	cfg := config.QueueConfig{
		Name:         queueName,
		Workers:      1,
		MaxQueue:     10,
		MaxRetries:   2,
		RetryDelayMs: 50,
		TimeoutSec:   1,
	}

	sr := &switchRunner{}

	rt, err := NewRuntime(
		cfg,
		st,
		zap.NewNop(),
		schema,
		[]string{"test"},
		"/dev/null",
		"", "",
		WithRunnerFactory(func() (Runner, error) { return sr, nil }),
	)
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	defer rt.Stop()

	mgr.mu.Lock()
	mgr.queues[queueName] = rt
	mgr.mu.Unlock()

	srv := newTestHTTPServer(mgr, queueName)
	defer srv.Close()

	idem := mustUUIDv7String(t)
	payload := []byte(`{"text":"retry test"}`)

	code, msgID, bodyStr, _ := doPostNewMessage(t, srv.URL, queueName, payload, idem)
	if code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d; body=%q", code, bodyStr)
	}
	if msgID == "" {
		t.Fatalf("missing X-Message-Id; body=%q", bodyStr)
	}

	stFinal, _ := waitTerminalStatus(t, st, msgID, 3*time.Second)
	if stFinal != storage.StatusSucceeded {
		t.Fatalf("expected succeeded after retry, got %s", stFinal)
	}

	if sr.calls.Load() != 2 {
		t.Fatalf("expected 2 runner calls (1 fail + 1 success), got %d", sr.calls.Load())
	}
}

type switchRunner struct {
	calls atomic.Int32
}

func (r *switchRunner) Run(ctx context.Context, cmdArgs []string, script string, job Job) Result {
	n := r.calls.Add(1)

	if n == 1 {
		return Result{
			Queue:      job.Queue,
			MsgID:      job.MsgID,
			ExitCode:   1,
			DurationMs: 1,
			Err:        errors.New("boom"),
		}
	}

	return Result{
		Queue:      job.Queue,
		MsgID:      job.MsgID,
		ExitCode:   0,
		DurationMs: 1,
		Err:        nil,
	}
}

func TestE2E_HTTP_RetryLimitExceeded_FinalFailed(t *testing.T) {
	dbPath := "test_e2e_http_retry_limit.db"
	_ = os.Remove(dbPath)

	st, err := storage.Open(storage.OpenOptions{FilePath: dbPath})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		_ = st.Close()
		_ = os.Remove(dbPath)
	}()

	tmpDir := t.TempDir()
	schemaPath := writeTempSchemaFile(t, tmpDir)
	schema, err := validate.LoadSchemaFromFile(schemaPath)
	if err != nil {
		t.Fatalf("LoadSchemaFromFile: %v", err)
	}

	mgr := NewManager(zap.NewNop(), filepath.Dir(dbPath), storage.OpenOptions{})

	queueName := "queue1"
	cfg := config.QueueConfig{
		Name:         queueName,
		Workers:      1,
		MaxQueue:     10,
		MaxRetries:   2,
		RetryDelayMs: 20,
		TimeoutSec:   1,
	}

	fr := &failRunner{}

	rt, err := NewRuntime(
		cfg,
		st,
		zap.NewNop(),
		schema,
		[]string{"test"},
		"/dev/null",
		"", "",
		WithRunnerFactory(func() (Runner, error) { return fr, nil }),
	)
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	defer rt.Stop()

	mgr.mu.Lock()
	mgr.queues[queueName] = rt
	mgr.mu.Unlock()

	srv := newTestHTTPServer(mgr, queueName)
	defer srv.Close()

	idem := mustUUIDv7String(t)
	payload := []byte(`{"text":"retry limit test"}`)

	code, msgID, bodyStr, _ := doPostNewMessage(t, srv.URL, queueName, payload, idem)
	if code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d; body=%q", code, bodyStr)
	}
	if msgID == "" {
		t.Fatalf("missing X-Message-Id; body=%q", bodyStr)
	}

	stFinal, res := waitTerminalStatus(t, st, msgID, 5*time.Second)
	if stFinal != storage.StatusFailed {
		t.Fatalf("expected failed after retries exhausted, got %s (exit=%d err=%q)", stFinal, res.ExitCode, res.Err)
	}

	wantCalls := int32(cfg.MaxRetries)
	gotCalls := fr.calls.Load()
	if gotCalls != wantCalls {
		t.Fatalf("expected runner calls=%d (attempts), got %d", wantCalls, gotCalls)
	}
}

type failRunner struct {
	calls atomic.Int32
}

func (r *failRunner) Run(ctx context.Context, cmdArgs []string, script string, job Job) Result {
	r.calls.Add(1)
	return Result{
		Queue:      job.Queue,
		MsgID:      job.MsgID,
		ExitCode:   1,
		DurationMs: 1,
		Err:        errors.New("always fail"),
	}
}

func TestE2E_HTTP_GetStatusAndResult_AfterJobDone(t *testing.T) {
	ts, qm, cleanup := newE2EServer(t, e2eServerOpts{})
	defer cleanup()

	queue := "queue1"

	msgID, idem := postNewMessage(t, ts.URL, queue, `{"text":"hello"}`, "")
	if msgID == "" || idem == "" {
		t.Fatalf("expected msgID+idemKey, got msgID=%q idem=%q", msgID, idem)
	}

	st, res := waitTerminalStatus(t, mustGetStore(t, qm, "queue1"), msgID, 2*time.Second)

	// GET status
	{
		type statusData struct {
			Queue  string         `json:"queue"`
			MsgID  string         `json:"msg_id"`
			Status storage.Status `json:"status"`
		}

		code, bodyB, ar, hdr := httpGetAPIWithHeaders(t, ts.URL+"/"+queue+"/status/"+msgID)
		body := string(bodyB)

		if code != http.StatusOK {
			t.Fatalf("status endpoint: expected 200, got %d; body=%s", code, body)
		}
		mustCheckJSONHeaders(t, hdr, bodyB, "status endpoint")

		got := mustUnmarshalData[statusData](t, ar, bodyB)

		if got.MsgID != msgID {
			t.Fatalf("status endpoint: expected msg_id=%s, got %s", msgID, got.MsgID)
		}
		if got.Status != st {
			t.Fatalf("status endpoint: expected status=%s, got %s", st, got.Status)
		}
	}

	// GET result
	{
		type resultData struct {
			Queue  string         `json:"queue"`
			MsgID  string         `json:"msg_id"`
			Status storage.Status `json:"status"`
			Result storage.Result `json:"result"`
		}

		code, bodyB, ar, hdr := httpGetAPIWithHeaders(t, ts.URL+"/"+queue+"/result/"+msgID)
		body := string(bodyB)

		if code != http.StatusOK {
			t.Fatalf("result endpoint: expected 200, got %d; body=%s", code, body)
		}
		mustCheckJSONHeaders(t, hdr, bodyB, "result endpoint")

		got := mustUnmarshalData[resultData](t, ar, bodyB)

		if got.MsgID != msgID {
			t.Fatalf("result endpoint: expected msg_id=%s, got %s", msgID, got.MsgID)
		}
		if got.Status != st {
			t.Fatalf("result endpoint: expected status=%s, got %s", st, got.Status)
		}
		if got.Result.ExitCode != res.ExitCode {
			t.Fatalf("result endpoint: expected exit_code=%d, got %d", res.ExitCode, got.Result.ExitCode)
		}
		if got.Result.FinishedAt == 0 {
			t.Fatalf("result endpoint: expected finished_at != 0")
		}
	}
}

func TestE2E_HTTP_InfoEndpoints_ReturnStats(t *testing.T) {
	ts, qm, cleanup := newE2EServer(t, e2eServerOpts{})
	defer cleanup()

	queue := "queue1"

	// создаём 2 задачи, чтобы avg_duration_ms был осмысленным (и succeeded > 0)
	msgID1, _ := postNewMessage(t, ts.URL, queue, `{"text":"info-test-1"}`, "")
	if msgID1 == "" {
		t.Fatalf("expected msgID1")
	}
	msgID2, _ := postNewMessage(t, ts.URL, queue, `{"text":"info-test-2"}`, "")
	if msgID2 == "" {
		t.Fatalf("expected msgID2")
	}

	// дождаться выполнения обоих
	_, _ = waitTerminalStatus(t, mustGetStore(t, qm, "queue1"), msgID1, 2*time.Second)
	_, _ = waitTerminalStatus(t, mustGetStore(t, qm, "queue1"), msgID2, 2*time.Second)

	var fromAll int64

	// 1) GET /info
	{
		type infoAllData struct {
			FromTimeMs int64 `json:"from_time_ms"`
			Queues     []struct {
				Queue         string  `json:"queue"`
				Succeeded     int     `json:"succeeded"`
				Failed        int     `json:"failed"`
				Retries       int     `json:"retries"`
				AvgDurationMs float64 `json:"avg_duration_ms"`
			} `json:"queues"`
		}

		code, bodyB, ar, hdr := httpGetAPIWithHeaders(t, ts.URL+"/info")
		body := string(bodyB)

		if code != http.StatusOK {
			t.Fatalf("/info: expected 200, got %d; body=%s", code, body)
		}
		mustCheckJSONHeaders(t, hdr, bodyB, "/info")

		got := mustUnmarshalData[infoAllData](t, ar, bodyB)

		// from_time_ms: допускаем 0 как дефолт
		if got.FromTimeMs < 0 {
			t.Fatalf("/info: expected from_time_ms >= 0; body=%s", body)
		}
		// раз мы НЕ передавали from_time_ms — ожидаем дефолт 0
		if got.FromTimeMs != 0 {
			t.Fatalf("/info: expected from_time_ms == 0 (default), got %d; body=%s", got.FromTimeMs, body)
		}
		fromAll = got.FromTimeMs

		// сортировка queues по имени
		for i := 1; i < len(got.Queues); i++ {
			if got.Queues[i-1].Queue > got.Queues[i].Queue {
				t.Fatalf("/info: expected queues sorted by queue asc, but %q > %q; body=%s",
					got.Queues[i-1].Queue, got.Queues[i].Queue, body)
			}
		}

		// наличие нужной очереди + avg_duration_ms
		found := false
		for _, q := range got.Queues {
			if q.Queue == queue {
				found = true

				if q.AvgDurationMs < 0 {
					t.Fatalf("/info: expected avg_duration_ms >= 0, got %f; body=%s", q.AvgDurationMs, body)
				}
				if q.Succeeded > 0 && q.AvgDurationMs == 0 {
					t.Fatalf("/info: expected avg_duration_ms > 0 when succeeded > 0; body=%s", body)
				}
				break
			}
		}
		if !found {
			t.Fatalf("/info: expected queue %q in response; body=%s", queue, body)
		}
	}

	// 2) GET /{queue}/info
	{
		type infoOneData struct {
			Queue         string  `json:"queue"`
			Succeeded     int     `json:"succeeded"`
			Failed        int     `json:"failed"`
			Retries       int     `json:"retries"`
			AvgDurationMs float64 `json:"avg_duration_ms"`
			FromTimeMs    int64   `json:"from_time_ms"`
		}

		code, bodyB, ar, hdr := httpGetAPIWithHeaders(t, ts.URL+"/"+queue+"/info")
		body := string(bodyB)

		if code != http.StatusOK {
			t.Fatalf("/%s/info: expected 200, got %d; body=%s", queue, code, body)
		}
		mustCheckJSONHeaders(t, hdr, bodyB, "/{queue}/info")

		got := mustUnmarshalData[infoOneData](t, ar, bodyB)

		if got.Queue != queue {
			t.Fatalf("/%s/info: expected queue=%q, got %q; body=%s", queue, queue, got.Queue, body)
		}

		// from_time_ms: допускаем 0 как дефолт + сверяем с /info
		if got.FromTimeMs < 0 {
			t.Fatalf("/%s/info: expected from_time_ms >= 0; body=%s", queue, body)
		}
		if got.FromTimeMs != fromAll {
			t.Fatalf("/%s/info: expected from_time_ms == /info.from_time_ms (%d), got %d; body=%s",
				queue, fromAll, got.FromTimeMs, body)
		}

		if got.AvgDurationMs < 0 {
			t.Fatalf("/%s/info: expected avg_duration_ms >= 0, got %f; body=%s", queue, got.AvgDurationMs, body)
		}
		if got.Succeeded > 0 && got.AvgDurationMs == 0 {
			t.Fatalf("/%s/info: expected avg_duration_ms > 0 when succeeded > 0; body=%s", queue, body)
		}
	}
}

func TestE2E_HTTP_StatusAndResult_NotFound_ReturnsAPIError(t *testing.T) {
	ts, _, cleanup := newE2EServer(t, e2eServerOpts{})
	defer cleanup()

	queue := "queue1"
	badID := "not-a-real-msg-id"

	// status
	{
		code, bodyB, ar, hdr := httpGetAPIAlwaysJSON(t, ts.URL+"/"+queue+"/status/"+badID)
		body := string(bodyB)

		if code != http.StatusNotFound {
			t.Fatalf("status notfound: expected 404, got %d; body=%s", code, body)
		}
		mustCheckJSONHeaders(t, hdr, bodyB, "status notfound")

		// если у тебя другой код в error.code — поставь его сюда.
		// если не хочешь фиксировать код — передай "" и будет проверка только структуры.
		mustCheckErrorResp(t, ar, bodyB, "")
	}

	// result
	{
		code, bodyB, ar, hdr := httpGetAPIAlwaysJSON(t, ts.URL+"/"+queue+"/result/"+badID)
		body := string(bodyB)

		if code != http.StatusNotFound {
			t.Fatalf("result notfound: expected 404, got %d; body=%s", code, body)
		}
		mustCheckJSONHeaders(t, hdr, bodyB, "result notfound")

		mustCheckErrorResp(t, ar, bodyB, "")
	}
}

func TestE2E_HTTP_InfoEndpoints_FromTimeQuery_IsReflected(t *testing.T) {
	ts, qm, cleanup := newE2EServer(t, e2eServerOpts{})
	defer cleanup()

	queue := "queue1"

	// создаём пару задач, чтобы было что агрегировать
	msgID1, _ := postNewMessage(t, ts.URL, queue, `{"text":"from-time-1"}`, "")
	msgID2, _ := postNewMessage(t, ts.URL, queue, `{"text":"from-time-2"}`, "")
	_, _ = waitTerminalStatus(t, mustGetStore(t, qm, "queue1"), msgID1, 2*time.Second)
	_, _ = waitTerminalStatus(t, mustGetStore(t, qm, "queue1"), msgID2, 2*time.Second)

	// задаём from_time_ms (например: 1 сек назад)
	from := time.Now().Add(-1 * time.Second).UnixMilli()

	// GET /info?from_time_ms=...
	{
		type infoAllData struct {
			FromTimeMs int64 `json:"from_time_ms"`
			Queues     []struct {
				Queue         string  `json:"queue"`
				Succeeded     int     `json:"succeeded"`
				Failed        int     `json:"failed"`
				Retries       int     `json:"retries"`
				AvgDurationMs float64 `json:"avg_duration_ms"`
			} `json:"queues"`
		}

		url := ts.URL + "/info?from_time_ms=" + strconv.FormatInt(from, 10)
		code, bodyB, ar, hdr := httpGetAPIWithHeaders(t, url)
		body := string(bodyB)

		if code != http.StatusOK {
			t.Fatalf("/info?from_time_ms: expected 200, got %d; body=%s", code, body)
		}
		mustCheckJSONHeaders(t, hdr, bodyB, "/info?from_time_ms")

		got := mustUnmarshalData[infoAllData](t, ar, bodyB)

		if got.FromTimeMs != from {
			t.Fatalf("/info?from_time_ms: expected from_time_ms=%d, got %d; body=%s", from, got.FromTimeMs, body)
		}

		// сортировка queues по имени
		for i := 1; i < len(got.Queues); i++ {
			if got.Queues[i-1].Queue > got.Queues[i].Queue {
				t.Fatalf("/info?from_time_ms: expected queues sorted, but %q > %q; body=%s",
					got.Queues[i-1].Queue, got.Queues[i].Queue, body)
			}
		}

		// наличие очереди
		found := false
		for _, q := range got.Queues {
			if q.Queue == queue {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("/info?from_time_ms: expected queue %q present; body=%s", queue, body)
		}
	}

	// GET /{queue}/info?from_time_ms=...
	{
		type infoOneData struct {
			Queue         string  `json:"queue"`
			Succeeded     int     `json:"succeeded"`
			Failed        int     `json:"failed"`
			Retries       int     `json:"retries"`
			AvgDurationMs float64 `json:"avg_duration_ms"`
			FromTimeMs    int64   `json:"from_time_ms"`
		}

		url := ts.URL + "/" + queue + "/info?from_time_ms=" + strconv.FormatInt(from, 10)
		code, bodyB, ar, hdr := httpGetAPIWithHeaders(t, url)
		body := string(bodyB)

		if code != http.StatusOK {
			t.Fatalf("/%s/info?from_time_ms: expected 200, got %d; body=%s", queue, code, body)
		}
		mustCheckJSONHeaders(t, hdr, bodyB, "/{queue}/info?from_time_ms")

		got := mustUnmarshalData[infoOneData](t, ar, bodyB)

		if got.Queue != queue {
			t.Fatalf("/%s/info?from_time_ms: expected queue=%q, got %q; body=%s", queue, queue, got.Queue, body)
		}
		if got.FromTimeMs != from {
			t.Fatalf("/%s/info?from_time_ms: expected from_time_ms=%d, got %d; body=%s", queue, from, got.FromTimeMs, body)
		}
	}
}