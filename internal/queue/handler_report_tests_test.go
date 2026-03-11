package queue

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go-web-server/internal/config"
	"go-web-server/internal/storage"

	"go.uber.org/zap"
)

// ---------------------------------------------------------------------------
// P4.1  HandleReportDone
// ---------------------------------------------------------------------------

func setupReportDoneManager(t *testing.T) (*Manager, string) {
	t.Helper()
	dir := t.TempDir()
	m := NewManager(zap.NewNop(), dir, storage.OpenOptions{}, "", false, "")
	cfg := config.QueueConfig{
		Name:    "q1",
		Workers: 0,
		Runtime: testRuntimeCmd(),
		Script:  "dummy.php",
	}
	if err := m.AddQueue(cfg, nil); err != nil {
		t.Fatalf("AddQueue: %v", err)
	}
	return m, "q1"
}

func doReportDone(m *Manager, queue, msgID string, body string, headers map[string]string) *httptest.ResponseRecorder {
	var r *http.Request
	if body != "" {
		r = httptest.NewRequest(http.MethodPost, "/internal/"+queue+"/done/"+msgID, strings.NewReader(body))
	} else {
		r = httptest.NewRequest(http.MethodPost, "/internal/"+queue+"/done/"+msgID, nil)
	}
	r.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	m.HandleReportDone(queue, msgID, w, r)
	return w
}

func TestHandleReportDone_EmptyBody_Returns400(t *testing.T) {
	m, q := setupReportDoneManager(t)
	w := doReportDone(m, q, "msg1", "", nil)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d; body=%s", w.Code, w.Body.String())
	}
}

func TestHandleReportDone_WhitespaceBody_Returns400(t *testing.T) {
	m, q := setupReportDoneManager(t)
	w := doReportDone(m, q, "msg1", "   \n  ", nil)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d; body=%s", w.Code, w.Body.String())
	}
}

func TestHandleReportDone_UnknownQueue_Returns404(t *testing.T) {
	m, _ := setupReportDoneManager(t)
	w := doReportDone(m, "nonexistent", "msg1", `{"exit_code":0}`, nil)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d; body=%s", w.Code, w.Body.String())
	}
}

func TestHandleReportDone_MissingQueueOrMsgID_Returns400(t *testing.T) {
	m, _ := setupReportDoneManager(t)

	// empty queue
	w := doReportDone(m, "", "msg1", `{"exit_code":0}`, nil)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("empty queue: expected 400, got %d", w.Code)
	}

	// empty msgID
	w = doReportDone(m, "q1", "", `{"exit_code":0}`, nil)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("empty msgID: expected 400, got %d", w.Code)
	}
}

func TestHandleReportDone_InvalidJSON_Returns400(t *testing.T) {
	m, q := setupReportDoneManager(t)
	w := doReportDone(m, q, "msg1", `{not json}`, nil)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d; body=%s", w.Code, w.Body.String())
	}
}

func TestHandleReportDone_UnknownFields_Returns400(t *testing.T) {
	m, q := setupReportDoneManager(t)
	w := doReportDone(m, q, "msg1", `{"exit_code":0,"unknown_field":"x"}`, nil)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for unknown fields, got %d; body=%s", w.Code, w.Body.String())
	}
}

func TestHandleReportDone_Success_MarksDone(t *testing.T) {
	m, q := setupReportDoneManager(t)

	// First put a message into storage so MarkDone has something to update
	rt, ok := m.Get(q)
	if !ok {
		t.Fatal("queue not found")
	}
	_, _, _, err := rt.Store.PutNewMessage(
		t.Context(),
		q, "msg1", []byte(`{}`), time.Now().UnixMilli(), 0,
	)
	if err != nil {
		t.Fatalf("PutNewMessage: %v", err)
	}
	_ = rt.Store.MarkProcessing("msg1", 1)

	w := doReportDone(m, q, "msg1", `{"exit_code":0,"duration_ms":42}`, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body=%s", w.Code, w.Body.String())
	}

	var resp apiResp
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if !resp.OK {
		t.Fatalf("expected ok=true, got false; body=%s", w.Body.String())
	}
}

func TestHandleReportDone_ExitCodeNonZero_StatusFailed(t *testing.T) {
	m, q := setupReportDoneManager(t)
	rt, _ := m.Get(q)
	_, _, _, _ = rt.Store.PutNewMessage(
		t.Context(),
		q, "msg2", []byte(`{}`), time.Now().UnixMilli(), 0,
	)
	_ = rt.Store.MarkProcessing("msg2", 1)

	w := doReportDone(m, q, "msg2", `{"exit_code":1,"err":"fail"}`, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body=%s", w.Code, w.Body.String())
	}

	// verify the message is now failed in storage
	st, _, err := rt.Store.GetStatusAndResult("msg2")
	if err != nil {
		t.Fatalf("GetStatusAndResult: %v", err)
	}
	if st != storage.StatusFailed {
		t.Fatalf("expected status=failed, got %s", st)
	}
}

func TestHandleReportDone_WorkerToken_Unauthorized(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(zap.NewNop(), dir, storage.OpenOptions{}, "secret123", false, "")
	cfg := config.QueueConfig{
		Name:    "q1",
		Workers: 0,
		Runtime: testRuntimeCmd(),
		Script:  "dummy.php",
	}
	if err := m.AddQueue(cfg, nil); err != nil {
		t.Fatalf("AddQueue: %v", err)
	}

	// no token
	w := doReportDone(m, "q1", "msg1", `{"exit_code":0}`, nil)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("no token: expected 401, got %d; body=%s", w.Code, w.Body.String())
	}

	// wrong token
	w = doReportDone(m, "q1", "msg1", `{"exit_code":0}`, map[string]string{
		"X-Worker-Token": "wrong",
	})
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token: expected 401, got %d; body=%s", w.Code, w.Body.String())
	}

	// correct token via X-Worker-Token
	rt, _ := m.Get("q1")
	_, _, _, _ = rt.Store.PutNewMessage(
		t.Context(),
		"q1", "msg1", []byte(`{}`), time.Now().UnixMilli(), 0,
	)
	_ = rt.Store.MarkProcessing("msg1", 1)

	w = doReportDone(m, "q1", "msg1", `{"exit_code":0}`, map[string]string{
		"X-Worker-Token": "secret123",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("correct token: expected 200, got %d; body=%s", w.Code, w.Body.String())
	}
}

func TestHandleReportDone_BearerToken_Authorized(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(zap.NewNop(), dir, storage.OpenOptions{}, "tok456", false, "")
	cfg := config.QueueConfig{
		Name:    "q1",
		Workers: 0,
		Runtime: testRuntimeCmd(),
		Script:  "dummy.php",
	}
	_ = m.AddQueue(cfg, nil)

	rt, _ := m.Get("q1")
	_, _, _, _ = rt.Store.PutNewMessage(
		t.Context(),
		"q1", "msg1", []byte(`{}`), time.Now().UnixMilli(), 0,
	)
	_ = rt.Store.MarkProcessing("msg1", 1)

	w := doReportDone(m, "q1", "msg1", `{"exit_code":0}`, map[string]string{
		"Authorization": "Bearer tok456",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("bearer: expected 200, got %d; body=%s", w.Code, w.Body.String())
	}
}

// ---------------------------------------------------------------------------
// P4.3  appendStdStreams
// ---------------------------------------------------------------------------

func TestAppendStdStreams_WritesLogFile(t *testing.T) {
	logDir := t.TempDir()
	dir := t.TempDir()
	m := NewManager(zap.NewNop(), dir, storage.OpenOptions{}, "", false, "")
	cfg := config.QueueConfig{
		Name:    "q1",
		Workers: 0,
		Runtime: testRuntimeCmd(),
		Script:  "dummy.php",
		LogDir:  logDir,
	}
	if err := m.AddQueue(cfg, nil); err != nil {
		t.Fatalf("AddQueue: %v", err)
	}

	rt, _ := m.Get("q1")
	job := Job{Queue: "q1", MessageGUID: "test-msg", Attempt: 1}
	rt.appendStdStreams(job, []byte("hello stdout"), []byte("hello stderr"))

	logPath := filepath.Join(logDir, "q1", "test-msg.log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}

	content := string(data)
	if !strings.Contains(content, "STDOUT") {
		t.Fatalf("expected STDOUT marker in log, got: %s", content)
	}
	if !strings.Contains(content, "hello stdout") {
		t.Fatalf("expected stdout content in log, got: %s", content)
	}
	if !strings.Contains(content, "STDERR") {
		t.Fatalf("expected STDERR marker in log, got: %s", content)
	}
	if !strings.Contains(content, "hello stderr") {
		t.Fatalf("expected stderr content in log, got: %s", content)
	}
	if !strings.Contains(content, "ATTEMPT 1") {
		t.Fatalf("expected ATTEMPT 1 in log, got: %s", content)
	}
}

func TestAppendStdStreams_EmptyOutput_NoFile(t *testing.T) {
	logDir := t.TempDir()
	dir := t.TempDir()
	m := NewManager(zap.NewNop(), dir, storage.OpenOptions{}, "", false, "")
	cfg := config.QueueConfig{
		Name:    "q1",
		Workers: 0,
		Runtime: testRuntimeCmd(),
		Script:  "dummy.php",
		LogDir:  logDir,
	}
	_ = m.AddQueue(cfg, nil)

	rt, _ := m.Get("q1")
	job := Job{Queue: "q1", MessageGUID: "empty-msg", Attempt: 1}
	rt.appendStdStreams(job, nil, nil)

	logPath := filepath.Join(logDir, "q1", "empty-msg.log")
	if _, err := os.Stat(logPath); !os.IsNotExist(err) {
		t.Fatalf("expected no log file for empty output, but file exists")
	}
}

func TestAppendStdStreams_AppendsMultipleAttempts(t *testing.T) {
	logDir := t.TempDir()
	dir := t.TempDir()
	m := NewManager(zap.NewNop(), dir, storage.OpenOptions{}, "", false, "")
	cfg := config.QueueConfig{
		Name:    "q1",
		Workers: 0,
		Runtime: testRuntimeCmd(),
		Script:  "dummy.php",
		LogDir:  logDir,
	}
	_ = m.AddQueue(cfg, nil)

	rt, _ := m.Get("q1")
	job1 := Job{Queue: "q1", MessageGUID: "multi-msg", Attempt: 1}
	rt.appendStdStreams(job1, []byte("attempt1"), nil)

	job2 := Job{Queue: "q1", MessageGUID: "multi-msg", Attempt: 2}
	rt.appendStdStreams(job2, []byte("attempt2"), nil)

	logPath := filepath.Join(logDir, "q1", "multi-msg.log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "ATTEMPT 1") || !strings.Contains(content, "ATTEMPT 2") {
		t.Fatalf("expected both attempts in log, got: %s", content)
	}
	if !strings.Contains(content, "attempt1") || !strings.Contains(content, "attempt2") {
		t.Fatalf("expected both attempt outputs in log, got: %s", content)
	}
}

// ---------------------------------------------------------------------------
// P4.4  Concurrent Manager mutations
// ---------------------------------------------------------------------------

func TestManager_ConcurrentAddDeleteReplace(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(zap.NewNop(), dir, storage.OpenOptions{}, "", false, "")

	const N = 20
	var wg sync.WaitGroup

	// concurrent AddQueue with unique names
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			cfg := config.QueueConfig{
				Name:    "q" + padInt(i),
				Workers: 0,
				Runtime: testRuntimeCmd(),
				Script:  "dummy.php",
			}
			_ = m.AddQueue(cfg, nil)
		}(i)
	}
	wg.Wait()

	names := m.ListNames()
	if len(names) != N {
		t.Fatalf("expected %d queues, got %d", N, len(names))
	}

	// concurrent ReplaceQueue
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			cfg := config.QueueConfig{
				Name:    "q" + padInt(i),
				Workers: 0,
				Runtime: testRuntimeCmd(),
				Script:  "replaced.php",
			}
			_ = m.ReplaceQueue(cfg, nil)
		}(i)
	}
	wg.Wait()

	names = m.ListNames()
	if len(names) != N {
		t.Fatalf("after replace: expected %d queues, got %d", N, len(names))
	}

	// concurrent DeleteQueue
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			m.DeleteQueue("q" + padInt(i))
		}(i)
	}
	wg.Wait()

	names = m.ListNames()
	if len(names) != 0 {
		t.Fatalf("after delete: expected 0 queues, got %d", len(names))
	}
}

func TestManager_ConcurrentAddAndGet(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(zap.NewNop(), dir, storage.OpenOptions{}, "", false, "")

	const N = 20
	var wg sync.WaitGroup

	// add half the queues first
	for i := 0; i < N/2; i++ {
		cfg := config.QueueConfig{
			Name:    "q" + padInt(i),
			Workers: 0,
			Runtime: testRuntimeCmd(),
			Script:  "dummy.php",
		}
		_ = m.AddQueue(cfg, nil)
	}

	// concurrently add more + Get existing
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			if i < N/2 {
				// Get existing
				rt, ok := m.Get("q" + padInt(i))
				if !ok || rt == nil {
					t.Errorf("expected q%s to exist", padInt(i))
				}
			} else {
				// Add new
				cfg := config.QueueConfig{
					Name:    "q" + padInt(i),
					Workers: 0,
					Runtime: testRuntimeCmd(),
					Script:  "dummy.php",
				}
				_ = m.AddQueue(cfg, nil)
			}
		}(i)
	}
	wg.Wait()

	names := m.ListNames()
	if len(names) != N {
		t.Fatalf("expected %d queues, got %d", N, len(names))
	}

	m.StopAll()
}

func TestManager_ConcurrentStopAll(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(zap.NewNop(), dir, storage.OpenOptions{}, "", false, "")

	for i := 0; i < 5; i++ {
		cfg := config.QueueConfig{
			Name:    "q" + padInt(i),
			Workers: 0,
			Runtime: testRuntimeCmd(),
			Script:  "dummy.php",
		}
		_ = m.AddQueue(cfg, nil)
	}

	// call StopAll concurrently — must not panic or deadlock
	var wg sync.WaitGroup
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func() {
			defer wg.Done()
			m.StopAll()
		}()
	}
	wg.Wait()
}

func padInt(i int) string {
	return bytes.NewBufferString("").String() + string(rune('A'+i%26)) + string(rune('0'+i/26))
}

// ---------------------------------------------------------------------------
// P4.2  maintenanceLoop — verify RequeueStuck and GC are called
// ---------------------------------------------------------------------------

func TestMaintenanceLoop_CallsRequeueStuckAndGC(t *testing.T) {
	ms := newMockStore()
	dir := t.TempDir()

	cfg := config.QueueConfig{
		Name:       "maint",
		Workers:    1,
		Runtime:    "exec",
		Script:     "",
		TimeoutSec: 1,
		Storage: config.StorageConfig{
			GCIntervalSec: 1, // 1 second — fast for test
		},
		RequeueStuckIntervalSec: 1,
	}

	m := NewManager(zap.NewNop(), dir, storage.OpenOptions{}, "", false, "")
	// We need to use NewRuntime directly with a mock store
	rt, err := NewRuntime(
		cfg,
		ms,
		zap.NewNop(),
		nil,
		[]string{testRuntimeCmd()},
		"",
		"", "",
	)
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	_ = m // manager not strictly needed here, runtime runs independently

	// Wait for maintenance loop to run at least once
	time.Sleep(2500 * time.Millisecond)

	rt.Stop()

	ms.mu.Lock()
	requeueCalled := ms.requeueStuckCalled
	gcCalled := ms.gcCalled
	ms.mu.Unlock()

	if requeueCalled == 0 {
		t.Fatal("expected RequeueStuck to be called at least once")
	}
	if gcCalled == 0 {
		t.Fatal("expected GC to be called at least once")
	}
}
