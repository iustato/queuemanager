package queue

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/zap"
	"go-web-server/internal/config"
	"go-web-server/internal/validate"

	"github.com/google/uuid"
)

// writeTempSchema writes schema JSON to a temp file and returns its path.
func writeTempSchema(t *testing.T, schema string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "schema.json")
	if err := os.WriteFile(path, []byte(schema), 0o600); err != nil {
		t.Fatalf("write schema: %v", err)
	}
	return path
}

func mustLoadSchema(t *testing.T, schema string) *validate.CompiledSchema {
	t.Helper()
	path := writeTempSchema(t, schema)
	cs, err := validate.LoadSchemaFromFile(path)
	if err != nil {
		t.Fatalf("LoadSchemaFromFile: %v", err)
	}
	return cs
}

// Minimal manager setup: put runtime into manager map so svc can find it.
// Adjust if your Manager fields differ.
func newTestManagerWithRuntime(queue string, rt *Runtime) *Manager {
	return &Manager{
		queues: map[string]*Runtime{
			queue: rt,
		},
		allowAutoGUID: true,
	}
}

func newTestRuntime(t *testing.T, queue string, st QueueStore, schema *validate.CompiledSchema, maxSize int) *Runtime {
	t.Helper()

	rt := &Runtime{
		Cfg: config.QueueConfig{
			Name:    queue,
			MaxSize: maxSize,
		},
		Store:  st,
		Schema: schema,
	}

	// minimal state so Enqueue works (adjust field names if differ)
	rt.st = &runtimeState{
		jobs: make(chan Job, 16),
		quit: make(chan struct{}),
	}
	return rt
}

func TestNormalizeMessageGUID_Empty_GeneratesUUIDv7(t *testing.T) {
	guid, err := normalizeMessageGUID("", true)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	u, err := uuid.Parse(guid)
	if err != nil {
		t.Fatalf("generated guid is not uuid: %v", err)
	}
	if u.Version() != uuid.Version(7) {
		t.Fatalf("expected uuidv7, got v%d", u.Version())
	}
}

func TestNormalizeMessageGUID_Empty_AutoDisabled_ReturnsError(t *testing.T) {
	_, err := normalizeMessageGUID("", false)
	var e ErrInvalidMessageGUID
	if !errors.As(err, &e) {
		t.Fatalf("expected ErrInvalidMessageGUID, got %T: %v", err, err)
	}
}

func TestQueueService_Push_QueueNotFound(t *testing.T) {
	mgr := &Manager{queues: map[string]*Runtime{}}
	svc := NewQueueService(mgr)

	_, err := svc.Push(context.Background(), PushRequest{
		Queue: "nope",
		Body:  []byte(`{"x":1}`),
	})
	if !errors.Is(err, ErrQueueNotFound) {
		t.Fatalf("expected ErrQueueNotFound, got %T: %v", err, err)
	}
}

func TestQueueService_Push_InvalidJSON(t *testing.T) {
	ms := newMockStore()

	// Allow any object; invalid JSON will be caught before schema rules.
	schema := mustLoadSchema(t, `{"type":"object"}`)
	rt := newTestRuntime(t, "q1", ms, schema, 0)

	mgr := newTestManagerWithRuntime("q1", rt)
	svc := NewQueueService(mgr)

	_, err := svc.Push(context.Background(), PushRequest{
		Queue: "q1",
		Body:  []byte(`{`), // invalid JSON
	})
	var e ErrInvalidJSON
	if !errors.As(err, &e) {
		t.Fatalf("expected ErrInvalidJSON, got %T: %v", err, err)
	}
}

func TestQueueService_Push_SchemaViolation(t *testing.T) {
	ms := newMockStore()

	// Require x to be a number
	schema := mustLoadSchema(t, `{
		"type":"object",
		"properties":{"x":{"type":"number"}},
		"required":["x"]
	}`)
	rt := newTestRuntime(t, "q1", ms, schema, 0)

	mgr := newTestManagerWithRuntime("q1", rt)
	svc := NewQueueService(mgr)

	_, err := svc.Push(context.Background(), PushRequest{
		Queue: "q1",
		Body:  []byte(`{"x":"nope"}`), // violates schema
	})
	var e ErrSchemaValidation
	if !errors.As(err, &e) {
		t.Fatalf("expected ErrSchemaValidation, got %T: %v", err, err)
	}
}

func TestQueueService_Push_TooLarge(t *testing.T) {
	ms := newMockStore()

	schema := mustLoadSchema(t, `{"type":"object"}`)
	rt := newTestRuntime(t, "q1", ms, schema, 3) // MaxSize=3 bytes

	mgr := newTestManagerWithRuntime("q1", rt)
	svc := NewQueueService(mgr)

	_, err := svc.Push(context.Background(), PushRequest{
		Queue: "q1",
		Body:  []byte(`{"x":1}`), // > 3 bytes
	})
	var e ErrPayloadTooLarge
	if !errors.As(err, &e) {
		t.Fatalf("expected ErrPayloadTooLarge, got %T: %v", err, err)
	}
}

func TestQueueService_Push_Dedup_ReturnsCreatedFalse_AndDoesNotEnqueue(t *testing.T) {
	ms := newMockStore()

	schema := mustLoadSchema(t, `{"type":"object"}`)
	rt := newTestRuntime(t, "q1", ms, schema, 0)

	mgr := newTestManagerWithRuntime("q1", rt)
	svc := NewQueueService(mgr)

	// generate valid UUIDv7 for MessageGUID
	guid := uuid.Must(uuid.NewV7()).String()

	// 1) first push => created true
	_, err := svc.Push(context.Background(), PushRequest{
		Queue:       "q1",
		Body:        []byte(`{"x":1}`),
		MessageGUID: guid,
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// drain 1 job
	select {
	case <-rt.st.jobs:
	default:
		t.Fatalf("expected job enqueued for first push")
	}

	// 2) second push with same guid => created false and must not enqueue
	resp2, err := svc.Push(context.Background(), PushRequest{
		Queue:       "q1",
		Body:        []byte(`{"x":1}`),
		MessageGUID: guid,
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if resp2.Created {
		t.Fatalf("expected Created=false on dedup")
	}

	select {
	case <-rt.st.jobs:
		t.Fatalf("did not expect enqueue on dedup")
	default:
	}
}

func TestQueueService_Push_Success_MarkQueuedCalled(t *testing.T) {
	ms := newMockStore()

	schema := mustLoadSchema(t, `{"type":"object"}`)
	rt := newTestRuntime(t, "q1", ms, schema, 0)

	mgr := newTestManagerWithRuntime("q1", rt)
	svc := NewQueueService(mgr)

	resp, err := svc.Push(context.Background(), PushRequest{
		Queue:  "q1",
		Body:   []byte(`{"x":1}`),
		Method: "POST",
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if resp.MessageGUID == "" {
		t.Fatalf("expected MessageGUID, got %+v", resp)
	}

	// job must be in memory queue
	select {
	case job := <-rt.st.jobs:
		if job.MessageGUID != resp.MessageGUID {
			t.Fatalf("job.MessageGUID mismatch: got %s want %s", job.MessageGUID, resp.MessageGUID)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("expected job enqueued")
	}

	// MarkQueued must be called once
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if len(ms.markQueuedCalls) != 1 {
		t.Fatalf("expected MarkQueuedCalls=1, got %d", len(ms.markQueuedCalls))
	}
	if ms.markQueuedCalls[0] != resp.MessageGUID {
		t.Fatalf("MarkQueued guid mismatch: got %s want %s", ms.markQueuedCalls[0], resp.MessageGUID)
	}
}

// compile-time check
var _ QueueStore = (*mockStore)(nil)

func TestRuntime_Enqueue_Full_ReturnsQueueBusy(t *testing.T) {
	rt := &Runtime{
		Cfg: config.QueueConfig{Name: "q1"},
	}
	rt.st = &runtimeState{
		jobs: make(chan Job),
		quit: make(chan struct{}),
		log:  zap.NewNop(),
	}

	ctx := context.Background()
	err := rt.Enqueue(ctx, Job{Queue: "q1", MessageGUID: "m1", Attempt: 1})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !errors.Is(err, ErrQueueBusy) {
		t.Fatalf("expected ErrQueueBusy, got %T: %v", err, err)
	}
}

func TestRuntime_Enqueue_ContextTimeout(t *testing.T) {
	rt := &Runtime{Cfg: config.QueueConfig{Name: "q1"}}
	rt.st = &runtimeState{
		jobs: make(chan Job), // unbuffered so it will block
		quit: make(chan struct{}),
		log:  zap.NewNop(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := rt.Enqueue(ctx, Job{Queue: "q1", MessageGUID: "m1", Attempt: 1})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %T: %v", err, err)
	}
}

func TestRuntime_Enqueue_Stopped_ReturnsQueueStopped(t *testing.T) {
	rt := &Runtime{Cfg: config.QueueConfig{Name: "q1"}}
	rt.st = &runtimeState{
		jobs: make(chan Job),
		quit: make(chan struct{}),
		log:  zap.NewNop(),
	}
	close(rt.st.quit)

	err := rt.Enqueue(context.Background(), Job{Queue: "q1", MessageGUID: "m1", Attempt: 1})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !errors.Is(err, ErrQueueStopped) {
		t.Fatalf("expected ErrQueueStopped, got %T: %v", err, err)
	}
}

func TestQueueService_Push_WhenEnqueueFails_DoesNotMarkQueued(t *testing.T) {
	ms := newMockStore()

	schema := mustLoadSchema(t, `{"type":"object"}`)

	rt := &Runtime{
		Cfg:    config.QueueConfig{Name: "q1"},
		Store:  ms,
		Schema: schema,
	}
	rt.st = &runtimeState{
		jobs: make(chan Job), // unbuffered => enqueue likely ErrQueueBusy
		quit: make(chan struct{}),
		log:  zap.NewNop(),
	}

	mgr := newTestManagerWithRuntime("q1", rt)
	svc := NewQueueService(mgr)

	errCh := make(chan error, 1)
	go func() {
		_, err := svc.Push(context.Background(), PushRequest{
			Queue:  "q1",
			Body:   []byte(`{"x":1}`),
			Method: "POST",
		})
		errCh <- err
	}()

	err := <-errCh
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !errors.Is(err, ErrQueueBusy) {
		t.Fatalf("expected ErrQueueBusy, got %T: %v", err, err)
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if len(ms.markQueuedCalls) != 0 {
		t.Fatalf("expected MarkQueued not called on enqueue fail, got %d calls", len(ms.markQueuedCalls))
	}
}
