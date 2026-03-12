package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go-web-server/internal/config"
	"go-web-server/internal/storage"
	"go-web-server/internal/validate"

	"go.uber.org/zap"
)

type runtimeState struct {
	jobsMu  sync.RWMutex
	jobs    chan Job
	quit    chan struct{}
	wg      sync.WaitGroup
	runners []Runner
	timeout time.Duration
	log     *zap.Logger

	stopOnce sync.Once
	stopped  atomic.Bool

	// protects wg.Add during shutdown (prevents Add racing with Wait)
	bgMu sync.Mutex

	// optional: avoid MkdirAll in hot path
	logDir        string
	closeJobsOnce sync.Once

	// parsed from config
	resultTTL    time.Duration
	enqueueWait  time.Duration
}

type QueueStore interface {
	// Create message with dedup barrier.
	// Returns (guid, created, enqueuedAtMs, error).
	PutNewMessage(
		ctx context.Context,
		queue, guid string,
		body []byte,
		enqueuedAtMs int64,
		expiresAtMs int64,
	) (string, bool, int64, error)

	// Atomically mark that message was actually put into in-memory queue successfully.
	MarkQueued(guid string) error

	// Persist that message must be re-enqueued later (enqueue failed / retry scheduled / stuck recovered).
	// Uses enqueue-failed index (bEnqFail).
	MarkEnqueueFailed(guid string, reason string) error

	// Worker lifecycle
	MarkProcessing(guid string, attempt int) error
	TouchProcessing(guid string) error

	// Retry scheduling (persist before planning retry in memory)
	RequeueForRetry(guid string, ts int64) error

	// Replace message body (worker returned new body via structured stdout)
	UpdateBody(guid string, body []byte) error

	// Finish
	MarkDone(guid string, status storage.Status, res storage.Result, ttl int64) error

	// Read API
	GetStatusAndResult(guid string) (storage.Status, storage.Result, error)
	GetInfo(queueName string, fromTime int64) (storage.QueueInfo, error)

	// Background maintenance
	RequeueStuck(now int64, limit int) (int, error)
	GC(now int64, limit int) ([]string, error)
}

type Runtime struct {
	// Runtime is immutable after NewRuntime.
	Cfg        config.QueueConfig
	Schema     *validate.CompiledSchema
	SchemaJSON json.RawMessage // raw JSON schema for GET /{queue}/schema
	Kind       RuntimeKind
	Command    []string
	ScriptPath string

	FPMNetwork string
	FPMAddress string

	Store QueueStore
	st    *runtimeState
}

func NewRuntime(
	cfg config.QueueConfig,
	store QueueStore,
	baseLog *zap.Logger,
	schema *validate.CompiledSchema,
	cmd []string,
	scriptPath string,
	fpmNetwork string,
	fpmAddress string,
	opts ...RuntimeOption,
) (*Runtime, error) {
	// ---- options ----
	ro := runtimeOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&ro)
		}
	}
	// ---- validate inputs ----
	if strings.TrimSpace(cfg.Name) == "" {
		return nil, fmt.Errorf("queue name is required")
	}

	rtStr := strings.TrimSpace(cfg.Runtime)
	kind := RuntimeKind(rtStr)

	// BACKWARD COMPAT: если runtime не задан, но команда есть — считаем exec
	if kind == "" {
		if len(cmd) > 0 {
			kind = RuntimeExec
			// опционально: rtStr = string(RuntimeExec) // только для логов, cfg менять не обязательно
		} else {
			return nil, fmt.Errorf("runtime is required (php-cgi/php-fpm/exec)")
		}
	}
	switch kind {
	case RuntimePHPFPM, RuntimePHPCGI, RuntimeExec:
		// ok
	default:
		return nil, fmt.Errorf("unsupported runtime %q (expected: php-cgi, php-fpm, exec)", cfg.Runtime)
	}

	if kind != RuntimeExec && strings.TrimSpace(scriptPath) == "" {
		return nil, fmt.Errorf("scriptPath is required for %s", kind)
	}
	if kind == RuntimeExec && len(cmd) == 0 {
		return nil, fmt.Errorf("command is required for exec runtime")
	}

	workers := cfg.Workers
	if workers <= 0 {
		workers = 1
	}

	timeout := 10 * time.Second
	if cfg.TimeoutSec > 0 {
		timeout = time.Duration(cfg.TimeoutSec) * time.Second
	}

	maxQueue := 100
	if cfg.MaxQueue > 0 {
		maxQueue = cfg.MaxQueue
	}

	l := baseLog
	if l == nil {
		l = zap.NewNop()
	}
	l = l.Named("queue_runtime").With(zap.String("queue", cfg.Name))

	rt := &Runtime{
		Cfg:    cfg,
		Schema: schema,

		Kind: kind,

		Command:    append([]string(nil), cmd...), // используется только для exec
		ScriptPath: scriptPath,

		FPMNetwork: fpmNetwork,
		FPMAddress: fpmAddress,

		Store: store,
	}

	// ---- log dir (optional optimization) ----
	var logDir string
	if cfg.LogDir != "" {
		logDir = filepath.Join(cfg.LogDir, cfg.Name)
		if err := os.MkdirAll(logDir, 0o755); err != nil {
			l.Warn("mkdir_queue_log_dir_failed", zap.String("path", logDir), zap.Error(err))
		}
	}

	// ---- parse durations from config ----
	var resultTTL time.Duration
	if strings.TrimSpace(cfg.ResultTTL) != "" {
		var err error
		resultTTL, err = config.ParseDurationExt(cfg.ResultTTL)
		if err != nil {
			return nil, fmt.Errorf("parse result_ttl: %w", err)
		}
	}
	// resultTTL == 0 означает "хранить вечно"

	enqueueWait := time.Duration(cfg.EnqueueWaitMs) * time.Millisecond
	if enqueueWait <= 0 {
		enqueueWait = 100 * time.Millisecond
	}

	// ---- make runners: one-runner-per-worker ----
	factory := ro.runnerFactory
	if factory == nil {
		factory = DefaultRunnerFactory(rt, ro.phpCgiBin)
	}

	runners := make([]Runner, workers)
	for i := 0; i < workers; i++ {
		r, err := factory()
		if err != nil {
			return nil, err
		}
		runners[i] = r
	}

	rt.st = &runtimeState{
		jobs:        make(chan Job, maxQueue),
		quit:        make(chan struct{}),
		runners:     runners,
		timeout:     timeout,
		log:         l,
		logDir:      logDir,
		resultTTL:   resultTTL,
		enqueueWait: enqueueWait,
	}

	// ---- start workers ----
	for wid := 1; wid <= workers; wid++ {
		rt.st.wg.Add(1)
		r := rt.st.runners[wid-1]
		go rt.workerLoop(wid, r)
	}

	// ---- maintenance ----
	rt.st.wg.Add(1)
	go rt.maintenanceLoop()

	rt.st.log.Info("queue_started",
		zap.Int("workers", workers),
		zap.Int("max_queue", maxQueue),
		zap.Int64("timeout_ms", timeout.Milliseconds()),
		zap.String("runner", fmt.Sprintf("%T", runners[0])),
		zap.Int("max_retries", rt.Cfg.MaxRetries),
		zap.Int("retry_delay_ms", rt.Cfg.RetryDelayMs),
	)

	return rt, nil
}

func (rt *Runtime) Enqueue(ctx context.Context, job Job) error {
	if rt.st == nil {
		return fmt.Errorf("%w: queue=%q", ErrRuntimeNotInit, rt.Cfg.Name)
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	// Protect send vs Stop() closing jobs.
	rt.st.jobsMu.RLock()
	defer rt.st.jobsMu.RUnlock()

	// Check AFTER lock to avoid window: check -> close -> send panic.
	if rt.st.stopped.Load() {
		return fmt.Errorf("%w: queue=%q", ErrQueueStopped, rt.Cfg.Name)
	}

	// fast path
	select {
	case rt.st.jobs <- job:
		return nil
	default:
		rt.st.log.Warn("queue_full_waiting_enqueue",
			zap.String("queue", rt.Cfg.Name),
			zap.String("message_guid", job.MessageGUID),
			zap.Int("attempt", job.Attempt),
		)
	}

	wait := rt.st.enqueueWait
	if wait <= 0 {
		wait = 100 * time.Millisecond
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case rt.st.jobs <- job:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-rt.st.quit:
		return fmt.Errorf("%w: queue=%q", ErrQueueStopped, rt.Cfg.Name)
	case <-timer.C:
		return fmt.Errorf("%w: queue=%q", ErrQueueBusy, rt.Cfg.Name)
	}
}

// Stop closes the jobs channel and signals quit. Workers drain the channel but
// skip processing once quit is closed. Persisted messages will be re-enqueued
// by RequeueStuck on next startup (at-least-once delivery).
func (rt *Runtime) Stop() {
	if rt.st == nil {
		return
	}

	rt.st.stopOnce.Do(func() {
		rt.st.stopped.Store(true)

		// 1) Stop accepting new jobs + let workers drain
		rt.st.jobsMu.Lock()
		rt.st.closeJobsOnce.Do(func() { close(rt.st.jobs) })
		rt.st.jobsMu.Unlock()

		// 2) Block scheduleRetry from wg.Add after stop started
		rt.st.bgMu.Lock()
		// nothing else here; we just use the mutex as a barrier
		rt.st.bgMu.Unlock()

		// 3) Broadcast stop to background goroutines
		close(rt.st.quit)
	})

	rt.st.wg.Wait()
	rt.st.log.Info("queue_stopped")
}
func (rt *Runtime) workerLoop(workerID int, runner Runner) {
	defer rt.st.wg.Done()

	scheduleRetry := func(job Job) {
		delay := time.Duration(rt.Cfg.RetryDelayMs) * time.Millisecond

		// Guard wg.Add vs Stop().Wait()
		rt.st.bgMu.Lock()
		if rt.st.stopped.Load() {
			rt.st.bgMu.Unlock()
			return
		}
		rt.st.wg.Add(1)
		rt.st.bgMu.Unlock()

		go func() {
			defer rt.st.wg.Done()

			enqueueSafe := func(j Job) bool {
				// Enqueue already guards against Stop() + close(jobs)
				if err := rt.Enqueue(context.Background(), j); err != nil {
					return false
				}
				return true
			}

			if delay <= 0 {
				select {
				case <-rt.st.quit:
					return
				default:
					_ = enqueueSafe(job)
					return
				}
			}

			t := time.NewTimer(delay)
			defer t.Stop()

			select {
			case <-rt.st.quit:
				return
			case <-t.C:
			}

			select {
			case <-rt.st.quit:
				return
			default:
				if enqueueSafe(job) {
					rt.st.log.Info("job_requeued",
						zap.String("message_guid", job.MessageGUID),
						zap.Int("attempt", job.Attempt),
						zap.Int64("retry_delay_ms", int64(delay/time.Millisecond)),
					)
				}
			}
		}()
	}

	// Graceful: drain jobs until channel is closed.
	for job := range rt.st.jobs {
		// Optional: if we're stopping, skip doing new work fast.
		// (You can remove this block if you want to finish everything no matter what.)
		select {
		case <-rt.st.quit:
			return
		default:
		}

		// FIX: isolate panic per job
		func(job Job) {
			defer func() {
				if r := recover(); r != nil {
					rt.st.log.Error("worker_panic_recovered",
						zap.Any("panic", r),
						zap.String("message_guid", job.MessageGUID),
						zap.Int("worker_id", workerID),
					)

					// optional: don't leave task stuck in processing
					if rt.Store != nil {
						now := time.Now().UnixMilli()
						var ttlMs int64
						if rt.st.resultTTL > 0 {
							ttlMs = time.Now().Add(rt.st.resultTTL).UnixMilli()
						} else {
							ttlMs = -1
						}

						if err := rt.Store.MarkDone(
							job.MessageGUID,
							storage.StatusFailed,
							storage.Result{
								FinishedAt: now,
								ExitCode:   -1,
								DurationMs: 0,
								Err:        "panic recovered",
							},
							ttlMs,
						); err != nil {
							rt.st.log.Error("mark_done_after_panic_failed",
								zap.String("message_guid", job.MessageGUID),
								zap.Error(err),
							)
						}
					}
				}
			}()

			// 1) MarkProcessing
			if rt.Store != nil {
				if err := rt.Store.MarkProcessing(job.MessageGUID, job.Attempt); err != nil {
					rt.st.log.Warn("mark_processing_failed",
						zap.String("message_guid", job.MessageGUID),
						zap.Int("worker_id", workerID),
						zap.Error(err),
					)
					return
				}
			}

			currentCmd := rt.Command
			scriptPath := rt.ScriptPath

			// 3) Execute with timeout
			ctx, cancel := context.WithTimeout(context.Background(), rt.st.timeout)
			res := runner.Run(ctx, currentCmd, scriptPath, job)
			cancel()

			// Parse structured stdout: {"output":"...","body":{...}}
			ws := parseWorkerStdout(res.Stdout)

			// If worker returned a new body — update storage and in-memory job (affects retries)
			if len(ws.Body) > 0 && rt.Store != nil {
				if err := rt.Store.UpdateBody(job.MessageGUID, []byte(ws.Body)); err != nil {
					rt.st.log.Warn("update_body_failed",
						zap.String("message_guid", job.MessageGUID),
						zap.Error(err),
					)
				} else {
					job.Body = []byte(ws.Body)
				}
			}

			rt.appendStdStreams(job, res.Stdout, res.Stderr)

			failed := (res.ExitCode != 0 || res.Err != nil)

			// 4) Retry
			if failed && job.Attempt < rt.Cfg.MaxRetries {
				next := job
				next.Attempt = job.Attempt + 1

				// Persist retry before scheduling in memory
				if rt.Store != nil {
					err := rt.Store.RequeueForRetry(job.MessageGUID, time.Now().UnixMilli())
					if err != nil {
						rt.st.log.Error("failed_to_persist_retry_status",
							zap.String("message_guid", job.MessageGUID),
							zap.Error(err),
						)
						return
					}
				}

				scheduleRetry(next)

				rt.st.log.Warn("job_retry_persistent_ok",
					zap.String("message_guid", job.MessageGUID),
					zap.Int("to_attempt", next.Attempt),
				)
				return
			}

			// 5) Finalize
			status := storage.StatusSucceeded
			if failed {
				status = storage.StatusFailed
			}

			if rt.Store != nil {
				now := time.Now()
				var ttlMs int64
				if rt.st.resultTTL > 0 {
					ttlMs = now.Add(rt.st.resultTTL).UnixMilli()
				} else {
					ttlMs = -1 // sentinel: убрать expiration, хранить вечно
				}

				err := rt.Store.MarkDone(
					job.MessageGUID,
					status,
					storage.Result{
						FinishedAt: now.UnixMilli(),
						ExitCode:   res.ExitCode,
						DurationMs: res.DurationMs,
						Err:        errString(res.Err),
						Output:     ws.Output,
					},
					ttlMs,
				)

				if err != nil {
					rt.st.log.Warn("mark_done_failed",
						zap.String("message_guid", job.MessageGUID),
						zap.Int("attempt", job.Attempt),
						zap.String("final_status", string(status)),
						zap.Error(err),
					)
				} else {
					rt.st.log.Info("mark_done_ok",
						zap.String("message_guid", job.MessageGUID),
						zap.Int("attempt", job.Attempt),
						zap.String("final_status", string(status)),
						zap.Int("exit_code", res.ExitCode),
						zap.Int64("duration_ms", res.DurationMs),
						zap.Int64("ttl_ms", ttlMs),
					)
				}
			}

			// 6) Log
			fields := []zap.Field{
				zap.String("message_guid", job.MessageGUID),
				zap.Int("worker_id", workerID),
				zap.Int("exit_code", res.ExitCode),
				zap.Int("attempt", job.Attempt),
				zap.Int64("duration_ms", res.DurationMs),
			}
			if res.Err != nil {
				fields = append(fields, zap.Error(res.Err))
				rt.st.log.Warn("job_done", fields...)
			} else {
				rt.st.log.Info("job_done", fields...)
			}
		}(job)
	}
}
func (rt *Runtime) maintenanceLoop() {
	defer rt.st.wg.Done()

	requeueInterval := time.Duration(rt.Cfg.RequeueStuckIntervalSec) * time.Second
	if requeueInterval <= 0 {
		requeueInterval = 15 * time.Second
	}
	gcInterval := time.Duration(rt.Cfg.Storage.GCIntervalSec) * time.Second
	if gcInterval <= 0 {
		gcInterval = 1 * time.Minute
	}

	requeueTicker := time.NewTicker(requeueInterval)
	gcTicker := time.NewTicker(gcInterval)
	defer requeueTicker.Stop()
	defer gcTicker.Stop()

	requeueBatch := rt.Cfg.RequeueStuckBatchLimit
	if requeueBatch <= 0 {
		requeueBatch = 200
	}
	gcBatch := rt.Cfg.GCBatchLimit
	if gcBatch <= 0 {
		gcBatch = 500
	}

	for {
		select {
		case <-rt.st.quit:
			return

		case <-requeueTicker.C:
			if rt.Store != nil {
				n, err := rt.Store.RequeueStuck(0, requeueBatch)
				if err != nil {
					rt.st.log.Warn("requeue_stuck_failed", zap.Error(err))
				} else if n > 0 {
					rt.st.log.Info("requeue_stuck_done", zap.Int("count", n))
				}
			}

		case <-gcTicker.C:
			if rt.Store != nil {
				deletedIDs, err := rt.Store.GC(0, gcBatch)
				if err != nil {
					rt.st.log.Warn("gc_failed", zap.Error(err))
				} else if len(deletedIDs) > 0 {
					rt.st.log.Info("gc_done", zap.Int("deleted", len(deletedIDs)))
					rt.cleanupLogFiles(deletedIDs)
				}
			}
		}
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// appendStdStreams writes stdout/stderr to per-message log.
// MkdirAll is avoided in hot path by creating rt.st.logDir in NewRuntime.
func (rt *Runtime) appendStdStreams(job Job, out, errb []byte) {
	if len(out) == 0 && len(errb) == 0 {
		return
	}
	if rt.st == nil {
		return
	}

	dir := rt.st.logDir
	if dir == "" {
		dir = filepath.Join(rt.Cfg.LogDir, job.Queue)
	}

	logPath := filepath.Join(dir, job.MessageGUID+".log")
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		rt.st.log.Warn("open_job_log_failed", zap.String("path", logPath), zap.Error(err))
		return
	}
	defer f.Close()

	_, _ = fmt.Fprintf(f, "\n=== ATTEMPT %d @ %s ===\n", job.Attempt, time.Now().Format(time.RFC3339))

	if len(out) > 0 {
		_, _ = f.WriteString("=== STDOUT ===\n")
		_, _ = f.Write(out)
		if out[len(out)-1] != '\n' {
			_, _ = f.WriteString("\n")
		}
	}
	if len(errb) > 0 {
		_, _ = f.WriteString("=== STDERR ===\n")
		_, _ = f.Write(errb)
		if errb[len(errb)-1] != '\n' {
			_, _ = f.WriteString("\n")
		}
	}
}

// cleanupLogFiles removes log files for messages deleted by GC.
func (rt *Runtime) cleanupLogFiles(msgIDs []string) {
	if rt.st == nil {
		return
	}
	dir := rt.st.logDir
	if dir == "" {
		return
	}
	for _, id := range msgIDs {
		p := filepath.Join(dir, id+".log")
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			rt.st.log.Warn("remove_job_log_failed", zap.String("path", p), zap.Error(err))
		}
	}
}
