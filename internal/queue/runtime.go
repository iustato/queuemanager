package queue

import (
    "bytes"
    "context"
    "encoding/binary"
    "errors"
    "fmt"
    "io"
    "net"
    "os"
    "os/exec"
    "path/filepath"
    "strconv"
    "strings"
    "sync"
    "sync/atomic"
    "time"

    "go-web-server/internal/config"
    "go-web-server/internal/storage"
    "go-web-server/internal/validate"

    "go.uber.org/zap"
)

type Job struct {
	Queue          string
	MsgID          string
	Body           []byte
	EnqueuedAt     time.Time
	Attempt        int
	Method         string
	QueryString    string
	ContentType    string
	RemoteAddr     string
	IdempotencyKey string
}

type Result struct {
	Queue      string
	MsgID      string
	ExitCode   int
	DurationMs int64
	Err        error
	Stdout     []byte
	Stderr     []byte
}

type Runner interface {
	Run(ctx context.Context, cmdArgs []string, script string, job Job) Result
}

// ========================= Exec (php-cgi, anything executable) =========================

type ExecRunner struct{}

func (ExecRunner) Run(ctx context.Context, cmdArgs []string, script string, job Job) Result {
    start := time.Now()

    // 1. Проверка команды
    if len(cmdArgs) == 0 {
        return Result{
            Queue:      job.Queue,
            MsgID:      job.MsgID,
            ExitCode:   1,
            Err:        fmt.Errorf("no command provided"),
            DurationMs: time.Since(start).Milliseconds(),
        }
    }

    // 2. Запрет php-fpm здесь (он должен идти через FastCGIRunner)
    if cmdArgs[0] == "php-fpm" {
        return Result{
            Queue:      job.Queue,
            MsgID:      job.MsgID,
            ExitCode:   1,
            Err:        fmt.Errorf("php-fpm must be executed via FastCGIRunner, not ExecRunner"),
            DurationMs: time.Since(start).Milliseconds(),
        }
    }

    // 3. Создаем команду ОС
    // Используем cmdArgs[0] как исполняемый файл, остальное как аргументы
    cmd := exec.CommandContext(ctx, cmdArgs[0], cmdArgs[1:]...)
    cmd.Stdin = bytes.NewReader(job.Body)

    // 4. Логика для php-cgi (подготовка окружения)
    if cmdArgs[0] == "php-cgi" {
        env, err := buildPhpEnvStandalone(script, job) 
        if err != nil {
            return Result{
                Queue: job.Queue, MsgID: job.MsgID, ExitCode: 1, Err: err,
                DurationMs: time.Since(start).Milliseconds(),
            }
        }
        cmd.Env = append(os.Environ(), env...)
    }

    var stdout, stderr bytes.Buffer
    cmd.Stdout = &stdout
    cmd.Stderr = &stderr

    // 5. Запуск
    err := cmd.Run()

    exitCode := 0
    if err != nil {
        exitCode = 1
        var ee *exec.ExitError
        if errors.As(err, &ee) {
            exitCode = ee.ExitCode()
        }
        if errors.Is(ctx.Err(), context.DeadlineExceeded) {
            exitCode = 124
            err = errors.New("timeout exceeded")
        }
    }

    outBytes := stdout.Bytes()

    // 6. Пост-обработка для php-cgi (отрезаем HTTP-заголовки)
    if cmdArgs[0] == "php-cgi" {
        _, bodyOnly := splitCGI(outBytes)
        outBytes = bodyOnly
    }

    return Result{
        Queue:      job.Queue,
        MsgID:      job.MsgID,
        ExitCode:   exitCode,
        Stdout:     outBytes,
        Stderr:     stderr.Bytes(),
        DurationMs: time.Since(start).Milliseconds(),
        Err:        err,
    }
}
// ========================= FastCGI (php-fpm) =========================

type FastCGIRunner struct {
	Network string // "unix" | "tcp"
	Address string // "/run/php/php8.3-fpm.sock" | "127.0.0.1:9000"
}

// FastCGI protocol constants
const (
	fcgiVersion1 = 1

	fcgiBeginRequest  = 1
	fcgiAbortRequest  = 2
	fcgiEndRequest    = 3
	fcgiParams        = 4
	fcgiStdin         = 5
	fcgiStdout        = 6
	fcgiStderr        = 7
	fcgiData          = 8
	fcgiGetValues     = 9
	fcgiGetValuesRes  = 10
	fcgiUnknownType   = 11

	fcgiResponder = 1
)

type fcgiHeader struct {
	Version       uint8
	Type          uint8
	RequestID     uint16
	ContentLength uint16
	PaddingLength uint8
	Reserved      uint8
}

type fcgiBeginRequestBody struct {
	Role     uint16
	Flags    uint8
	Reserved [5]uint8
}

func (r FastCGIRunner) Run(ctx context.Context, _ []string, script string, job Job) Result {
	start := time.Now()

	// 1) validate inputs
	if strings.TrimSpace(script) == "" {
		return Result{
			Queue:      job.Queue,
			MsgID:      job.MsgID,
			ExitCode:   1,
			Err:        fmt.Errorf("no script path provided for FastCGI"),
			DurationMs: time.Since(start).Milliseconds(),
		}
	}
	if strings.TrimSpace(r.Network) == "" || strings.TrimSpace(r.Address) == "" {
		return Result{
			Queue:      job.Queue,
			MsgID:      job.MsgID,
			ExitCode:   1,
			Err:        fmt.Errorf("FPM network/address not configured"),
			DurationMs: time.Since(start).Milliseconds(),
		}
	}

	// 2) build CGI/FastCGI env list (K=V)
	envList, err := buildPhpEnvStandalone(script, job)
	if err != nil {
		return Result{
			Queue:      job.Queue,
			MsgID:      job.MsgID,
			ExitCode:   1,
			Err:        err,
			DurationMs: time.Since(start).Milliseconds(),
		}
	}

	// 3) convert "K=V" to map
	params := make(map[string]string, len(envList)+8)
	for _, kv := range envList {
		if i := strings.IndexByte(kv, '='); i > 0 {
			params[kv[:i]] = kv[i+1:]
		}
	}

	// 4) some commonly expected params (harmless if extra)
	params["SERVER_NAME"] = "queue-service"
	params["SERVER_PORT"] = "80"
	params["REQUEST_URI"] = "/" + job.Queue + "/job"
	params["DOCUMENT_ROOT"] = filepath.Dir(script)

	// 5) connect to php-fpm
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, r.Network, r.Address)
	if err != nil {
		return Result{
			Queue:      job.Queue,
			MsgID:      job.MsgID,
			ExitCode:   1,
			Err:        fmt.Errorf("dial fpm (%s %s): %w", r.Network, r.Address, err),
			DurationMs: time.Since(start).Milliseconds(),
		}
	}
	defer conn.Close()

	// respect context deadline for read/write
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}

	const reqID uint16 = 1

	// BEGIN_REQUEST
	br := fcgiBeginRequestBody{Role: fcgiResponder, Flags: 0}
	var brBuf [8]byte
	binary.BigEndian.PutUint16(brBuf[0:2], br.Role)
	brBuf[2] = br.Flags
	if err := fcgiWriteRecord(conn, fcgiBeginRequest, reqID, brBuf[:]); err != nil {
		return fcgiErr(job, start, err)
	}

	// PARAMS
	if err := fcgiWriteParams(conn, reqID, params); err != nil {
		return fcgiErr(job, start, err)
	}
	// empty PARAMS to terminate
	if err := fcgiWriteRecord(conn, fcgiParams, reqID, nil); err != nil {
		return fcgiErr(job, start, err)
	}

	// STDIN (request body)
	if len(job.Body) > 0 {
		if err := fcgiWriteStream(conn, fcgiStdin, reqID, job.Body); err != nil {
			return fcgiErr(job, start, err)
		}
	}
	// empty STDIN to terminate
	if err := fcgiWriteRecord(conn, fcgiStdin, reqID, nil); err != nil {
		return fcgiErr(job, start, err)
	}

	// Read response
	var outStdout, outStderr bytes.Buffer
	appExit := 0

	for {
		h, content, err := fcgiReadRecord(conn)
		if err != nil {
			// timeout?
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return Result{
					Queue:      job.Queue,
					MsgID:      job.MsgID,
					ExitCode:   124,
					Stdout:     outStdout.Bytes(),
					Stderr:     outStderr.Bytes(),
					DurationMs: time.Since(start).Milliseconds(),
					Err:        errors.New("timeout exceeded"),
				}
			}
			return fcgiErr(job, start, err)
		}

		// only care about our request id
		if h.RequestID != reqID {
			continue
		}

		switch h.Type {
		case fcgiStdout:
			if len(content) > 0 {
				_, _ = outStdout.Write(content)
			}
		case fcgiStderr:
			if len(content) > 0 {
				_, _ = outStderr.Write(content)
			}
		case fcgiEndRequest:
			// content: appStatus(4) + protocolStatus(1) + reserved(3)
			if len(content) >= 5 {
				appExit = int(binary.BigEndian.Uint32(content[0:4]))
			}
			goto DONE
		default:
			// ignore
		}
	}

DONE:
	outBytes := outStdout.Bytes()

	// php-fpm stdout is headers+body (CGI-style)
	_, bodyOnly := splitCGI(outBytes)

	exitCode := 0
	var runErr error
	if appExit != 0 {
		exitCode = appExit
		runErr = fmt.Errorf("php exited with status %d", appExit)
	}

	return Result{
		Queue:      job.Queue,
		MsgID:      job.MsgID,
		ExitCode:   exitCode,
		Stdout:     bodyOnly,
		Stderr:     outStderr.Bytes(),
		DurationMs: time.Since(start).Milliseconds(),
		Err:        runErr,
	}
}

func fcgiErr(job Job, start time.Time, err error) Result {
	return Result{
		Queue:      job.Queue,
		MsgID:      job.MsgID,
		ExitCode:   1,
		Err:        err,
		DurationMs: time.Since(start).Milliseconds(),
	}
}

// Write a single FastCGI record (with padding to 8-byte boundary)
func fcgiWriteRecord(w io.Writer, typ uint8, reqID uint16, content []byte) error {
	const maxContent = 65535
	if len(content) > maxContent {
		return fmt.Errorf("fcgi content too large: %d", len(content))
	}

	pad := (8 - (len(content) % 8)) % 8
	h := fcgiHeader{
		Version:       fcgiVersion1,
		Type:          typ,
		RequestID:     reqID,
		ContentLength: uint16(len(content)),
		PaddingLength: uint8(pad),
	}

	var hb [8]byte
	hb[0] = h.Version
	hb[1] = h.Type
	binary.BigEndian.PutUint16(hb[2:4], h.RequestID)
	binary.BigEndian.PutUint16(hb[4:6], h.ContentLength)
	hb[6] = h.PaddingLength
	hb[7] = 0

	if _, err := w.Write(hb[:]); err != nil {
		return err
	}
	if len(content) > 0 {
		if _, err := w.Write(content); err != nil {
			return err
		}
	}
	if pad > 0 {
		var zeros [8]byte
		if _, err := w.Write(zeros[:pad]); err != nil {
			return err
		}
	}
	return nil
}

// Split stream into chunks and write as records
func fcgiWriteStream(w io.Writer, typ uint8, reqID uint16, data []byte) error {
	const chunk = 65535
	for len(data) > 0 {
		n := len(data)
		if n > chunk {
			n = chunk
		}
		if err := fcgiWriteRecord(w, typ, reqID, data[:n]); err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

// Encode name/value as per FastCGI spec
func fcgiEncodeNameValue(name, value string) []byte {
	n := []byte(name)
	v := []byte(value)

	var b bytes.Buffer
	writeLen := func(l int) {
		if l < 128 {
			_ = b.WriteByte(byte(l))
		} else {
			// 4 bytes, MSB set
			x := uint32(l) | 0x80000000
			var tmp [4]byte
			binary.BigEndian.PutUint32(tmp[:], x)
			_, _ = b.Write(tmp[:])
		}
	}
	writeLen(len(n))
	writeLen(len(v))
	_, _ = b.Write(n)
	_, _ = b.Write(v)
	return b.Bytes()
}

func fcgiWriteParams(w io.Writer, reqID uint16, params map[string]string) error {
	// Accumulate into reasonably sized chunks
	var chunk bytes.Buffer

	flush := func() error {
		if chunk.Len() == 0 {
			return nil
		}
		if err := fcgiWriteRecord(w, fcgiParams, reqID, chunk.Bytes()); err != nil {
			return err
		}
		chunk.Reset()
		return nil
	}

	// deterministic order not required, but helps debugging
	for k, v := range params {
		enc := fcgiEncodeNameValue(k, v)
		if chunk.Len()+len(enc) > 60000 { // keep below 65535
			if err := flush(); err != nil {
				return err
			}
		}
		_, _ = chunk.Write(enc)
	}

	return flush()
}

func fcgiReadRecord(r io.Reader) (fcgiHeader, []byte, error) {
	var hb [8]byte
	if _, err := io.ReadFull(r, hb[:]); err != nil {
		return fcgiHeader{}, nil, err
	}
	h := fcgiHeader{
		Version:       hb[0],
		Type:          hb[1],
		RequestID:     binary.BigEndian.Uint16(hb[2:4]),
		ContentLength: binary.BigEndian.Uint16(hb[4:6]),
		PaddingLength: hb[6],
		Reserved:      hb[7],
	}

	if h.Version != fcgiVersion1 {
		return h, nil, fmt.Errorf("unexpected fcgi version: %d", h.Version)
	}

	content := make([]byte, int(h.ContentLength))
	if h.ContentLength > 0 {
		if _, err := io.ReadFull(r, content); err != nil {
			return h, nil, err
		}
	}
	if h.PaddingLength > 0 {
		// discard padding
		pad := make([]byte, int(h.PaddingLength))
		if _, err := io.ReadFull(r, pad); err != nil {
			return h, nil, err
		}
	}
	return h, content, nil
}

type runtimeState struct {
	jobs    chan Job
	quit    chan struct{}
	wg      sync.WaitGroup
	runner  Runner
	timeout time.Duration
	log     *zap.Logger

	stopOnce sync.Once
	stopped  atomic.Bool
}

type Runtime struct {
	mu sync.RWMutex
	Cfg    config.QueueConfig
	Schema *validate.CompiledSchema
	Command    []string // ["php-cgi"] or ["php-fpm"]
	ScriptPath string

	FPMNetwork string // "unix" | "tcp"
	FPMAddress string // "/run/php/php8.3-fpm.sock" | "127.0.0.1:9000"
	Store *storage.Store

	st *runtimeState
}

func (rt *Runtime) initIfNeeded(baseLog *zap.Logger) error {
	if rt.st != nil {
		return nil
	}

	workers := rt.Cfg.Workers
	if workers <= 0 {
		workers = 1
	}

	timeout := 10 * time.Second
	if rt.Cfg.TimeoutSec > 0 {
		timeout = time.Duration(rt.Cfg.TimeoutSec) * time.Second
	}

	maxQueue := 100
	if rt.Cfg.MaxQueue > 0 {
		maxQueue = rt.Cfg.MaxQueue
	}

	l := baseLog
	if l == nil {
		l = zap.NewNop()
	}
	l = l.Named("queue_runtime").With(zap.String("queue", rt.Cfg.Name))

	var runner Runner = ExecRunner{}
	if len(rt.Command) > 0 && rt.Command[0] == "php-fpm" {
		// Default to unix socket if not specified
		netw := strings.TrimSpace(rt.FPMNetwork)
		addr := strings.TrimSpace(rt.FPMAddress)
		if netw == "" {
			netw = "unix"
		}
		if addr == "" {
			// common for Ubuntu 24.04 with PHP 8.3
			addr = "/run/php/php8.3-fpm.sock"
		}
		runner = FastCGIRunner{Network: netw, Address: addr}
	}

	rt.st = &runtimeState{
		jobs:    make(chan Job, maxQueue),
		quit:    make(chan struct{}),
		runner:  runner,
		timeout: timeout,
		log:     l,
	}

	for wid := 1; wid <= workers; wid++ {
		rt.st.wg.Add(1)
		go rt.workerLoop(wid)//запуск как фоновая задача
	}

	rt.st.wg.Add(1)
	go rt.maintenanceLoop()


	rt.st.log.Info("queue_started",
    zap.Int("workers", workers),
    zap.Int("max_queue", maxQueue),
    zap.Int64("timeout_ms", timeout.Milliseconds()),
    zap.String("runner", fmt.Sprintf("%T", runner)),
    zap.Int("max_retries", rt.Cfg.MaxRetries),
    zap.Int("retry_delay_ms", rt.Cfg.RetryDelayMs),
)
	return nil
}
func (rt *Runtime) Enqueue(ctx context.Context, job Job) (err error) {
	if rt.st == nil {
		return fmt.Errorf("runtime not initialized for queue %q", rt.Cfg.Name)
	}

	// 1) Быстрая проверка
	if rt.st.stopped.Load() {
		return fmt.Errorf("queue %q is stopped", rt.Cfg.Name)
	}
	if e := ctx.Err(); e != nil {
		return e
	}

	// Защита от panic при send в закрытый канал
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("queue %q is closing/stopped", rt.Cfg.Name)
		}
	}()

	// 2) Быстрая попытка без ожидания
	select {
	case rt.st.jobs <- job:
		return nil
	default:
		// очередь заполнена — пойдём в ожидание ниже
		rt.st.log.Warn("queue_full_waiting_enqueue",
			zap.String("queue", rt.Cfg.Name),
			zap.String("msg_id", job.MsgID),
			zap.Int("attempt", job.Attempt),
		)
	}

	// 3) Ограниченное ожидание (100ms) или отмена контекста
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case rt.st.jobs <- job:
		return nil

	case <-ctx.Done():
		return ctx.Err()

	case <-rt.st.quit:
		return fmt.Errorf("queue %q is stopped", rt.Cfg.Name)

	case <-timer.C:
		return fmt.Errorf("queue %q is full and busy", rt.Cfg.Name)
	}
}

func (rt *Runtime) Stop() {
	if rt.st == nil {
		return
	}

	rt.st.stopOnce.Do(func() {
		rt.st.stopped.Store(true)
		close(rt.st.quit)      // сигнал всем выйти
		// НЕ закрываем jobs здесь!
	})

	rt.st.wg.Wait()

	// теперь точно нет воркеров/планировщиков, которые могли бы писать в jobs
	// закрывать jobs можно, но не обязательно
	// close(rt.st.jobs)

	rt.st.log.Info("queue_stopped")
}


func (rt *Runtime) workerLoop(workerID int) {
	defer rt.st.wg.Done()

	// Объявляем scheduleRetry как анонимную функцию (замыкание) внутри воркера
	scheduleRetry := func(job Job) {
		delay := time.Duration(rt.Cfg.RetryDelayMs) * time.Millisecond
		if delay <= 0 {
			select {
			case <-rt.st.quit:
				return
			case rt.st.jobs <- job:
				return
			}
		}

		rt.st.wg.Add(1)
		go func() {
			defer rt.st.wg.Done()
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
			case rt.st.jobs <- job:
				rt.st.log.Info("job_requeued",
					zap.String("msg_id", job.MsgID),
					zap.Int("attempt", job.Attempt),
					zap.Int64("retry_delay_ms", int64(delay/time.Millisecond)),
				)
			}
		}()
	}

	for {
		select {
		case <-rt.st.quit:
			return

		case job, ok := <-rt.st.jobs:
			if !ok {
				return
			}

			// 1) MarkProcessing
			if rt.Store != nil {
				if err := rt.Store.MarkProcessing(job.MsgID, job.Attempt); err != nil {
					rt.st.log.Warn("mark_processing_failed",
						zap.String("msg_id", job.MsgID),
						zap.Int("worker_id", workerID),
						zap.Error(err),
					)
					continue
				}

				rt.st.log.Info("mark_processing_ok",
					zap.String("msg_id", job.MsgID),
					zap.Int("worker_id", workerID),
					zap.Int("attempt", job.Attempt),
				)
			}

			// 2) Снапшот конфига
			rt.mu.RLock()
			currentCmd := append([]string(nil), rt.Command...)
			scriptPath := rt.ScriptPath
			rt.mu.RUnlock()

			// 3) Выполнение под таймаутом
			ctx, cancel := context.WithTimeout(context.Background(), rt.st.timeout)
			res := rt.st.runner.Run(ctx, currentCmd, scriptPath, job)
			cancel()

			rt.appendStdStreams(job, res.Stdout, res.Stderr)

			failed := (res.ExitCode != 0 || res.Err != nil)

			// 4) Проверка на Retry
			if failed && job.Attempt < rt.Cfg.MaxRetries {
				if rt.Store != nil {
					if err := rt.Store.RequeueForRetry(job.MsgID, time.Now().UnixMilli()); err == nil {
						next := job
						next.Attempt = job.Attempt + 1
						scheduleRetry(next)

						rt.st.log.Warn("job_retry_scheduled",
							zap.String("msg_id", job.MsgID),
							zap.Int("worker_id", workerID),
							zap.Int("from_attempt", job.Attempt),
							zap.Int("to_attempt", next.Attempt),
							zap.Int("exit_code", res.ExitCode),
							zap.String("err", errString(res.Err)),
						)
						continue // Переходим к следующей задаче, не вызывая MarkDone
					}
					// Если Requeue в базу не удался, падаем ниже в MarkDone(Failed)
				}
			}

			// 5) Финализация (если не ретрай или ретраи кончились)
			status := storage.StatusSucceeded
			if failed {
				status = storage.StatusFailed
			}

			if rt.Store != nil {
				ttlMs := time.Now().Add(10 * time.Minute).UnixMilli()

				err := rt.Store.MarkDone(
					job.MsgID,
					status,
					storage.Result{
						FinishedAt: time.Now().UnixMilli(),
						ExitCode:   res.ExitCode,
						DurationMs: res.DurationMs,
						Err:        errString(res.Err),
					},
					ttlMs,
				)

				if err != nil {
					rt.st.log.Warn("mark_done_failed",
						zap.String("msg_id", job.MsgID),
						zap.Int("attempt", job.Attempt),
						zap.String("final_status", string(status)),
						zap.Error(err),
					)
				} else {
					rt.st.log.Info("mark_done_ok",
						zap.String("msg_id", job.MsgID),
						zap.Int("attempt", job.Attempt),
						zap.String("final_status", string(status)),
						zap.Int("exit_code", res.ExitCode),
						zap.Int64("duration_ms", res.DurationMs),
						zap.Int64("ttl_ms", ttlMs),
					)
				}
			}

			// 6) Лог
			fields := []zap.Field{
				zap.String("msg_id", job.MsgID),
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
		}
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func (rt *Runtime) appendStdStreams(job Job, out, errb []byte) {
	if len(out) == 0 && len(errb) == 0 {
		return
	}

	dir := filepath.Join("configs/scripts/logs", job.Queue)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		rt.st.log.Warn("mkdir_job_log_dir_failed", zap.Error(err))
		return
	}

	logPath := filepath.Join(dir, job.MsgID+".log")
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

func (rt *Runtime) maintenanceLoop() {
	defer rt.st.wg.Done()

	requeueTicker := time.NewTicker(15 * time.Second) // RequeueStuck
	gcTicker := time.NewTicker(1 * time.Minute)       // GC
	defer requeueTicker.Stop()
	defer gcTicker.Stop()

	for {
		select {
		case <-rt.st.quit:
			return

		case <-requeueTicker.C:
			if rt.Store != nil {
				n, err := rt.Store.RequeueStuck(0, 200)
				if err != nil {
					rt.st.log.Warn("requeue_stuck_failed", zap.Error(err))
				} else if n > 0 {
					rt.st.log.Info("requeue_stuck_done", zap.Int("count", n))
				}
			}

		case <-gcTicker.C:
			if rt.Store != nil {
				n, err := rt.Store.GC(0, 500)
				if err != nil {
					rt.st.log.Warn("gc_failed", zap.Error(err))
				} else if n > 0 {
					rt.st.log.Info("gc_done", zap.Int("deleted", n))
				}
			}
		}
	}
}

// ========================= helpers =========================
func buildPhpEnvStandalone(scriptPath string, job Job) ([]string, error) {
    if strings.TrimSpace(scriptPath) == "" {
        return nil, fmt.Errorf("no script path provided")
    }

    method := job.Method
    if method == "" { method = "POST" }

    ct := job.ContentType
    if ct == "" { ct = "application/json" }

    env := []string{
        "GATEWAY_INTERFACE=CGI/1.1",
        "SERVER_PROTOCOL=HTTP/1.1",
        "REQUEST_METHOD=" + method,
        "SCRIPT_FILENAME=" + scriptPath,
        "SCRIPT_NAME=" + filepath.Base(scriptPath),
        "QUERY_STRING=" + job.QueryString,
        "CONTENT_TYPE=" + ct,
        "CONTENT_LENGTH=" + strconv.Itoa(len(job.Body)),
        "REMOTE_ADDR=" + stripPort(job.RemoteAddr),
        "REDIRECT_STATUS=200",
        "MSG_ID=" + job.MsgID,
        "QUEUE=" + job.Queue,
        "ATTEMPT=" + strconv.Itoa(job.Attempt),
    }
    
    if job.IdempotencyKey != "" {
        env = append(env, "HTTP_IDEMPOTENCY_KEY="+job.IdempotencyKey)
    }

    return env, nil
}

func stripPort(addr string) string {
	if addr == "" {
		return ""
	}
	host, _, err := net.SplitHostPort(addr)
	if err == nil {
		return host
	}
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
