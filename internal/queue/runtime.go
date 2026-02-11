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
	"time"

	"go-web-server/internal/config"
	"go-web-server/internal/validate"
	"go-web-server/internal/storage"

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
	Stdout     []byte
	Stderr     []byte
	DurationMs int64
	Err        error
}

type Runner interface {
	Run(ctx context.Context, rt *Runtime, job Job) Result
}

// ========================= Exec (php-cgi, anything executable) =========================

type ExecRunner struct{}

func (ExecRunner) Run(ctx context.Context, rt *Runtime, job Job) Result {
	start := time.Now()

	if len(rt.Command) == 0 {
		return Result{
			Queue:      job.Queue,
			MsgID:      job.MsgID,
			ExitCode:   1,
			Err:        fmt.Errorf("no command configured for queue %q", rt.Cfg.Name),
			DurationMs: time.Since(start).Milliseconds(),
		}
	}

	// IMPORTANT: php-fpm is not an executable job. It must use FastCGI runner.
	if rt.Command[0] == "php-fpm" {
		return Result{
			Queue:      job.Queue,
			MsgID:      job.MsgID,
			ExitCode:   1,
			Err:        fmt.Errorf("php-fpm must be executed via FastCGIRunner (FastCGI protocol), not ExecRunner"),
			DurationMs: time.Since(start).Milliseconds(),
		}
	}

	cmd := exec.CommandContext(ctx, rt.Command[0], rt.Command[1:]...)
	cmd.Stdin = bytes.NewReader(job.Body)

	// CGI mode logic for php-cgi
	if rt.Command[0] == "php-cgi" {
		env, err := rt.buildPhpEnv(job)
		if err != nil {
			return Result{
				Queue:      job.Queue,
				MsgID:      job.MsgID,
				ExitCode:   1,
				Err:        err,
				DurationMs: time.Since(start).Milliseconds(),
			}
		}
		cmd.Env = append(os.Environ(), env...)
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

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

	// php-cgi stdout contains headers+body
	if rt.Command[0] == "php-cgi" {
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

func (r FastCGIRunner) Run(ctx context.Context, rt *Runtime, job Job) Result {
	start := time.Now()

	// basic validation
	if strings.TrimSpace(rt.ScriptPath) == "" {
		return Result{
			Queue:      job.Queue,
			MsgID:      job.MsgID,
			ExitCode:   1,
			Err:        fmt.Errorf("no script configured for queue %q", rt.Cfg.Name),
			DurationMs: time.Since(start).Milliseconds(),
		}
	}
	if strings.TrimSpace(r.Network) == "" || strings.TrimSpace(r.Address) == "" {
		return Result{
			Queue:      job.Queue,
			MsgID:      job.MsgID,
			ExitCode:   1,
			Err:        fmt.Errorf("php-fpm selected but FPMNetwork/FPMAddress not set for queue %q", rt.Cfg.Name),
			DurationMs: time.Since(start).Milliseconds(),
		}
	}

	// Build CGI/FastCGI env
	envList, err := rt.buildPhpEnv(job)
	if err != nil {
		return Result{
			Queue:      job.Queue,
			MsgID:      job.MsgID,
			ExitCode:   1,
			Err:        err,
			DurationMs: time.Since(start).Milliseconds(),
		}
	}

	// Convert "K=V" to map
	params := make(map[string]string, len(envList)+8)
	for _, kv := range envList {
		if i := strings.IndexByte(kv, '='); i > 0 {
			params[kv[:i]] = kv[i+1:]
		}
	}

	// Some commonly expected params (harmless if extra)
	params["SERVER_NAME"] = "queue-service"
	params["SERVER_PORT"] = "80"
	params["REQUEST_URI"] = "/" + job.Queue + "/job" // not used by PHP typically, but fine
	params["DOCUMENT_ROOT"] = filepath.Dir(rt.ScriptPath)

	// Connect
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

	// Respect context deadline for read/write
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}

	const reqID uint16 = 1

	// BEGIN_REQUEST
	br := fcgiBeginRequestBody{Role: fcgiResponder, Flags: 0}
	var brBuf [8]byte
	binary.BigEndian.PutUint16(brBuf[0:2], br.Role)
	brBuf[2] = br.Flags
	// reserved already zero
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

		// Only care about our request id
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
				// protocolStatus := content[4]
				_ = appExit
			}
			goto DONE
		default:
			// ignore
		}
	}

DONE:
	outBytes := outStdout.Bytes()

	// php-fpm stdout is also headers+body (CGI-style)
	_, bodyOnly := splitCGI(outBytes)
	outBytes = bodyOnly

	exitCode := 0
	var runErr error
	if appExit != 0 {
		// treat non-zero as failure; keep stderr for details
		exitCode = appExit
		runErr = fmt.Errorf("php exited with status %d", appExit)
	}
	// If FPM wrote to stderr, keep it; not necessarily fatal.
	if runErr == nil && outStderr.Len() > 0 {
		// keep non-fatal; leave Err=nil, but you may choose to set warning elsewhere
	}

	return Result{
		Queue:      job.Queue,
		MsgID:      job.MsgID,
		ExitCode:   exitCode,
		Stdout:     outBytes,
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

// ========================= Runtime =========================

func (rt *Runtime) buildPhpEnv(job Job) ([]string, error) {
	if strings.TrimSpace(rt.ScriptPath) == "" {
		return nil, fmt.Errorf("no script configured for queue %q", rt.Cfg.Name)
	}

	method := strings.TrimSpace(job.Method)
	if method == "" {
		method = "POST"
	}

	ct := strings.TrimSpace(job.ContentType)
	if ct == "" {
		ct = "application/json"
	}

	remote := stripPort(job.RemoteAddr)

	env := []string{
		"GATEWAY_INTERFACE=CGI/1.1",
		"SERVER_PROTOCOL=HTTP/1.1",
		"REQUEST_METHOD=" + method,
		"SCRIPT_FILENAME=" + rt.ScriptPath,
		"SCRIPT_NAME=" + filepath.Base(rt.ScriptPath),
		"QUERY_STRING=" + job.QueryString,
		"CONTENT_TYPE=" + ct,
		"CONTENT_LENGTH=" + strconv.Itoa(len(job.Body)),
		"REMOTE_ADDR=" + remote,
		"REDIRECT_STATUS=200",
		"MSG_ID=" + job.MsgID,
		"QUEUE=" + job.Queue,
		"ATTEMPT=" + strconv.Itoa(job.Attempt),
	}

	if k := strings.TrimSpace(job.IdempotencyKey); k != "" {
		env = append(env, "HTTP_IDEMPOTENCY_KEY="+k)
	}

	return env, nil
}

type runtimeState struct {
	jobs    chan Job
	quit    chan struct{}
	wg      sync.WaitGroup
	runner  Runner
	timeout time.Duration
	log     *zap.Logger
}

type Runtime struct {
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
		go rt.workerLoop(wid)
	}

	rt.st.log.Info("queue_started",
		zap.Int("workers", workers),
		zap.Int("max_queue", maxQueue),
		zap.Int64("timeout_ms", timeout.Milliseconds()),
		zap.String("runner", fmt.Sprintf("%T", runner)),
	)

	return nil
}

func (rt *Runtime) Enqueue(job Job) error {
	if rt.st == nil {
		return fmt.Errorf("runtime not initialized for queue %q", rt.Cfg.Name)
	}
	select {
	case rt.st.jobs <- job:
		return nil
	default:
		return fmt.Errorf("queue %q is full", rt.Cfg.Name)
	}
}

func (rt *Runtime) Stop() {
	if rt.st == nil {
		return
	}
	close(rt.st.quit)
	rt.st.wg.Wait()
	rt.st.log.Info("queue_stopped")
}

func (rt *Runtime) workerLoop(workerID int) {
	defer rt.st.wg.Done()

	for {
		select {
		case <-rt.st.quit:
			return
		case job, ok := <-rt.st.jobs:
			if !ok {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), rt.st.timeout)
			res := rt.st.runner.Run(ctx, rt, job)
			cancel()

			fields := []zap.Field{
				zap.String("msg_id", job.MsgID),
				zap.Int("worker_id", workerID),
				zap.Int("exit_code", res.ExitCode),
				zap.Int("attempt", job.Attempt),
				zap.Int64("duration_ms", res.DurationMs),
				zap.String("cmd", safeCmd(rt.Command)),
			}

			if res.Err != nil {
				fields = append(fields, zap.String("err", res.Err.Error()))
				rt.st.log.Warn("job_done", fields...)
			} else {
				rt.st.log.Info("job_done", fields...)
			}
		}
	}
}

// ========================= helpers =========================

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
