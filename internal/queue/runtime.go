package queue

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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

	"go.uber.org/zap"
)

type Job struct {
	Queue      string
	MsgID      string
	Body       []byte
	EnqueuedAt time.Time
	Attempt    int

	// CGI context
	Method      string
	QueryString string
	ContentType string
	RemoteAddr  string
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

// ExecRunner запускает команду rt.Command (из YAML runtime) и скрипт rt.ScriptPath (из YAML script)
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

	cmd := exec.CommandContext(ctx, rt.Command[0], rt.Command[1:]...)
	cmd.Stdin = bytes.NewReader(job.Body)

	// CGI mode for php-cgi
	// CGI mode for php-cgi
if rt.Command[0] == "php-cgi" {
    if strings.TrimSpace(rt.ScriptPath) == "" {
        return Result{
            Queue:      job.Queue,
            MsgID:      job.MsgID,
            ExitCode:   1,
            Err:        fmt.Errorf("no script configured for queue %q", rt.Cfg.Name),
            DurationMs: time.Since(start).Milliseconds(),
        }
    }

    method := job.Method
    if strings.TrimSpace(method) == "" {
        method = "POST"
    }

    ct := job.ContentType
    if strings.TrimSpace(ct) == "" {
        ct = "application/json"
    }

    remote := stripPort(job.RemoteAddr)

    env := append(os.Environ(),
        "GATEWAY_INTERFACE=CGI/1.1",
        "SERVER_PROTOCOL=HTTP/1.1",
        "REQUEST_METHOD="+method,
        "SCRIPT_FILENAME="+rt.ScriptPath,
        "SCRIPT_NAME="+filepath.Base(rt.ScriptPath),
        "QUERY_STRING="+job.QueryString,
        "CONTENT_TYPE="+ct,
        "CONTENT_LENGTH="+strconv.Itoa(len(job.Body)),
        "REMOTE_ADDR="+remote,
        "REDIRECT_STATUS=200",
        "MSG_ID="+job.MsgID,
        "QUEUE="+job.Queue,
        "ATTEMPT="+strconv.Itoa(job.Attempt),
    )

    if k := strings.TrimSpace(job.IdempotencyKey); k != "" {
        env = append(env, "HTTP_IDEMPOTENCY_KEY="+k)
    }

    cmd.Env = env
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

	// If php-cgi: stdout contains "Header: v\r\n...\r\n\r\n<body>"
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

type runtimeState struct {
	jobs chan Job
	quit chan struct{}
	wg   sync.WaitGroup

	runner  Runner
	timeout time.Duration
	log     *zap.Logger
}

type Runtime struct {
	Cfg    config.QueueConfig
	Schema *validate.CompiledSchema

	Command    []string // например: ["php-cgi"]
	ScriptPath string   // абсолютный путь к scripts/queue1.php

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

	st := &runtimeState{
		jobs:    make(chan Job, maxQueue),
		quit:    make(chan struct{}),
		runner:  ExecRunner{},
		timeout: timeout,
		log:     l,
	}
	rt.st = st

	for wid := 1; wid <= workers; wid++ {
		st.wg.Add(1)
		go rt.workerLoop(wid)
	}

	st.log.Info("queue_started",
		zap.Int("workers", workers),
		zap.Int("max_queue", maxQueue),
		zap.Int64("timeout_ms", timeout.Milliseconds()),
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

			// TODO: storage (bbolt)
		}
	}
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
