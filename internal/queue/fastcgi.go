package queue

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/yookoala/gofast"
)

var ErrRunnerBusy = errors.New("fastcgi runner is busy (concurrent Run calls are not allowed)")

type PooledFastCGIRunner struct {
	Network string
	Address string

	ServerName  string
	ServerPort  string
	DialTimeout time.Duration

	DocumentRoot     string
	MaxResponseBytes int64
	TruncateOnLimit  bool

	client gofast.Client
	busy   atomic.Bool
}

func (r *PooledFastCGIRunner) makeResult(job Job, start time.Time, exitCode int, httpStatus int, err error) Result {
	return Result{
		Queue:      job.Queue,
		MsgID:      job.MsgID,
		ExitCode:   exitCode,
		HTTPStatus: httpStatus,
		Err:        err,
		DurationMs: time.Since(start).Milliseconds(),
	}
}

func (r *PooledFastCGIRunner) makeResultOut(job Job, start time.Time, exitCode int, httpStatus int, out, errb []byte, err error) Result {
	res := r.makeResult(job, start, exitCode, httpStatus, err)
	res.Stdout = out
	res.Stderr = errb
	return res
}

func (r *PooledFastCGIRunner) resetClient() {
	if r.client != nil {
		r.client.Close()
		r.client = nil
	}
}

func (r *PooledFastCGIRunner) ensureClient(ctx context.Context) (gofast.Client, error) {
	if r.client != nil {
		return r.client, nil
	}

	dialTimeout := r.DialTimeout
	if dialTimeout <= 0 {
		dialTimeout = 3 * time.Second
	}
	if dl, ok := ctx.Deadline(); ok {
		remain := time.Until(dl)
		if remain <= 0 {
			return nil, context.DeadlineExceeded
		}
		if remain < dialTimeout {
			dialTimeout = remain
		}
	}

	connFactory := func() (net.Conn, error) {
		return net.DialTimeout(r.Network, r.Address, dialTimeout)
	}
	factory := gofast.SimpleClientFactory(connFactory)
	c, err := factory()
	if err != nil {
		return nil, err
	}
	r.client = c
	return r.client, nil
}

func (r *PooledFastCGIRunner) Run(ctx context.Context, _ []string, script string, job Job) Result {
	start := time.Now()

	if !r.busy.CompareAndSwap(false, true) {
		return r.makeResult(job, start, 1, 0, ErrRunnerBusy)
	}
	defer r.busy.Store(false)

	if strings.TrimSpace(script) == "" {
		return r.makeResult(job, start, 1, 0, fmt.Errorf("no script path provided for FastCGI"))
	}
	if strings.TrimSpace(r.Network) == "" || strings.TrimSpace(r.Address) == "" {
		return r.makeResult(job, start, 1, 0, fmt.Errorf("FPM network/address not configured"))
	}
	if err := ctx.Err(); err != nil {
		return r.makeResult(job, start, 1, 0, err)
	}

	absScript, err := filepath.Abs(script)
	if err != nil {
		return r.makeResult(job, start, 1, 0, fmt.Errorf("abs script path: %w", err))
	}

	params := buildPhpEnvMap(absScript, job)

	serverName := strings.TrimSpace(r.ServerName)
	if serverName == "" {
		serverName = "queue-service"
	}
	serverPort := strings.TrimSpace(r.ServerPort)
	if serverPort == "" {
		serverPort = "80"
	}
	params["SERVER_NAME"] = serverName
	params["SERVER_PORT"] = serverPort
	params["REQUEST_URI"] = "/" + job.Queue + "/job"

	docRoot := strings.TrimSpace(r.DocumentRoot)
	if docRoot == "" {
		docRoot = filepath.Dir(absScript)
	}
	params["DOCUMENT_ROOT"] = docRoot

	maxResp := r.MaxResponseBytes
	if maxResp <= 0 {
		maxResp = 4 << 20 // 4 MiB
	}

	client, err := r.ensureClient(ctx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return r.makeResult(job, start, 124, 0, errors.New("timeout exceeded"))
		}
		return r.makeResult(job, start, 1, 0, fmt.Errorf("dial fpm (%s %s): %w", r.Network, r.Address, err))
	}

	// Build gofast Request with FCGI_KEEP_CONN for connection reuse.
	req := &gofast.Request{
		Role:     gofast.RoleResponder,
		Params:   params,
		Stdin:    io.NopCloser(bytes.NewReader(job.Body)),
		KeepConn: true,
	}

	type rr struct {
		stdout   []byte
		stderr   []byte
		status   int
		err      error
		panicked bool
	}
	ch := make(chan rr, 1)

	go func(c gofast.Client) {
		defer func() {
			if rec := recover(); rec != nil {
				select {
				case ch <- rr{
					err:      fmt.Errorf("panic in fcgi request: %v", rec),
					panicked: true,
				}:
				default:
				}
			}
		}()

		resp, e := c.Do(req)
		if e != nil {
			select {
			case ch <- rr{err: e}:
			default:
			}
			return
		}
		defer resp.Close()

		rec := httptest.NewRecorder()
		var stderrBuf bytes.Buffer
		if e := resp.WriteTo(rec, &stderrBuf); e != nil {
			select {
			case ch <- rr{err: e}:
			default:
			}
			return
		}

		select {
		case ch <- rr{
			stdout: rec.Body.Bytes(),
			stderr: stderrBuf.Bytes(),
			status: rec.Code,
		}:
		default:
		}
	}(client)

	select {
	case <-ctx.Done():
		r.resetClient()
		select {
		case <-ch:
		default:
		}

		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return r.makeResult(job, start, 124, 0, errors.New("timeout exceeded"))
		}
		return r.makeResult(job, start, 1, 0, ctx.Err())

	case out := <-ch:
		if out.panicked {
			r.resetClient()
			return r.makeResult(job, start, 1, 0, out.err)
		}

		if out.err != nil {
			r.resetClient()
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return r.makeResult(job, start, 124, 0, errors.New("timeout exceeded"))
			}
			return r.makeResult(job, start, 1, 0, out.err)
		}

		httpStatus := out.status
		body := out.stdout

		if int64(len(body)) > maxResp {
			if r.TruncateOnLimit {
				body = body[:maxResp]
				return r.makeResultOut(job, start, 1, httpStatus, body, out.stderr,
					fmt.Errorf("fcgi response too large (>%d bytes)", maxResp),
				)
			}
			return r.makeResultOut(job, start, 1, httpStatus, nil, out.stderr,
				fmt.Errorf("fcgi response too large (>%d bytes)", maxResp),
			)
		}

		if httpStatus >= 400 {
			return r.makeResultOut(job, start, 1, httpStatus, body, out.stderr,
				fmt.Errorf("php-fpm returned HTTP %d", httpStatus),
			)
		}

		return r.makeResultOut(job, start, 0, httpStatus, body, out.stderr, nil)
	}
}
