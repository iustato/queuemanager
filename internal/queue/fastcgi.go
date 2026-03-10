package queue

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	fcgiclient "github.com/tomasen/fcgi_client"
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

	client *fcgiclient.FCGIClient
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

func (r *PooledFastCGIRunner) ensureClient(ctx context.Context) (*fcgiclient.FCGIClient, error) {
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

	c, err := fcgiclient.DialTimeout(r.Network, r.Address, dialTimeout)
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

	method := strings.ToUpper(strings.TrimSpace(job.Method))
	if method == "" {
		method = "POST"
	}

	client, err := r.ensureClient(ctx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return r.makeResult(job, start, 124, 0, errors.New("timeout exceeded"))
		}
		return r.makeResult(job, start, 1, 0, fmt.Errorf("dial fpm (%s %s): %w", r.Network, r.Address, err))
	}

	type rr struct {
		resp     *http.Response
		err      error
		panicked bool
	}
	ch := make(chan rr, 1)

	go func(c *fcgiclient.FCGIClient) {
		var resp *http.Response
		defer func() {
			if rec := recover(); rec != nil {
				if resp != nil && resp.Body != nil {
					_ = resp.Body.Close()
				}
				select {
				case ch <- rr{
					resp:     nil,
					err:      fmt.Errorf("panic in fcgi request: %v", rec),
					panicked: true,
				}:
				default:
				}
			}
		}()

		var e error
		switch method {
		case "GET":
			resp, e = c.Get(params)
		default:
			ct := strings.TrimSpace(job.ContentType)
			if ct == "" {
				ct = "application/json"
			}
			resp, e = c.Post(params, ct, bytes.NewReader(job.Body), len(job.Body))
		}

		select {
		case ch <- rr{resp: resp, err: e, panicked: false}:
		default:
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
		}
	}(client)

	select {
	case <-ctx.Done():
		r.resetClient()
		select {
		case out := <-ch:
			if out.resp != nil && out.resp.Body != nil {
				_ = out.resp.Body.Close()
			}
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

		if out.resp == nil {
			r.resetClient()
			return r.makeResult(job, start, 1, 0, errors.New("nil fcgi response"))
		}

		defer out.resp.Body.Close()
		httpStatus := out.resp.StatusCode

		limited := io.LimitReader(out.resp.Body, maxResp+1)
		body, readErr := io.ReadAll(limited)
		if readErr != nil {
			r.resetClient()
			return r.makeResult(job, start, 1, httpStatus, fmt.Errorf("read fcgi response body: %w", readErr))
		}

		if int64(len(body)) > maxResp {
			if r.TruncateOnLimit {
				body = body[:maxResp]
				return r.makeResultOut(job, start, 1, httpStatus, body, nil,
					fmt.Errorf("fcgi response too large (>%d bytes)", maxResp),
				)
			}
			return r.makeResultOut(job, start, 1, httpStatus, nil, nil,
				fmt.Errorf("fcgi response too large (>%d bytes)", maxResp),
			)
		}

		if httpStatus >= 400 {
			return r.makeResultOut(job, start, 1, httpStatus, body, nil,
				fmt.Errorf("php-fpm returned HTTP %s", out.resp.Status),
			)
		}

		return r.makeResultOut(job, start, 0, httpStatus, body, nil, nil)
	}
}