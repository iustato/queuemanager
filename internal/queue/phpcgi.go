package queue

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// PHPCGIRunner запускает php-cgi как CGI (с правильным окружением) и отрезает CGI-заголовки.
type PHPCGIRunner struct {
	// Path to php-cgi binary (default: "php-cgi").
	PhpCgiBin string

	// Ограничения вывода (anti-OOM). 0 => drop all output.
	MaxStdoutBytes int
	MaxStderrBytes int

	// В prod на unix включать true: убиваем дерево процессов при отмене контекста.
	KillProcessGroup bool
}

func (r PHPCGIRunner) Run(ctx context.Context, _ []string, script string, job Job) Result {
	start := time.Now()

	makeResult := func(exitCode int, err error, out, serr []byte) Result {
		return Result{
			Queue:       job.Queue,
			MessageGUID: job.MessageGUID,
			ExitCode:   exitCode,
			Stdout:     out,
			Stderr:     serr,
			DurationMs: time.Since(start).Milliseconds(),
			Err:        err,
		}
	}

	// validate
	if strings.TrimSpace(script) == "" {
		return makeResult(1, fmt.Errorf("no script path provided for php-cgi"), nil, nil)
	}
	if err := ctx.Err(); err != nil {
		return makeResult(1, err, nil, nil)
	}

	absScript, err := filepath.Abs(script)
	if err != nil {
		return makeResult(1, fmt.Errorf("abs script path: %w", err), nil, nil)
	}

	// Build CGI env (MUST include REDIRECT_STATUS and SCRIPT_FILENAME absolute path)
	env, err := buildPhpEnvStandalone(absScript, job)
	if err != nil {
		return makeResult(1, err, nil, nil)
	}

	// Prepare process
	bin := r.PhpCgiBin
	if bin == "" {
		bin = "php-cgi"
	}
	cmd := exec.CommandContext(ctx, bin)
	cmd.Stdin = bytes.NewReader(job.Body)

	// Kill process tree on unix (stubs on !unix)
	if r.KillProcessGroup {
		makeProcessGroup(cmd)
	}

	cmd.Env = append(os.Environ(), env...)

	// Bounded output to prevent OOM
	var stdout, stderr limitBuffer
	stdout.limit = r.MaxStdoutBytes
	stderr.limit = r.MaxStderrBytes
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	runErr := cmd.Run()

	// If context canceled/timeout: Go kills only parent; kill process group (unix) to avoid orphans
	if ctx.Err() != nil && r.KillProcessGroup {
		killProcessGroup(cmd)
	}

	// Timeout/cancel: preserve ctx.Err() (so errors.Is works)
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return makeResult(124, ctx.Err(), stdout.Bytes(), stderr.Bytes())
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return makeResult(1, ctx.Err(), stdout.Bytes(), stderr.Bytes())
	}

	// Compute exit code
	exitCode := 0
	if runErr != nil {
		exitCode = 1
		var ee *exec.ExitError
		if errors.As(runErr, &ee) {
			exitCode = ee.ExitCode()
		}
	}

	// php-cgi prints CGI headers + body; handle both \r\n\r\n and \n\n; if no headers, body starts at 0.
	raw := stdout.Bytes()
	bodyStart := splitCGIBodyIndex(raw)
	bodyOnly := raw[bodyStart:]

	return makeResult(exitCode, runErr, bodyOnly, stderr.Bytes())
}