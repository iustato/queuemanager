package queue

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"time"
)

type ExecRunner struct {
	MaxStdoutBytes   int
	MaxStderrBytes   int
	KillProcessGroup bool // ВКЛЮЧАТЬ в prod на unix
}

func (r ExecRunner) Run(ctx context.Context, cmdArgs []string, _ string, job Job) Result {
	start := time.Now()

	makeResult := func(exitCode int, err error, out, serr []byte) Result {
		return Result{
			Queue:      job.Queue,
			MsgID:      job.MsgID,
			ExitCode:   exitCode,
			Stdout:     out,
			Stderr:     serr,
			DurationMs: time.Since(start).Milliseconds(),
			Err:        err,
		}
	}

	if len(cmdArgs) == 0 {
		return makeResult(1, fmt.Errorf("no command provided"), nil, nil)
	}

	cmd := exec.CommandContext(ctx, cmdArgs[0], cmdArgs[1:]...)
	cmd.Stdin = bytes.NewReader(job.Body)

	// Убиваем дерево процессов на unix (на windows/mac через stub будет no-op)
	if r.KillProcessGroup {
		makeProcessGroup(cmd)
	}

	var stdout, stderr limitBuffer
	stdout.limit = r.MaxStdoutBytes
	stderr.limit = r.MaxStderrBytes

	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()

	// Если контекст отменился — Go мог убить только родителя.
	// Добиваем группу (на unix); на !unix это no-op.
	if ctx.Err() != nil && r.KillProcessGroup {
		killProcessGroup(cmd)
	}

	// Timeout/cancel: НЕ затираем ctx.Err()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return makeResult(124, ctx.Err(), stdout.Bytes(), stderr.Bytes())
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return makeResult(1, ctx.Err(), stdout.Bytes(), stderr.Bytes())
	}

	exitCode := 0
	if err != nil {
		exitCode = 1
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			exitCode = ee.ExitCode()
		}
	}

	return makeResult(exitCode, err, stdout.Bytes(), stderr.Bytes())
}