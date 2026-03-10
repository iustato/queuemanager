package queue

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

type TestRunnerMode string

const (
	ModeOK    TestRunnerMode = "ok"
	ModeFail  TestRunnerMode = "fail"
	ModePanic TestRunnerMode = "panic"
	ModeBlock TestRunnerMode = "block"
)

// TestRunner — управляемый runner для стабильных интеграционных/E2E тестов.
type TestRunner struct {
	mode  atomic.Value // stores TestRunnerMode
	calls atomic.Int32
	done  chan string

	// optional: customize fail error
	failErr error
}

func NewTestRunner(mode TestRunnerMode) *TestRunner {
	tr := &TestRunner{
		done:    make(chan string, 100),
		failErr: errors.New("runner failed"),
	}
	tr.mode.Store(mode)
	return tr
}

func (tr *TestRunner) SetMode(m TestRunnerMode) { tr.mode.Store(m) }
func (tr *TestRunner) Calls() int               { return int(tr.calls.Load()) }
func (tr *TestRunner) Done() <-chan string      { return tr.done }
func (tr *TestRunner) SetFailErr(err error) {
	if err == nil {
		err = errors.New("runner failed")
	}
	tr.failErr = err
}

func (tr *TestRunner) Run(ctx context.Context, _ []string, _ string, job Job) Result {
	start := time.Now()
	tr.calls.Add(1)

	m, _ := tr.mode.Load().(TestRunnerMode)

	switch m {
	case ModeOK:
		select {
		case tr.done <- job.MsgID:
		default:
		}
		return Result{
			Queue:      job.Queue,
			MsgID:      job.MsgID,
			ExitCode:   0,
			DurationMs: time.Since(start).Milliseconds(),
		}

	case ModeFail:
		select {
		case tr.done <- job.MsgID:
		default:
		}
		return Result{
			Queue:      job.Queue,
			MsgID:      job.MsgID,
			ExitCode:   1,
			DurationMs: time.Since(start).Milliseconds(),
			Err:        tr.failErr,
		}

	case ModePanic:
		panic("boom")

	case ModeBlock:
		<-ctx.Done()
		select {
		case tr.done <- job.MsgID:
		default:
		}
		return Result{
			Queue:      job.Queue,
			MsgID:      job.MsgID,
			ExitCode:   1,
			DurationMs: time.Since(start).Milliseconds(),
			Err:        ctx.Err(),
		}

	default:
		select {
		case tr.done <- job.MsgID:
		default:
		}
		return Result{
			Queue:      job.Queue,
			MsgID:      job.MsgID,
			ExitCode:   0,
			DurationMs: time.Since(start).Milliseconds(),
		}
	}
}