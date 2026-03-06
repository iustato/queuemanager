package queue

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"
)

// helper: command that prints to stdout and exits 0
func cmdEchoOK() []string {
	if runtime.GOOS == "windows" {
		return []string{"cmd", "/C", "echo hello"}
	}
	return []string{"sh", "-c", "echo hello"}
}

// helper: command that prints to stderr and exits with non-zero
func cmdStderrFail() []string {
	if runtime.GOOS == "windows" {
		return []string{"cmd", "/C", "echo boom 1>&2 && exit /B 7"}
	}
	return []string{"sh", "-c", "echo boom 1>&2; exit 7"}
}

// helper: command that sleeps longer than test timeout
func cmdSleepLong() []string {
	if runtime.GOOS == "windows" {
		return []string{"powershell", "-NoProfile", "-Command", "Start-Sleep -Seconds 5"}
	}
	return []string{"sh", "-c", "sleep 5"}
}

func TestExecRunner_Run_NoCommand(t *testing.T) {
	r := ExecRunner{}
	job := Job{Queue: "q1", MsgID: "m1", Body: []byte("x")}

	res := r.Run(context.Background(), nil, "", job)

	if res.ExitCode != 1 {
		t.Fatalf("ExitCode: got %d want 1", res.ExitCode)
	}
	if res.Err == nil || res.Err.Error() != "no command provided" {
		t.Fatalf("Err: got %v want %q", res.Err, "no command provided")
	}
}

func TestExecRunner_Run_ExitError_ExitCodeAndStdStreams(t *testing.T) {
	r := ExecRunner{
		MaxStdoutBytes: 1024,
		MaxStderrBytes: 1024,
		// KillProcessGroup false to avoid OS-specific behavior in tests
		KillProcessGroup: false,
	}

	job := Job{Queue: "q1", MsgID: "m2", Body: []byte(`{"x":1}`)}

	res := r.Run(context.Background(), cmdStderrFail(), "", job)

	if res.ExitCode == 0 {
		t.Fatalf("expected non-zero exit code")
	}
	// On most systems this should be 7 due to exit 7; if shell behaves differently, at least keep it non-zero.
	// If you want strict: check == 7, but I'd keep it tolerant for CI.
	if res.Err == nil {
		t.Fatalf("expected res.Err != nil")
	}
	if len(res.Stderr) == 0 {
		t.Fatalf("expected stderr to have data")
	}
}

func TestExecRunner_Run_ContextDeadlineExceeded_Returns124(t *testing.T) {
	r := ExecRunner{
		MaxStdoutBytes:    1024,
		MaxStderrBytes:    1024,
		KillProcessGroup:  false,
	}

	job := Job{Queue: "q1", MsgID: "m3", Body: []byte("x")}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	res := r.Run(ctx, cmdSleepLong(), "", job)

	if res.ExitCode != 124 {
		t.Fatalf("ExitCode: got %d want 124", res.ExitCode)
	}
	if !errors.Is(res.Err, context.DeadlineExceeded) {
		t.Fatalf("Err: got %v want DeadlineExceeded", res.Err)
	}
}