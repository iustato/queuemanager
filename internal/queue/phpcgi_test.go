package queue

import (
	"context"
	"testing"
)

func TestPHPCGIRunner_Run_NoScript(t *testing.T) {
	r := PHPCGIRunner{}
	res := r.Run(context.Background(), nil, "   ", Job{Queue: "q", MessageGUID: "m"})
	if res.ExitCode != 1 || res.Err == nil {
		t.Fatalf("expected exitCode=1 and err, got: %#v", res)
	}
}

func TestPHPCGIRunner_Run_ContextCanceled(t *testing.T) {
	r := PHPCGIRunner{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	res := r.Run(ctx, nil, "job.php", Job{Queue: "q", MessageGUID: "m"})
	if res.ExitCode != 1 || res.Err == nil {
		t.Fatalf("expected exitCode=1 and err, got: %#v", res)
	}
}