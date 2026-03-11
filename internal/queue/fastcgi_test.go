package queue

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

func TestPooledFastCGIRunner_MakeResult_And_MakeResultOut(t *testing.T) {
	r := &PooledFastCGIRunner{}
	job := Job{Queue: "q1", MessageGUID: "m1"}

	start := time.Now().Add(-10 * time.Millisecond)

	res := r.makeResult(job, start, 1, 500, context.Canceled)
	if res.Queue != "q1" || res.MessageGUID != "m1" {
		t.Fatalf("unexpected ids: %#v", res)
	}
	if res.ExitCode != 1 || res.HTTPStatus != 500 {
		t.Fatalf("unexpected codes: %#v", res)
	}
	if res.Err == nil {
		t.Fatalf("expected err")
	}

	out := []byte("out")
	errb := []byte("err")
	res2 := r.makeResultOut(job, start, 0, 200, out, errb, nil)
	if res2.ExitCode != 0 || res2.HTTPStatus != 200 {
		t.Fatalf("unexpected codes: %#v", res2)
	}
	if string(res2.Stdout) != "out" || string(res2.Stderr) != "err" {
		t.Fatalf("unexpected streams: %#v", res2)
	}
}

func TestPooledFastCGIRunner_EnsureClient_Dials_And_ResetClient(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	accepted := make(chan struct{}, 1)
	go func() {
		c, e := ln.Accept()
		if e != nil {
			return
		}
		defer c.Close()
		accepted <- struct{}{}
		time.Sleep(200 * time.Millisecond)
	}()

	r := &PooledFastCGIRunner{
		Network:     "tcp",
		Address:     ln.Addr().String(),
		DialTimeout: 300 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	c, err := r.ensureClient(ctx)
	if err != nil {
		t.Fatalf("ensureClient: %v", err)
	}
	if c == nil || r.client == nil {
		t.Fatalf("expected client to be set")
	}

	select {
	case <-accepted:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("server did not accept connection")
	}

	// cached
	c2, err := r.ensureClient(ctx)
	if err != nil {
		t.Fatalf("ensureClient #2: %v", err)
	}
	if c2 != c {
		t.Fatalf("expected same cached client")
	}

	r.resetClient()
	if r.client != nil {
		t.Fatalf("expected client=nil after resetClient")
	}
}

func TestPooledFastCGIRunner_EnsureClient_ContextAlreadyExpired(t *testing.T) {
	r := &PooledFastCGIRunner{
		Network:     "tcp",
		Address:     "127.0.0.1:1",
		DialTimeout: 3 * time.Second,
	}

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-1*time.Second))
	defer cancel()

	_, err := r.ensureClient(ctx)
	if err == nil {
		t.Fatalf("expected error")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "deadline") {
		t.Fatalf("expected deadline-ish error, got: %v", err)
	}
}

func TestPooledFastCGIRunner_Run_Busy(t *testing.T) {
	r := &PooledFastCGIRunner{
		Network: "tcp",
		Address: "127.0.0.1:1",
	}
	r.busy.Store(true)

	res := r.Run(context.Background(), nil, "job.php", Job{Queue: "q", MessageGUID: "m"})
	if res.Err != ErrRunnerBusy {
		t.Fatalf("expected ErrRunnerBusy, got: %v", res.Err)
	}
	if res.ExitCode != 1 {
		t.Fatalf("expected exitCode=1, got %d", res.ExitCode)
	}
}

func TestPooledFastCGIRunner_Run_NoScript(t *testing.T) {
	r := &PooledFastCGIRunner{
		Network: "tcp",
		Address: "127.0.0.1:1",
	}
	res := r.Run(context.Background(), nil, "   ", Job{Queue: "q", MessageGUID: "m"})
	if res.ExitCode != 1 || res.Err == nil {
		t.Fatalf("expected exitCode=1 and err, got: %#v", res)
	}
}

func TestPooledFastCGIRunner_Run_NoNetworkOrAddress(t *testing.T) {
	r := &PooledFastCGIRunner{}
	res := r.Run(context.Background(), nil, "job.php", Job{Queue: "q", MessageGUID: "m"})
	if res.ExitCode != 1 || res.Err == nil {
		t.Fatalf("expected exitCode=1 and err, got: %#v", res)
	}
}

func TestPooledFastCGIRunner_Run_ContextCanceled(t *testing.T) {
	r := &PooledFastCGIRunner{
		Network: "tcp",
		Address: "127.0.0.1:1",
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	res := r.Run(ctx, nil, "job.php", Job{Queue: "q", MessageGUID: "m"})
	if res.ExitCode != 1 || res.Err == nil {
		t.Fatalf("expected exitCode=1 and err, got: %#v", res)
	}
}