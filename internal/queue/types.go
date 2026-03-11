package queue

import (
	"context"
	"time"
)

type Job struct {
	Queue       string
	MessageGUID string
	Body        []byte
	EnqueuedAt  time.Time
	Attempt     int
	Method      string
	QueryString string
	ContentType string
	RemoteAddr  string
}

type Result struct {
	Queue       string
	MessageGUID string
	ExitCode    int
	DurationMs  int64
	Err         error
	Stdout      []byte
	Stderr      []byte
	HTTPStatus  int
}

type Runner interface {
	Run(ctx context.Context, cmdArgs []string, script string, job Job) Result
}

const (
	headerMessageGUID = "X-Message-GUID"
	cmdNewMessage     = "newmessage"
	maxBodyBytes      = 1 << 20 // 1 MiB
)
