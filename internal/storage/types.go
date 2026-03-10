package storage

import (
	"errors"

	bolt "go.etcd.io/bbolt"
)
// Buckets (NEW MODEL)
var (
	bMeta = []byte("meta") // msg_id -> Meta(msgpack)
	bBody = []byte("body") // msg_id -> raw payload bytes
	bIdem = []byte("idem") // idem_key -> msg_id
	bExp  = []byte("exp")  // expKey(8B expiresAtMs + msg_id) -> msg_id
    bProc = []byte("proc") // procKey(8B leaseUntilMs + msg_id) -> msg_id
	bEnqFail = []byte("enqfail") // новый индекс для быстрого получения сообщений с неудачной попыткой постановки в очередь: enqFailKey(8B failedAtMs + msg_id) -> msg_id
)

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("message already exists")
	ErrNotReady = errors.New("result not ready")
	ErrBucketMissing = errors.New("storage bucket missing")
)

type Store struct {
	db *bolt.DB
	processingTimeoutMs int64
    gcProcessingGraceMs int64
}

type Meta struct {
	Queue          string `msgpack:"Queue" json:"queue"`
	MsgID          string `msgpack:"MsgID" json:"msg_id"`
	IdempotencyKey string `msgpack:"IdempotencyKey" json:"idempotency_key"`

	EnqueuedAtMs int64  `msgpack:"EnqueuedAtMs" json:"enqueued_at_ms"`
	Status       Status `msgpack:"Status" json:"status"`
	Attempt      int    `msgpack:"Attempt" json:"attempt"`
	Result       Result `msgpack:"Result,omitempty" json:"result,omitempty"`

	// для метрик и разборов
	LastRequeueReason RequeueReason `msgpack:"LastRequeueReason,omitempty" json:"last_requeue_reason,omitempty"`
	LastRequeueAtMs   int64         `msgpack:"LastRequeueAtMs,omitempty" json:"last_requeue_at_ms,omitempty"`

	// 0 => forever
	ExpiresAtMs    int64 `msgpack:"ExpiresAtMs" json:"expires_at_ms"`
	StartedAtMs    int64 `msgpack:"StartedAtMs,omitempty" json:"started_at_ms,omitempty"`
	LeaseUntilMs   int64 `msgpack:"LeaseUntilMs,omitempty" json:"lease_until_ms,omitempty"`
	UpdatedAtMs    int64 `msgpack:"UpdatedAtMs,omitempty" json:"updated_at_ms,omitempty"`
	ExecutedTimeMs int64 `msgpack:"ExecutedTimeMs,omitempty" json:"executed_time_ms,omitempty"`

	EnqueueFailedAtMs int64  `msgpack:"EnqueueFailedAtMs,omitempty" json:"enqueue_failed_at_ms,omitempty"`
	EnqueueError      string `msgpack:"EnqueueError,omitempty" json:"enqueue_error,omitempty"`
}

type Result struct {
	ExitCode   int    `msgpack:"ExitCode" json:"exit_code"`
	DurationMs int64  `msgpack:"DurationMs" json:"duration_ms"`
	Err        string `msgpack:"Err,omitempty" json:"err,omitempty"`
	FinishedAt int64  `msgpack:"FinishedAt,omitempty" json:"finished_at_ms,omitempty"`
}

type QueueInfo struct {
	Succeeded     int     `json:"succeeded"`
	Failed        int     `json:"failed"`
	Retries       int     `json:"retries"`
	AvgDurationMs float64 `json:"avg_duration_ms"`
}

type RequeueReason string

const (
	RequeueNone         RequeueReason = ""
	RequeueLeaseExpired RequeueReason = "lease_expired" // зависла в processing и lease истёк
	RequeueRetry        RequeueReason = "retry"         // retry после ошибки/таймаута
	defaultProcessingTimeoutMs int64 = 120_000
	defaultGCProcessingGraceMs int64 = 120_000
)

