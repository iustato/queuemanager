package storage

type Status string

const (
    StatusCreated       Status = "created"
    StatusQueued        Status = "queued"
    StatusEnqueueFailed Status = "enqueue_failed"
    StatusProcessing    Status = "processing"
    StatusSucceeded     Status = "succeeded"
    StatusFailed        Status = "failed"
)