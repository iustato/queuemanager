package queue

import (
    "context"
    "errors"
    "fmt"
    "strings"
    "time"

    "go-web-server/internal/config"
    uuidutil "go-web-server/internal/uuid"
    "go-web-server/internal/validate"

    "github.com/google/uuid"
)

type PushRequest struct {
	Queue    string
	Body     []byte
	IdemKey  string
	AutoIdem bool

	Method      string
	QueryString string
	ContentType string
	RemoteAddr  string
}

type PushResponse struct {
	MsgID   string
	IdemKey string
	Created bool // false => dedup

	Queued bool // true => rt.Enqueue succeeded and MarkQueued done
}

type QueueService struct {
	mgr *Manager
}

func NewQueueService(mgr *Manager) *QueueService { return &QueueService{mgr: mgr} }

type ErrInvalidIdemKey struct{ Msg string }
func (e ErrInvalidIdemKey) Error() string { return e.Msg }

type ErrInvalidJSON struct{ Cause error }
func (e ErrInvalidJSON) Error() string { return "invalid json" }
func (e ErrInvalidJSON) Unwrap() error { return e.Cause }

type ErrSchemaValidation struct{ Cause error }
func (e ErrSchemaValidation) Error() string { return "schema validation failed" }
func (e ErrSchemaValidation) Unwrap() error { return e.Cause }

type ErrPayloadTooLarge struct{ LimitBytes int64 }
func (e ErrPayloadTooLarge) Error() string { return "payload too large" }

func (s *QueueService) Push(ctx context.Context, req PushRequest) (PushResponse, error) {
	if err := ctx.Err(); err != nil {
		return PushResponse{}, err
	}

	rt, ok := s.mgr.Get(req.Queue)
	if !ok || rt == nil {
		return PushResponse{}, ErrQueueNotFound
	}

	idemKey, err := normalizeIdemKey(req.IdemKey, req.AutoIdem)
	if err != nil {
		return PushResponse{}, err
	}

	limit := int64(maxBodyBytes)
	if rt.Cfg.MaxSize > 0 && int64(rt.Cfg.MaxSize) < limit {
		limit = int64(rt.Cfg.MaxSize)
	}
	if int64(len(req.Body)) > limit {
		return PushResponse{}, ErrPayloadTooLarge{LimitBytes: limit}
	}

	if err := rt.Schema.ValidateBytes(req.Body); err != nil {
		if errors.Is(err, validate.ErrInvalidJSON) {
			return PushResponse{}, ErrInvalidJSON{Cause: err}
		}
		return PushResponse{}, ErrSchemaValidation{Cause: err}
	}

	msgID, err := uuidutil.NewV7()
	if err != nil {
		return PushResponse{}, fmt.Errorf("msg_id_generation_failed: %w", err)
	}
	effectiveMsgID := msgID

	messageExpiry, _ := config.ParseDurationExt(rt.Cfg.MessageExpiry)
	if messageExpiry <= 0 {
		messageExpiry = 30 * 24 * time.Hour
	}
	expiresAtMs := time.Now().Add(messageExpiry).UnixMilli()

	if rt.Store != nil {
		storedMsgID, wasCreated, err := rt.Store.PutNewMessage(
			ctx,
			req.Queue,
			msgID,
			req.Body,
			idemKey,
			time.Now().UnixMilli(),
			expiresAtMs,
		)
		if err != nil {
			return PushResponse{MsgID: msgID, IdemKey: idemKey}, err
		}
		if !wasCreated {
			return PushResponse{
				MsgID:   storedMsgID,
				IdemKey: idemKey,
				Created: false,
				Queued:  false,
			}, nil
		}
		effectiveMsgID = storedMsgID
	}

	job := Job{
		Queue:          req.Queue,
		MsgID:          effectiveMsgID,
		Body:           req.Body,
		EnqueuedAt:     time.Now(),
		Attempt:        1,
		Method:         req.Method,
		QueryString:    req.QueryString,
		ContentType:    req.ContentType,
		RemoteAddr:     req.RemoteAddr,
		IdempotencyKey: idemKey,
	}

	if err := rt.Enqueue(ctx, job); err != nil {
		return PushResponse{
			MsgID:   effectiveMsgID,
			IdemKey: idemKey,
			Created: true,
			Queued:  false,
		}, err
	}

	if rt.Store != nil {
		if err := rt.Store.MarkQueued(effectiveMsgID); err != nil {
			return PushResponse{
				MsgID:   effectiveMsgID,
				IdemKey: idemKey,
				Created: true,
				Queued:  true,
			}, err
		}
	}

	return PushResponse{
		MsgID:   effectiveMsgID,
		IdemKey: idemKey,
		Created: true,
		Queued:  true,
	}, nil
}

func normalizeIdemKey(raw string, auto bool) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		if !auto {
			return "", ErrInvalidIdemKey{Msg: "missing Idempotency-Key header"}
		}
		gen, err := uuidutil.NewV7()
		if err != nil {
			return "", err
		}
		return gen, nil
	}

	parsed, err := uuid.Parse(raw)
	if err != nil {
		return "", ErrInvalidIdemKey{Msg: "invalid Idempotency-Key format"}
	}
	if parsed.Version() != uuid.Version(7) {
		return "", ErrInvalidIdemKey{Msg: "Idempotency-Key must be UUIDv7"}
	}
	return raw, nil
}

// чтобы go vet не ругался, если где-то хочешь errors.Is на schema/json
var _ = errors.Is