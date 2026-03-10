# Code Review Report — queuemanager

**Date:** 2026-03-09
**Reviewer:** Claude (AI Code Review)
**Codebase:** Go queue manager service with BoltDB persistence and PHP worker support

---

## Overview

Overall the architecture is solid: clean separation between HTTP handlers, queue service, runtime,
and storage. Structured logging, proper idempotency, at-least-once delivery with retry and GC are
all good design choices. However, several bugs and issues were found ranging from critical to minor.

---

## Critical Bugs

### 1. `fastcgi.go:107-112` — `absScript` computed but never used

```go
absScript, err := filepath.Abs(script)   // computed
...
params := buildPhpEnvMap(script, job)    // original relative `script` used instead!
```

`SCRIPT_FILENAME` in the FastCGI params will be a relative path. PHP-FPM **requires** an absolute
path for `SCRIPT_FILENAME`, so any relative script path will silently fail with a
"no input file specified" error.

**Fix:** Pass `absScript` to `buildPhpEnvMap` instead of `script`.

---

### 2. `manager_http.go:297` — No body size limit in `HandleReportDone`

```go
body, _ := io.ReadAll(r.Body)  // unbounded read!
```

`HandleNewMessage` correctly uses `readLimitedBody()`, but the internal `/done` callback reads the
entire body without a limit. A malicious or buggy worker could exhaust server memory.

**Fix:** Use `io.LimitReader` or `readLimitedBody` consistent with other handlers.

---

### 3. `manager_http.go:297-301` — Empty callback silently drops done status

```go
if len(bytes.TrimSpace(body)) == 0 {
    w.WriteHeader(http.StatusNoContent)
    return
}
```

If a worker sends an empty POST to `/internal/{queue}/done/{msg_id}`, the handler returns
`204 No Content` **without** calling `MarkDone`. The message stays stuck in `processing` state
until `RequeueStuck` rescues it (up to 15 seconds by default, then re-executed). This
backward-compat shortcut can cause silent duplicate executions.

---

## High Severity

### 4. `configs/queue2.yaml:9` — Hardcoded developer Windows/WSL path

```yaml
script: /mnt/c/Users/Den/Desktop/queue-service/configs/scripts/job.php
```

This is a developer's WSL path. It will fail immediately on any non-Windows machine or CI
environment. This config should never have been committed to the repository.

---

### 5. `main.go:277` — `StopAll()` has no timeout

```go
shutdownCtx, _ := context.WithTimeout(context.Background(), shutdownTimeout) // 10s
publicSrv.Shutdown(shutdownCtx)
internalSrv.Shutdown(shutdownCtx)
qm.StopAll()  // no timeout — calls wg.Wait() which can block forever
```

HTTP servers respect the shutdown timeout, but `StopAll()` → `rt.Stop()` → `rt.st.wg.Wait()`
has no deadline. If a worker is stuck in a subprocess that does not honor context cancellation,
the process will hang indefinitely on SIGTERM.

**Fix:** Pass the shutdown context into `StopAll` (or a derived context with a separate timeout).

---

### 6. `manager_http.go:371-383` — Timing attack on `checkWorkerToken`

```go
if strings.TrimSpace(r.Header.Get("X-Worker-Token")) == token {
```

Plain string comparison (`==`) is not constant-time. An attacker can brute-force the token
via timing side-channel.

**Fix:** Use `subtle.ConstantTimeCompare([]byte(got), []byte(token)) == 1`.

---

### 7. `runtime.go:664` — Unbounded log file accumulation

`appendStdStreams` creates one file per message: `{logDir}/{msg_id}.log`. With a busy queue
(e.g. 300 workers under continuous traffic), millions of files will accumulate in one directory.
There is no rotation, no size cap, and no cleanup. The BoltDB GC removes storage records but
does **not** remove the corresponding log files. This will eventually cause:

- Filesystem inode exhaustion
- `os.OpenFile` failures (silently logged as warnings only)
- Catastrophically slow directory listing

---

## Medium Severity

### 8. `engine.go:23` — `bp` (bProc) bucket missing from nil-check in `GC()`

```go
if bm == nil || bb == nil || bi == nil || be == nil || bf == nil {
    return ErrBucketMissing
}
// bp (bProc) is fetched above but not checked here
if bp != nil && meta.LeaseUntilMs > 0 { ... }  // guarded per-use instead
```

`bp` is excluded from the guard that validates all buckets exist. The per-use `bp != nil` check
prevents a panic, but a missing `bProc` bucket is silently tolerated in `GC()` while the same
condition would return `ErrBucketMissing` in `RequeueStuck`. Inconsistent and fragile.

---

### 9. `runtime.go:353-355` — "Fast stop" comment is misleading

```go
// Stop is a "fast" stop: workers exit on quit even if jobs channel still contains items.
```

The actual behavior: `jobs` is closed first, then `quit` is closed last (after the `bgMu`
barrier). Workers `range` over `jobs` and check `quit` before each job — but `quit` is not yet
closed during the barrier window, so workers continue processing buffered jobs. With a large
buffer this can take significant time, contradicting the "fast stop" claim.

---

### 10. HTTP 409 Conflict used for "not ready" status

`GetStatusAndResult` returns `ErrNotReady` for non-terminal message states, which the
`/status/{msg_id}` endpoint maps to `HTTP 409 Conflict`. HTTP 409 semantically means a resource
conflict (e.g. optimistic locking failure), not "result not ready yet". Clients polling for
status will receive 409 for all in-progress messages, which will confuse standard HTTP
clients and proxies.

**Fix:** Use `HTTP 200` with status in the body, or `HTTP 202 Accepted`.

---

### 11. `service.go:177-184` — UUIDv7-only idempotency key restriction is undocumented

```go
if parsed.Version() != uuid.Version(7) {
    return "", ErrInvalidIdemKey{Msg: "Idempotency-Key must be UUIDv7"}
}
```

Forcing UUIDv7 specifically is an unusual constraint not mentioned in the README. The standard
Idempotency-Key header (per Stripe/Google conventions) accepts any opaque string. Callers using
other UUID versions or custom string keys will receive a confusing `422` rejection.

---

## Low Severity / Style

### 12. `runtime.go:122` — Missing indentation

```go
rtStr := strings.TrimSpace(cfg.Runtime)
kind := RuntimeKind(rtStr)  // missing one tab of indentation
```

### 13. `manager_http.go:135-136` and `191-192` — Duplicate comments

```go
// GET /{queue}/status/{msg_id}
// GET /{queue}/status/{msg_id}  // exact duplicate
```

Same issue on lines 191-192 for `/result`.

### 14. `fastcgi.go:54-60` — Inconsistent indentation in `resetClient`

Uses 4-space indentation and contains a spurious blank line. The rest of the codebase uses tabs.

### 15. `storage/storage.go:233-234` — Two blank lines between functions

Go convention uses one blank line between top-level declarations.

---

## Summary

| # | Severity | Location | Issue |
|---|----------|----------|-------|
| 1 | **Critical** | `fastcgi.go:112` | `absScript` computed but relative path used — PHP-FPM will fail |
| 2 | **Critical** | `manager_http.go:297` | No body size limit in `/done` callback |
| 3 | **Critical** | `manager_http.go:299` | Empty callback silently ignores done status, causes duplicate execution |
| 4 | **High** | `queue2.yaml:9` | Developer's Windows/WSL path hardcoded in config |
| 5 | **High** | `main.go:277` | `StopAll()` has no timeout — can block shutdown indefinitely |
| 6 | **High** | `manager_http.go:371` | Non-constant-time token comparison — timing attack vector |
| 7 | **High** | `runtime.go:664` | Log files never cleaned up — disk/inode exhaustion in production |
| 8 | **Medium** | `engine.go:23` | `bp` bucket missing from nil-check in `GC()` |
| 9 | **Medium** | `runtime.go:353` | "Fast stop" comment misleading — workers drain full buffer |
| 10 | **Medium** | `storage.go:259` + `manager_http.go:167` | HTTP 409 Conflict incorrect for "not ready" state |
| 11 | **Medium** | `service.go:181` | UUIDv7-only key restriction undocumented and non-standard |
| 12 | Low | `runtime.go:122` | Missing indentation |
| 13 | Low | `manager_http.go:135,191` | Duplicate comments |
| 14 | Low | `fastcgi.go:54` | Inconsistent indentation (spaces vs tabs) |
| 15 | Low | `storage.go:233` | Two blank lines between functions |

---

## Priority Recommendations

The three most urgent fixes before any production deployment:

1. **Fix `absScript` in FastCGI runner** (#1) — the php-fpm runtime is currently broken for
   any relative script path.
2. **Add body size limit and fix empty-body handling in `HandleReportDone`** (#2, #3) — the
   internal callback endpoint is both a memory exhaustion risk and a source of duplicate
   job execution.
3. **Add log file cleanup to GC** (#7) — without it, long-running deployments will exhaust
   disk inodes silently.
