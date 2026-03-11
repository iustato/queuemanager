package queue

import "encoding/json"

// workerStdout is the structured stdout contract for workers.
//
// Workers write JSON to stdout:
//
//	{"output": "...", "body": {...}}
//
// - output: stored as Result.Output, returned via GET /{queue}/result/{guid}
// - body:   replaces the message body in storage (used for retries)
//
// If stdout is empty → zero struct (no output, no body update).
// If stdout is not valid JSON → Output = raw text, Body = nil (graceful fallback).
type workerStdout struct {
	Output string          `json:"output,omitempty"`
	Body   json.RawMessage `json:"body,omitempty"`
}

func parseWorkerStdout(stdout []byte) workerStdout {
	if len(stdout) == 0 {
		return workerStdout{}
	}
	var ws workerStdout
	if err := json.Unmarshal(stdout, &ws); err != nil {
		// non-JSON stdout → treat whole output as raw text
		return workerStdout{Output: string(stdout)}
	}
	return ws
}
