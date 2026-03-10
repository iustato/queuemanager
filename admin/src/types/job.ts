export type JobStatus = 'queued' | 'processing' | 'succeeded' | 'failed'

export interface JobResult {
  exit_code: number
  duration_ms: number
  err?: string
  finished_at_ms?: number
}

export interface JobStatusData {
  queue: string
  msg_id: string
  status: JobStatus
}

export interface JobResultData {
  queue: string
  msg_id: string
  status: JobStatus
  result?: JobResult
}

export interface SubmitJobResult {
  msg_id: string
  idempotency_key: string
}
