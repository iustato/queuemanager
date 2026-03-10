export interface IdempotencyConfig {
  accept_max_age?: string
  retention_min?: string
}

export interface StorageConfig {
  retention?: string
  forever?: boolean
  gc_interval_sec?: number
  gc_max_deletes?: number
}

export interface QueueConfig {
  name: string
  schema_file?: string
  max_size?: number
  workers: number
  runtime: string
  script?: string
  timeout_sec?: number
  max_queue?: number
  command?: string[]
  fpm_network?: string
  fpm_address?: string
  fpm_dial_timeout_ms?: number
  fpm_server_name?: string
  fpm_server_port?: string
  idempotency?: IdempotencyConfig
  storage?: StorageConfig
  max_retries?: number
  retry_delay_ms?: number
  log_dir?: string
  result_ttl?: string
  message_expiry?: string
  push_timeout_sec?: number
  enqueue_wait_ms?: number
  max_stdout_bytes?: number
  max_stderr_bytes?: number
  max_response_bytes?: number
  truncate_on_limit?: boolean
  requeue_stuck_interval_sec?: number
  requeue_stuck_batch_limit?: number
  gc_batch_limit?: number
}

export interface QueueEntry {
  name: string
  config: QueueConfig
}

export interface QueueInfo {
  queue: string
  succeeded: number
  failed: number
  retries: number
  avg_duration_ms: number
  from_time_ms: number
}

export interface QueueRow {
  queue: string
  succeeded: number
  failed: number
  retries: number
  avg_duration_ms: number
}

export interface AllQueuesInfoData {
  from_time_ms: number
  queues: QueueRow[]
}
