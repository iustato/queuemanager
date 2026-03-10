import { z } from 'zod'

export const idempotencyConfigSchema = z.object({
  accept_max_age: z.string().optional(),
  retention_min: z.string().optional(),
})

export const storageConfigSchema = z.object({
  retention: z.string().optional(),
  forever: z.boolean().optional(),
  gc_interval_sec: z.number().int().positive().optional(),
  gc_max_deletes: z.number().int().positive().optional(),
})

export const queueConfigSchema = z
  .object({
    name: z
      .string()
      .min(1, 'Queue name is required')
      .regex(/^[a-zA-Z0-9_-]+$/, 'Only letters, numbers, _ and - are allowed'),
    workers: z.number().int().min(1, 'At least 1 worker').max(100),
    runtime: z.enum(['exec', 'php-cgi', 'php-fpm']),
    command: z.array(z.string().min(1)).optional(),
    script: z.string().optional(),
    timeout_sec: z.number().int().positive().optional(),
    max_queue: z.number().int().positive().optional(),
    max_size: z.number().int().positive().optional(),
    fpm_network: z.string().optional(),
    fpm_address: z.string().optional(),
    fpm_dial_timeout_ms: z.number().int().positive().optional(),
    fpm_server_name: z.string().optional(),
    fpm_server_port: z.string().optional(),
    idempotency: idempotencyConfigSchema.optional(),
    storage: storageConfigSchema.optional(),
    max_retries: z.number().int().min(0).optional(),
    retry_delay_ms: z.number().int().min(0).optional(),
    log_dir: z.string().optional(),
    result_ttl: z.string().optional(),
    message_expiry: z.string().optional(),
    push_timeout_sec: z.number().int().positive().optional(),
    enqueue_wait_ms: z.number().int().positive().optional(),
    max_stdout_bytes: z.number().int().positive().optional(),
    max_stderr_bytes: z.number().int().positive().optional(),
    max_response_bytes: z.number().int().positive().optional(),
    truncate_on_limit: z.boolean().optional(),
    requeue_stuck_interval_sec: z.number().int().positive().optional(),
    requeue_stuck_batch_limit: z.number().int().positive().optional(),
    gc_batch_limit: z.number().int().positive().optional(),
  })
  .refine(
    (d) => d.runtime !== 'exec' || (d.command?.length ?? 0) > 0,
    { message: 'command is required for exec runtime', path: ['command'] },
  )
  .refine(
    (d) => d.runtime !== 'php-cgi' || !!d.script,
    { message: 'script is required for php-cgi runtime', path: ['script'] },
  )
  .refine(
    (d) => d.runtime !== 'php-fpm' || !!d.fpm_address,
    { message: 'fpm_address is required for php-fpm runtime', path: ['fpm_address'] },
  )

export type QueueFormValues = z.infer<typeof queueConfigSchema>
