import { ApiRequestError } from '@/types/api'
import { apiFetch } from './client'
import type { JobStatus, JobStatusData, JobResultData, SubmitJobResult } from '@/types/job'

export async function submitJob(
  queue: string,
  payload: unknown,
  idempotencyKey?: string,
): Promise<SubmitJobResult> {
  const headers: Record<string, string> = {}
  if (idempotencyKey) {
    headers['Idempotency-Key'] = idempotencyKey
  }

  const res = await fetch(`/api/backend/${encodeURIComponent(queue)}/newmessage`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...headers },
    body: JSON.stringify(payload),
  })

  if (!res.ok) {
    throw new ApiRequestError(res.status, 'submit_failed', await res.text())
  }

  const msgId = res.headers.get('X-Message-Id') ?? ''
  const idemKey = res.headers.get('Idempotency-Key') ?? ''

  return { msg_id: msgId, idempotency_key: idemKey }
}

export async function getJobStatus(queue: string, msgId: string): Promise<JobStatusData> {
  try {
    return await apiFetch<JobStatusData>(`/${encodeURIComponent(queue)}/status/${encodeURIComponent(msgId)}`)
  } catch (e) {
    if (e instanceof ApiRequestError && e.code === 'not_ready') {
      return { queue, msg_id: msgId, status: 'processing' as JobStatus }
    }
    throw e
  }
}

export function getJobResult(queue: string, msgId: string): Promise<JobResultData> {
  return apiFetch(`/${encodeURIComponent(queue)}/result/${encodeURIComponent(msgId)}`)
}
