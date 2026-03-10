import { apiFetch } from './client'
import type { QueueConfig, QueueEntry, QueueInfo, AllQueuesInfoData } from '@/types/queue'

export interface ListQueuesData {
  queues: QueueEntry[]
}

export interface QueueRequest {
  config: QueueConfig
  schema?: Record<string, unknown> | null
}

export function listQueues(): Promise<ListQueuesData> {
  return apiFetch('/api/queues')
}

export function createQueue(req: QueueRequest): Promise<{ name: string }> {
  return apiFetch('/api/queues', { method: 'POST', body: JSON.stringify(req) })
}

export function updateQueue(name: string, req: QueueRequest): Promise<{ name: string }> {
  return apiFetch(`/api/queues/${encodeURIComponent(name)}`, { method: 'PUT', body: JSON.stringify(req) })
}

export function deleteQueue(name: string): Promise<{ deleted: string }> {
  return apiFetch(`/api/queues/${encodeURIComponent(name)}`, { method: 'DELETE' })
}

export function getAllQueuesInfo(fromTimeMs?: number): Promise<AllQueuesInfoData> {
  const q = fromTimeMs ? `?from_time_ms=${fromTimeMs}` : ''
  return apiFetch(`/info${q}`)
}

export function getQueueInfo(queue: string, fromTimeMs?: number): Promise<QueueInfo> {
  const q = fromTimeMs ? `?from_time_ms=${fromTimeMs}` : ''
  return apiFetch(`/${encodeURIComponent(queue)}/info${q}`)
}
