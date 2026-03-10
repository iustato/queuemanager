import { ApiRequestError } from '@/types/api'

const BACKEND = '/api/backend'

export async function apiFetch<T>(path: string, options?: RequestInit): Promise<T> {
  const url = `${BACKEND}${path}`
  const res = await fetch(url, {
    headers: { 'Content-Type': 'application/json', ...options?.headers },
    ...options,
  })

  const contentType = res.headers.get('content-type') ?? ''
  if (!contentType.includes('application/json')) {
    if (!res.ok) {
      throw new ApiRequestError(res.status, 'http_error', await res.text())
    }
    return res as unknown as T
  }

  const json = await res.json()

  if (!json.ok) {
    const err = json.error ?? {}
    throw new ApiRequestError(res.status, err.code ?? 'unknown', err.message ?? 'Unknown error', err.details)
  }

  return json.data as T
}
