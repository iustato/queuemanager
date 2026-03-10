export interface ApiOK<T> {
  ok: true
  data: T
  request_id?: string
}

export interface ApiErrorBody {
  ok: false
  error: {
    code: string
    message: string
    details?: unknown
  }
  request_id?: string
}

export type ApiResponse<T> = ApiOK<T> | ApiErrorBody

export class ApiRequestError extends Error {
  constructor(
    public readonly status: number,
    public readonly code: string,
    message: string,
    public readonly details?: unknown,
  ) {
    super(message)
    this.name = 'ApiRequestError'
  }
}
