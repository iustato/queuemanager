import { useQuery, useMutation } from '@tanstack/react-query'
import { submitJob, getJobStatus, getJobResult } from '@/lib/api/jobs'
import type { JobStatus } from '@/types/job'

const TERMINAL: JobStatus[] = ['succeeded', 'failed']

export const jobKeys = {
  status: (queue: string, msgId: string) => ['job', 'status', queue, msgId] as const,
  result: (queue: string, msgId: string) => ['job', 'result', queue, msgId] as const,
}

export function useJobStatus(queue: string, msgId: string, enabled = true) {
  return useQuery({
    queryKey: jobKeys.status(queue, msgId),
    queryFn: () => getJobStatus(queue, msgId),
    enabled: enabled && !!queue && !!msgId,
    refetchInterval: (query) => {
      const status = query.state.data?.status
      if (status && TERMINAL.includes(status)) return false
      return 2_000
    },
    retry: 1,
  })
}

export function useJobResult(queue: string, msgId: string, enabled = true) {
  return useQuery({
    queryKey: jobKeys.result(queue, msgId),
    queryFn: () => getJobResult(queue, msgId),
    enabled: enabled && !!queue && !!msgId,
    retry: false,
  })
}

export function useSubmitJob() {
  return useMutation({
    mutationFn: ({
      queue,
      payload,
      idempotencyKey,
    }: {
      queue: string
      payload: unknown
      idempotencyKey?: string
    }) => submitJob(queue, payload, idempotencyKey),
  })
}
