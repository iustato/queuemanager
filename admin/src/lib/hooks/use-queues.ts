import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  listQueues,
  getAllQueuesInfo,
  getQueueInfo,
  createQueue,
  updateQueue,
  deleteQueue,
  type QueueRequest,
} from '@/lib/api/queues'

export const queueKeys = {
  all: ['queues'] as const,
  list: () => [...queueKeys.all, 'list'] as const,
  info: (queue?: string, from?: number) => [...queueKeys.all, 'info', queue ?? 'all', from ?? 0] as const,
}

export function useListQueues() {
  return useQuery({
    queryKey: queueKeys.list(),
    queryFn: listQueues,
  })
}

export function useAllQueuesInfo(fromTimeMs?: number) {
  return useQuery({
    queryKey: queueKeys.info(undefined, fromTimeMs),
    queryFn: () => getAllQueuesInfo(fromTimeMs),
    refetchInterval: 5_000,
  })
}

export function useQueueInfo(queue: string, fromTimeMs?: number) {
  return useQuery({
    queryKey: queueKeys.info(queue, fromTimeMs),
    queryFn: () => getQueueInfo(queue, fromTimeMs),
    enabled: !!queue,
    refetchInterval: 3_000,
  })
}

export function useCreateQueue() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (req: QueueRequest) => createQueue(req),
    onSuccess: () => qc.invalidateQueries({ queryKey: queueKeys.all }),
  })
}

export function useUpdateQueue() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ name, req }: { name: string; req: QueueRequest }) => updateQueue(name, req),
    onSuccess: () => qc.invalidateQueries({ queryKey: queueKeys.all }),
  })
}

export function useDeleteQueue() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (name: string) => deleteQueue(name),
    onSuccess: () => qc.invalidateQueries({ queryKey: queueKeys.all }),
  })
}
