'use client'

import { use } from 'react'
import Link from 'next/link'
import { ArrowLeft, CheckCircle, XCircle, Clock, Loader } from 'lucide-react'

import { buttonVariants } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { Separator } from '@/components/ui/separator'

import { useJobStatus, useJobResult } from '@/lib/hooks/use-jobs'
import type { JobStatus } from '@/types/job'

const TERMINAL: JobStatus[] = ['succeeded', 'failed']

export default function JobTrackerPage({
  params,
}: {
  params: Promise<{ queue: string; msgId: string }>
}) {
  const { queue, msgId } = use(params)
  const queueName = decodeURIComponent(queue)
  const messageId = decodeURIComponent(msgId)

  const { data: statusData, isLoading: statusLoading } = useJobStatus(queueName, messageId)
  const isTerminal = statusData?.status && TERMINAL.includes(statusData.status)

  const { data: resultData } = useJobResult(queueName, messageId, !!isTerminal)

  const status = statusData?.status ?? 'queued'

  return (
    <div className="mx-auto max-w-2xl p-6">
      <div className="mb-6 flex items-center gap-3">
        <Link href={`/queues/${encodeURIComponent(queueName)}`} className={buttonVariants({ variant: 'ghost', size: 'icon' })}>
          <ArrowLeft size={16} />
        </Link>
        <div>
          <h1 className="text-xl font-semibold">Job Tracker</h1>
          <p className="font-mono text-xs text-muted-foreground">{messageId}</p>
        </div>
      </div>

      {/* Status badge */}
      <div className="mb-6 flex items-center gap-3">
        <StatusIcon status={status} loading={statusLoading} />
        <div>
          <p className="text-sm text-muted-foreground">Queue: {queueName}</p>
          {!isTerminal && !statusLoading && (
            <p className="text-xs text-muted-foreground">Polling every 2s…</p>
          )}
        </div>
      </div>

      {/* Timeline */}
      <div className="mb-6 space-y-2">
        <h2 className="text-sm font-medium text-muted-foreground">Timeline</h2>
        <Timeline status={status} />
      </div>

      {/* Result */}
      {isTerminal && resultData?.result && (
        <>
          <Separator className="my-4" />
          <Card>
            <CardHeader>
              <CardTitle className="text-sm">Result</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3 text-sm">
              <Row label="Exit code" value={String(resultData.result.exit_code)} mono />
              <Row label="Duration" value={`${resultData.result.duration_ms}ms`} mono />
              {resultData.result.err && <Row label="Error" value={resultData.result.err} mono error />}
              {resultData.result.finished_at_ms && (
                <Row
                  label="Finished at"
                  value={new Date(resultData.result.finished_at_ms).toLocaleString()}
                />
              )}
            </CardContent>
          </Card>
        </>
      )}

      {statusLoading && <Skeleton className="h-24 w-full" />}
    </div>
  )
}

function StatusIcon({ status, loading }: { status: JobStatus; loading?: boolean }) {
  if (loading) return <Skeleton className="h-8 w-24" />
  const map: Record<JobStatus, { icon: React.ReactNode; label: string; cls: string }> = {
    queued: {
      icon: <Clock size={20} />,
      label: 'Queued',
      cls: 'text-muted-foreground',
    },
    processing: {
      icon: <Loader size={20} className="animate-spin" />,
      label: 'Processing',
      cls: 'text-blue-600 dark:text-blue-400',
    },
    succeeded: {
      icon: <CheckCircle size={20} />,
      label: 'Succeeded',
      cls: 'text-green-600 dark:text-green-400',
    },
    failed: {
      icon: <XCircle size={20} />,
      label: 'Failed',
      cls: 'text-red-600 dark:text-red-400',
    },
  }
  const { icon, label, cls } = map[status] ?? map.queued
  return (
    <div className={`flex items-center gap-2 text-lg font-semibold ${cls}`}>
      {icon}
      {label}
    </div>
  )
}

function Timeline({ status }: { status: JobStatus }) {
  const steps: { key: JobStatus | 'created'; label: string }[] = [
    { key: 'created', label: 'Created' },
    { key: 'queued', label: 'Queued' },
    { key: 'processing', label: 'Processing' },
    { key: status === 'failed' ? 'failed' : 'succeeded', label: status === 'failed' ? 'Failed' : 'Succeeded' },
  ]

  const ORDER: (JobStatus | 'created')[] = ['created', 'queued', 'processing', 'succeeded', 'failed']
  const currentIdx = ORDER.indexOf(status)

  return (
    <ol className="relative ml-2 border-l border-muted-foreground/20">
      {steps.map(({ key, label }, i) => {
        const idx = ORDER.indexOf(key)
        const done = idx <= currentIdx
        const active = idx === currentIdx
        return (
          <li key={key} className={`mb-3 ml-4 ${i === steps.length - 1 ? 'mb-0' : ''}`}>
            <span
              className={`absolute -left-1.5 h-3 w-3 rounded-full border ${done ? 'bg-primary border-primary' : 'bg-background border-muted-foreground/40'
                }`}
            />
            <p className={`text-sm ${active ? 'font-medium' : done ? 'text-muted-foreground' : 'text-muted-foreground/50'}`}>
              {label}
            </p>
          </li>
        )
      })}
    </ol>
  )
}

function Row({
  label,
  value,
  mono,
  error,
}: {
  label: string
  value: string
  mono?: boolean
  error?: boolean
}) {
  return (
    <div className="flex justify-between gap-4">
      <span className="text-muted-foreground">{label}</span>
      <span className={`${mono ? 'font-mono text-xs' : ''} ${error ? 'text-destructive' : ''}`}>{value}</span>
    </div>
  )
}
