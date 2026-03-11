'use client'

import { use } from 'react'
import { useRouter } from 'next/navigation'
import Link from 'next/link'
import { toast } from 'sonner'
import { ArrowLeft, Pencil, Trash2 } from 'lucide-react'
import { useState } from 'react'

import { Button, buttonVariants } from '@/components/ui/button'
import { cn } from '@/lib/utils'
import { Badge } from '@/components/ui/badge'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Skeleton } from '@/components/ui/skeleton'
import { Textarea } from '@/components/ui/textarea'
import { Label } from '@/components/ui/label'

import { useListQueues, useQueueInfo } from '@/lib/hooks/use-queues'
import { useSubmitJob } from '@/lib/hooks/use-jobs'
import { DeleteQueueDialog } from '@/components/queues/delete-dialog'

const TIME_OPTIONS = [
  { label: '1h', ms: 3_600_000 },
  { label: '24h', ms: 86_400_000 },
  { label: '7d', ms: 604_800_000 },
  { label: 'All', ms: 0 },
] as const

export default function QueueDetailPage({ params }: { params: Promise<{ queue: string }> }) {
  const { queue } = use(params)
  const queueName = decodeURIComponent(queue)

  const router = useRouter()
  const [timeFilter, setTimeFilter] = useState(0)
  const [jobPayload, setJobPayload] = useState('{}')
  const [deleteOpen, setDeleteOpen] = useState(false)

  const { data: info, isLoading: infoLoading } = useQueueInfo(queueName, timeFilter)
  const { data: listData } = useListQueues()
  const submit = useSubmitJob()

  const queueConfig = listData?.queues?.find((q) => q.name === queueName)?.config

  async function handleSubmitJob() {
    let parsed: unknown
    try {
      parsed = JSON.parse(jobPayload)
    } catch {
      toast.error('Invalid JSON payload')
      return
    }
    submit.mutate(
      { queue: queueName, payload: parsed },
      {
        onSuccess: (res) => {
          toast.success(`Job submitted: ${res.msg_id}`)
          router.push(`/queues/${encodeURIComponent(queueName)}/jobs/${encodeURIComponent(res.msg_id)}`)
        },
        onError: (e) => {
          toast.error(e instanceof Error ? e.message : 'Submit failed')
        },
      },
    )
  }

  return (
    <div className="flex flex-col gap-6 p-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <Link href="/" className={buttonVariants({ variant: 'ghost', size: 'icon' })}>
            <ArrowLeft size={16} />
          </Link>
          <div>
            <h1 className="text-xl font-semibold">{queueName}</h1>
            {queueConfig && (
              <div className="flex items-center gap-2 text-sm text-muted-foreground">
                <Badge variant="outline" className="font-mono">
                  {queueConfig.runtime}
                </Badge>
                <span>{queueConfig.workers} worker{queueConfig.workers !== 1 ? 's' : ''}</span>
              </div>
            )}
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Link
            href={`/queues/${encodeURIComponent(queueName)}/edit`}
            className={buttonVariants({ variant: 'outline', size: 'sm' })}
          >
            <Pencil size={14} className="mr-1" />
            Edit
          </Link>
          <Button
            variant="outline"
            size="sm"
            className="text-destructive hover:text-destructive"
            onClick={() => setDeleteOpen(true)}
          >
            <Trash2 size={14} className="mr-1" />
            Delete
          </Button>
        </div>
      </div>

      <Tabs defaultValue="stats">
        <TabsList>
          <TabsTrigger value="stats">Stats</TabsTrigger>
          <TabsTrigger value="submit">Submit Job</TabsTrigger>
          <TabsTrigger value="config">Config</TabsTrigger>
        </TabsList>

        {/* Stats tab */}
        <TabsContent value="stats" className="mt-4 space-y-4">
          <div className="flex items-center gap-1">
            <span className="mr-2 text-xs text-muted-foreground">Period:</span>
            {TIME_OPTIONS.map((opt) => (
              <Button
                key={opt.label}
                variant={timeFilter === opt.ms ? 'secondary' : 'ghost'}
                size="sm"
                className="h-7 px-3 text-xs"
                onClick={() => setTimeFilter(opt.ms)}
              >
                {opt.label}
              </Button>
            ))}
          </div>

          <div className="grid grid-cols-4 gap-4">
            <StatCard label="Succeeded" value={info?.succeeded} loading={infoLoading} color="text-green-700 dark:text-green-400" />
            <StatCard label="Failed" value={info?.failed} loading={infoLoading} color="text-red-700 dark:text-red-400" />
            <StatCard label="Retries" value={info?.retries} loading={infoLoading} color="text-yellow-700 dark:text-yellow-400" />
            <StatCard
              label="Avg Duration"
              value={info?.avg_duration_ms != null ? `${Math.round(info.avg_duration_ms)}ms` : undefined}
              loading={infoLoading}
            />
          </div>
        </TabsContent>

        {/* Submit tab */}
        <TabsContent value="submit" className="mt-4">
          <div className="max-w-lg space-y-4">
            <div className="space-y-2">
              <Label>JSON Payload</Label>
              <Textarea
                value={jobPayload}
                onChange={(e) => setJobPayload(e.target.value)}
                rows={10}
                className="font-mono text-sm"
                placeholder="{}"
              />
            </div>
            <Button onClick={handleSubmitJob} disabled={submit.isPending}>
              {submit.isPending ? 'Submitting…' : 'Submit Job'}
            </Button>
          </div>
        </TabsContent>

        {/* Config tab */}
        <TabsContent value="config" className="mt-4">
          {queueConfig ? (
            <pre className="overflow-auto rounded-lg border bg-muted p-4 text-xs">
              {JSON.stringify(queueConfig, null, 2)}
            </pre>
          ) : (
            <Skeleton className="h-32 w-full" />
          )}
        </TabsContent>
      </Tabs>

      <DeleteQueueDialog
        name={queueName}
        open={deleteOpen}
        onOpenChange={setDeleteOpen}
        onDeleted={() => router.push('/')}
      />
    </div>
  )
}

function StatCard({
  label,
  value,
  loading,
  color = '',
}: {
  label: string
  value?: number | string
  loading?: boolean
  color?: string
}) {
  return (
    <Card>
      <CardHeader className="pb-1">
        <CardTitle className="text-xs font-medium text-muted-foreground">{label}</CardTitle>
      </CardHeader>
      <CardContent>
        {loading ? (
          <Skeleton className="h-8 w-16" />
        ) : (
          <span className={`text-2xl font-bold tabular-nums ${color}`}>{value ?? 0}</span>
        )}
      </CardContent>
    </Card>
  )
}
