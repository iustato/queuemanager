'use client'

import { use } from 'react'
import { useRouter } from 'next/navigation'
import Link from 'next/link'
import { toast } from 'sonner'
import { ArrowLeft } from 'lucide-react'

import { buttonVariants } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { QueueForm } from '@/components/queues/queue-form'
import { useListQueues, useUpdateQueue } from '@/lib/hooks/use-queues'
import type { QueueFormValues } from '@/lib/schemas/queue'

export default function EditQueuePage({ params }: { params: Promise<{ queue: string }> }) {
  const { queue } = use(params)
  const queueName = decodeURIComponent(queue)

  const router = useRouter()
  const { data: listData, isLoading } = useListQueues()
  const update = useUpdateQueue()

  const entry = listData?.queues?.find((q) => q.name === queueName)

  async function handleSubmit(values: QueueFormValues, schema: Record<string, unknown> | null) {
    await update.mutateAsync(
      { name: queueName, req: { config: values, schema } },
      {
        onSuccess: () => {
          toast.success(`Queue "${queueName}" updated`)
          router.push(`/queues/${encodeURIComponent(queueName)}`)
        },
        onError: (e) => {
          toast.error(e instanceof Error ? e.message : 'Update failed')
        },
      },
    )
  }

  return (
    <div className="mx-auto max-w-2xl p-6">
      <div className="mb-6 flex items-center gap-3">
        <Link href={`/queues/${encodeURIComponent(queueName)}`} className={buttonVariants({ variant: 'ghost', size: 'icon' })}>
          <ArrowLeft size={16} />
        </Link>
        <div>
          <h1 className="text-xl font-semibold">Edit &quot;{queueName}&quot;</h1>
          <p className="text-sm text-muted-foreground">Modify queue configuration</p>
        </div>
      </div>

      {isLoading && <Skeleton className="h-64 w-full" />}

      {!isLoading && !entry && (
        <p className="text-muted-foreground">Queue not found.</p>
      )}

      {!isLoading && entry && (
        <QueueForm
          defaultValues={entry.config}
          onSubmit={handleSubmit}
          submitLabel="Save Changes"
          nameReadonly
        />
      )}
    </div>
  )
}
