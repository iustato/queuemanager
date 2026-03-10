'use client'

import { useRouter } from 'next/navigation'
import { toast } from 'sonner'
import { ArrowLeft } from 'lucide-react'
import Link from 'next/link'

import { buttonVariants } from '@/components/ui/button'
import { QueueForm } from '@/components/queues/queue-form'
import { useCreateQueue } from '@/lib/hooks/use-queues'
import type { QueueFormValues } from '@/lib/schemas/queue'

export default function NewQueuePage() {
  const router = useRouter()
  const create = useCreateQueue()

  async function handleSubmit(values: QueueFormValues, schema: Record<string, unknown> | null) {
    await create.mutateAsync(
      { config: values, schema },
      {
        onSuccess: () => {
          toast.success(`Queue "${values.name}" created`)
          router.push(`/queues/${encodeURIComponent(values.name)}`)
        },
        onError: (e) => {
          toast.error(e instanceof Error ? e.message : 'Create failed')
        },
      },
    )
  }

  return (
    <div className="mx-auto max-w-2xl p-6">
      <div className="mb-6 flex items-center gap-3">
        <Link href="/" className={buttonVariants({ variant: 'ghost', size: 'icon' })}>
          <ArrowLeft size={16} />
        </Link>
        <div>
          <h1 className="text-xl font-semibold">New Queue</h1>
          <p className="text-sm text-muted-foreground">Configure and create a new queue</p>
        </div>
      </div>
      <QueueForm onSubmit={handleSubmit} submitLabel="Create Queue" />
    </div>
  )
}
