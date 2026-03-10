'use client'

import { toast } from 'sonner'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { useDeleteQueue } from '@/lib/hooks/use-queues'

interface Props {
  name: string | null
  open: boolean
  onOpenChange: (open: boolean) => void
  onDeleted?: () => void
}

export function DeleteQueueDialog({ name, open, onOpenChange, onDeleted }: Props) {
  const del = useDeleteQueue()

  function handleDelete() {
    if (!name) return
    del.mutate(name, {
      onSuccess: () => {
        toast.success(`Queue "${name}" deleted`)
        onOpenChange(false)
        onDeleted?.()
      },
      onError: (e) => {
        toast.error(e instanceof Error ? e.message : 'Delete failed')
      },
    })
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Delete queue &quot;{name}&quot;?</DialogTitle>
          <DialogDescription>
            This will stop the queue runtime and remove all associated data. This action cannot be undone.
          </DialogDescription>
        </DialogHeader>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button variant="destructive" onClick={handleDelete} disabled={del.isPending}>
            {del.isPending ? 'Deleting…' : 'Delete'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
