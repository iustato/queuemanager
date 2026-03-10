'use client'

import { useState, useMemo } from 'react'
import Link from 'next/link'
import { Plus, RefreshCw, Trash2, Pencil } from 'lucide-react'

import { Button, buttonVariants } from '@/components/ui/button'
import { cn } from '@/lib/utils'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { useAllQueuesInfo, useListQueues } from '@/lib/hooks/use-queues'
import { DeleteQueueDialog } from '@/components/queues/delete-dialog'

const TIME_OPTIONS = [
  { label: '1h', ms: 3_600_000 },
  { label: '24h', ms: 86_400_000 },
  { label: '7d', ms: 604_800_000 },
  { label: 'All', ms: 0 },
] as const

export default function DashboardPage() {
  const [timeFilter, setTimeFilter] = useState<number>(0)
  const [deleteTarget, setDeleteTarget] = useState<string | null>(null)

  const fromMs = timeFilter > 0 ? Date.now() - timeFilter : undefined

  const { data: infoData, isLoading: infoLoading, refetch } = useAllQueuesInfo(fromMs)
  const { data: listData, isLoading: listLoading } = useListQueues()

  const isLoading = infoLoading || listLoading

  const rows = useMemo(() => {
    const configMap = new Map(listData?.queues?.map((q) => [q.name, q.config]) ?? [])
    const statsMap = new Map(infoData?.queues?.map((q) => [q.queue, q]) ?? [])
    const allNames = new Set([...configMap.keys(), ...statsMap.keys()])
    return Array.from(allNames)
      .sort()
      .map((name) => ({ name, config: configMap.get(name), stats: statsMap.get(name) }))
  }, [listData, infoData])

  return (
    <div className="flex flex-col gap-6 p-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold">Dashboard</h1>
          <p className="text-sm text-muted-foreground">
            {rows.length} queue{rows.length !== 1 ? 's' : ''}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" onClick={() => refetch()}>
            <RefreshCw size={14} className="mr-1" />
            Refresh
          </Button>
          <Link href="/queues/new" className={buttonVariants({ size: 'sm' })}>
            <Plus size={14} className="mr-1" />
            New Queue
          </Link>
        </div>
      </div>

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

      <div className="rounded-lg border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Queue</TableHead>
              <TableHead>Runtime</TableHead>
              <TableHead className="text-center">Workers</TableHead>
              <TableHead className="text-center">Succeeded</TableHead>
              <TableHead className="text-center">Failed</TableHead>
              <TableHead className="text-center">Retries</TableHead>
              <TableHead className="text-right">Avg Duration</TableHead>
              <TableHead className="w-[80px]" />
            </TableRow>
          </TableHeader>
          <TableBody>
            {isLoading &&
              Array.from({ length: 3 }).map((_, i) => (
                <TableRow key={i}>
                  {Array.from({ length: 8 }).map((_, j) => (
                    <TableCell key={j}>
                      <Skeleton className="h-4 w-16" />
                    </TableCell>
                  ))}
                </TableRow>
              ))}

            {!isLoading && rows.length === 0 && (
              <TableRow>
                <TableCell colSpan={8} className="h-24 text-center text-muted-foreground">
                  No queues found.{' '}
                  <Link href="/queues/new" className="underline">
                    Create one
                  </Link>
                </TableCell>
              </TableRow>
            )}

            {!isLoading &&
              rows.map(({ name, config, stats }) => (
                <TableRow key={name}>
                  <TableCell className="font-medium">
                    <Link href={`/queues/${encodeURIComponent(name)}`} className="hover:underline">
                      {name}
                    </Link>
                  </TableCell>
                  <TableCell>
                    {config ? (
                      <Badge variant="outline" className="font-mono text-xs">
                        {config.runtime}
                      </Badge>
                    ) : (
                      <span className="text-muted-foreground">—</span>
                    )}
                  </TableCell>
                  <TableCell className="text-center">{config?.workers ?? '—'}</TableCell>
                  <TableCell className="text-center font-mono text-green-700 dark:text-green-400">
                    {stats?.succeeded ?? 0}
                  </TableCell>
                  <TableCell className="text-center font-mono text-red-700 dark:text-red-400">
                    {stats?.failed ?? 0}
                  </TableCell>
                  <TableCell className="text-center font-mono text-yellow-700 dark:text-yellow-400">
                    {stats?.retries ?? 0}
                  </TableCell>
                  <TableCell className="text-right font-mono text-xs text-muted-foreground">
                    {stats?.avg_duration_ms != null ? `${Math.round(stats.avg_duration_ms)}ms` : '—'}
                  </TableCell>
                  <TableCell>
                    <div className="flex items-center justify-end gap-1">
                      <Link
                        href={`/queues/${encodeURIComponent(name)}/edit`}
                        className={cn(buttonVariants({ variant: 'ghost', size: 'icon' }), 'h-7 w-7')}
                      >
                        <Pencil size={13} />
                      </Link>
                      <Button
                        variant="ghost"
                        size="icon"
                        className="h-7 w-7 text-destructive hover:text-destructive"
                        onClick={() => setDeleteTarget(name)}
                      >
                        <Trash2 size={13} />
                      </Button>
                    </div>
                  </TableCell>
                </TableRow>
              ))}
          </TableBody>
        </Table>
      </div>

      <DeleteQueueDialog
        name={deleteTarget}
        open={!!deleteTarget}
        onOpenChange={(open) => !open && setDeleteTarget(null)}
      />
    </div>
  )
}
