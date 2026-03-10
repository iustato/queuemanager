'use client'

import Link from 'next/link'
import { usePathname } from 'next/navigation'
import { LayoutDashboard, List, Plus } from 'lucide-react'
import { cn } from '@/lib/utils'
import { useListQueues } from '@/lib/hooks/use-queues'
import { buttonVariants } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'

export function Sidebar() {
  const pathname = usePathname()
  const { data, isLoading } = useListQueues()

  return (
    <aside className="flex h-screen w-60 shrink-0 flex-col border-r bg-background">
      {/* Logo */}
      <div className="flex h-14 items-center border-b px-4">
        <Link href="/" className="flex items-center gap-2 font-semibold tracking-tight">
          <span className="rounded bg-primary px-1.5 py-0.5 text-xs text-primary-foreground">Q</span>
          Queue Admin
        </Link>
      </div>

      {/* Nav */}
      <nav className="flex flex-1 flex-col gap-1 overflow-y-auto p-2">
        <NavItem href="/" icon={<LayoutDashboard size={15} />} label="Dashboard" active={pathname === '/'} />

        <div className="mt-4 px-2">
          <div className="mb-1 flex items-center justify-between">
            <span className="text-[11px] font-medium uppercase tracking-wider text-muted-foreground">Queues</span>
            <Link href="/queues/new">
              <Plus size={14} className="text-muted-foreground hover:text-foreground" />
            </Link>
          </div>

          {isLoading && (
            <div className="space-y-1">
              {[1, 2, 3].map((i) => (
                <Skeleton key={i} className="h-7 w-full" />
              ))}
            </div>
          )}

          {data?.queues?.map((q) => (
            <NavItem
              key={q.name}
              href={`/queues/${encodeURIComponent(q.name)}`}
              icon={<List size={14} />}
              label={q.name}
              active={pathname.startsWith(`/queues/${encodeURIComponent(q.name)}`)}
            />
          ))}

          {!isLoading && !data?.queues?.length && (
            <p className="px-2 py-1 text-xs text-muted-foreground">No queues yet</p>
          )}
        </div>
      </nav>

      {/* Footer */}
      <div className="border-t p-2">
        <Link href="/queues/new" className={cn(buttonVariants({ size: 'sm' }), 'w-full')}>
          <Plus size={14} className="mr-1" />
          New Queue
        </Link>
      </div>
    </aside>
  )
}

function NavItem({
  href,
  icon,
  label,
  active,
}: {
  href: string
  icon: React.ReactNode
  label: string
  active: boolean
}) {
  return (
    <Link
      href={href}
      className={cn(
        'flex items-center gap-2 rounded-md px-2 py-1.5 text-sm transition-colors',
        active
          ? 'bg-accent text-accent-foreground font-medium'
          : 'text-muted-foreground hover:bg-accent hover:text-accent-foreground',
      )}
    >
      {icon}
      <span className="truncate">{label}</span>
    </Link>
  )
}
