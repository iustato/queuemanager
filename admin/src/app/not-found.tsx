import Link from 'next/link'

export default function NotFound() {
  return (
    <div className="flex flex-1 flex-col items-center justify-center gap-4 p-6">
      <h2 className="text-lg font-semibold">Page not found</h2>
      <p className="text-sm text-muted-foreground">The page you are looking for does not exist.</p>
      <Link
        href="/"
        className="inline-flex h-8 items-center rounded-lg border px-3 text-sm font-medium hover:bg-muted"
      >
        Back to Dashboard
      </Link>
    </div>
  )
}
