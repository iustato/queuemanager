'use client'

import { useForm, Controller } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { useState } from 'react'

import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Textarea } from '@/components/ui/textarea'
import { Separator } from '@/components/ui/separator'

import { queueConfigSchema, type QueueFormValues } from '@/lib/schemas/queue'
import type { QueueConfig } from '@/types/queue'

interface Props {
  defaultValues?: Partial<QueueConfig>
  defaultSchema?: string
  onSubmit: (values: QueueFormValues, schema: Record<string, unknown> | null) => Promise<void>
  submitLabel?: string
  nameReadonly?: boolean
}

export function QueueForm({ defaultValues, defaultSchema = '', onSubmit, submitLabel = 'Save', nameReadonly }: Props) {
  const [schemaJson, setSchemaJson] = useState(defaultSchema)
  const [schemaError, setSchemaError] = useState<string | null>(null)
  const [pending, setPending] = useState(false)

  // Converts empty/NaN inputs to undefined for optional number fields
  const optNum = {
    setValueAs: (v: unknown) =>
      v === '' || v == null || isNaN(Number(v)) ? undefined : Number(v),
  }

  const form = useForm<QueueFormValues>({
    resolver: zodResolver(queueConfigSchema),
    defaultValues: {
      name: '',
      workers: 2,
      runtime: 'exec',
      ...defaultValues,
    } as QueueFormValues,
  })

  const runtime = form.watch('runtime')

  function parseSchema(): Record<string, unknown> | null {
    const trimmed = schemaJson.trim()
    if (!trimmed) return null
    try {
      const parsed = JSON.parse(trimmed)
      setSchemaError(null)
      return parsed as Record<string, unknown>
    } catch (e) {
      setSchemaError((e as Error).message)
      return undefined as unknown as null // signal error
    }
  }

  async function handleSubmit(values: QueueFormValues) {
    const schema = parseSchema()
    if (schemaJson.trim() && schema === null) return // schema parse failed
    setPending(true)
    try {
      await onSubmit(values, schema)
    } finally {
      setPending(false)
    }
  }

  return (
    <form onSubmit={form.handleSubmit(handleSubmit)} className="space-y-6">
      {/* Core fields */}
      <div className="grid grid-cols-2 gap-4">
        <Field label="Queue name" error={form.formState.errors.name?.message}>
          <Input
            {...form.register('name')}
            placeholder="my-queue"
            readOnly={nameReadonly}
            className={nameReadonly ? 'bg-muted cursor-not-allowed' : ''}
          />
        </Field>

        <Field label="Workers" error={form.formState.errors.workers?.message}>
          <Input
            {...form.register('workers', { valueAsNumber: true })}
            type="number"
            min={1}
            max={100}
            placeholder="2"
          />
        </Field>
      </div>

      <Field label="Runtime" error={form.formState.errors.runtime?.message}>
        <Controller
          control={form.control}
          name="runtime"
          render={({ field }) => (
            <Select value={field.value} onValueChange={field.onChange}>
              <SelectTrigger>
                <SelectValue placeholder="Select runtime" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="exec">exec</SelectItem>
                <SelectItem value="php-cgi">php-cgi</SelectItem>
                <SelectItem value="php-fpm">php-fpm</SelectItem>
              </SelectContent>
            </Select>
          )}
        />
      </Field>

      {runtime === 'exec' && (
        <Field
          label="Command"
          description="Space-separated args, e.g. /usr/bin/php /var/www/worker.php"
          error={form.formState.errors.command?.message}
        >
          <Input
            placeholder="/usr/bin/php /var/www/worker.php"
            defaultValue={defaultValues?.command?.join(' ')}
            onChange={(e) => {
              const parts = e.target.value.split(/\s+/).filter(Boolean)
              form.setValue('command', parts)
            }}
          />
        </Field>
      )}

      {runtime === 'php-cgi' && (
        <Field label="Script path" error={form.formState.errors.script?.message}>
          <Input {...form.register('script')} placeholder="/var/www/worker.php" />
        </Field>
      )}

      {runtime === 'php-fpm' && (
        <div className="space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <Field label="FPM network" error={form.formState.errors.fpm_network?.message}>
              <Select
                defaultValue={defaultValues?.fpm_network ?? 'tcp'}
                onValueChange={(v) => form.setValue('fpm_network', v ?? undefined)}
              >
                <SelectTrigger>
                  <SelectValue placeholder="tcp" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="tcp">tcp</SelectItem>
                  <SelectItem value="unix">unix</SelectItem>
                </SelectContent>
              </Select>
            </Field>
            <Field label="FPM address" error={form.formState.errors.fpm_address?.message}>
              <Input {...form.register('fpm_address')} placeholder="127.0.0.1:9000" />
            </Field>
          </div>
          <Field label="Script path" error={form.formState.errors.script?.message}>
            <Input {...form.register('script')} placeholder="/var/www/worker.php" />
          </Field>
        </div>
      )}

      <Separator />

      {/* Advanced */}
      <details>
        <summary className="cursor-pointer text-sm font-medium text-muted-foreground hover:text-foreground">
          Advanced options
        </summary>
        <div className="mt-4 grid grid-cols-2 gap-4">
          <Field label="Timeout (sec)" error={form.formState.errors.timeout_sec?.message}>
            <Input {...form.register('timeout_sec', optNum)} type="number" min={1} placeholder="30" />
          </Field>
          <Field label="Max queue size" error={form.formState.errors.max_queue?.message}>
            <Input {...form.register('max_queue', optNum)} type="number" min={1} placeholder="1000" />
          </Field>
          <Field label="Max retries" error={form.formState.errors.max_retries?.message}>
            <Input {...form.register('max_retries', optNum)} type="number" min={0} placeholder="0" />
          </Field>
          <Field label="Retry delay (ms)" error={form.formState.errors.retry_delay_ms?.message}>
            <Input {...form.register('retry_delay_ms', optNum)} type="number" min={0} placeholder="1000" />
          </Field>
          <Field label="Result TTL" error={form.formState.errors.result_ttl?.message}>
            <Input {...form.register('result_ttl')} placeholder="10m" />
          </Field>
          <Field label="Message expiry" error={form.formState.errors.message_expiry?.message}>
            <Input {...form.register('message_expiry')} placeholder="1h" />
          </Field>
          <Field label="Log dir" error={form.formState.errors.log_dir?.message}>
            <Input {...form.register('log_dir')} placeholder="/var/log/queues/my-queue" />
          </Field>
        </div>
      </details>

      <Separator />

      {/* JSON Schema */}
      <div className="space-y-2">
        <Label>JSON Schema (optional)</Label>
        <p className="text-xs text-muted-foreground">
          If provided, incoming messages will be validated against this schema.
        </p>
        <Textarea
          value={schemaJson}
          onChange={(e) => {
            setSchemaJson(e.target.value)
            setSchemaError(null)
          }}
          placeholder={'{\n  "type": "object",\n  "properties": {}\n}'}
          rows={8}
          className="font-mono text-xs"
        />
        {schemaError && <p className="text-xs text-destructive">{schemaError}</p>}
      </div>

      <Button type="submit" disabled={pending}>
        {pending ? 'Saving…' : submitLabel}
      </Button>
    </form>
  )
}

function Field({
  label,
  description,
  error,
  children,
}: {
  label: string
  description?: string
  error?: string
  children: React.ReactNode
}) {
  return (
    <div className="space-y-1.5">
      <Label>{label}</Label>
      {description && <p className="text-xs text-muted-foreground">{description}</p>}
      {children}
      {error && <p className="text-xs text-destructive">{error}</p>}
    </div>
  )
}
