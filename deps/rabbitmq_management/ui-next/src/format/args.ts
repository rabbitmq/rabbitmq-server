import type { AmqpTable, AmqpValue } from '../api/types/common'

interface ArgInfo {
  short: string
  type: 'string' | 'int' | 'boolean'
}

/** Arguments that the UI presents specially, from `KNOWN_ARGS` in the classic UI. */
export const KNOWN_ARGS: Record<string, ArgInfo> = {
  'alternate-exchange': { short: 'AE', type: 'string' },
  'x-message-ttl': { short: 'TTL', type: 'int' },
  'x-expires': { short: 'Exp', type: 'int' },
  'x-max-length': { short: 'Lim', type: 'int' },
  'x-max-length-bytes': { short: 'Lim B', type: 'int' },
  'x-delivery-limit': { short: 'DlL', type: 'int' },
  'x-overflow': { short: 'Ovfl', type: 'string' },
  'x-dead-letter-exchange': { short: 'DLX', type: 'string' },
  'x-dead-letter-routing-key': { short: 'DLK', type: 'string' },
  'x-queue-master-locator': { short: 'ML', type: 'string' },
  'x-queue-leader-locator': { short: 'LL', type: 'string' },
  'x-max-priority': { short: 'Pri', type: 'int' },
  'x-single-active-consumer': { short: 'SAC', type: 'boolean' },
  'x-consumer-disconnected-timeout': { short: 'CDT', type: 'int' },
  'x-delayed-retry-type': { short: 'DRT', type: 'string' },
  'x-delayed-retry-min': { short: 'DRm', type: 'int' },
  'x-delayed-retry-max': { short: 'DRM', type: 'int' },
  'x-member-placement-tag': { short: 'MPT', type: 'string' },
}

/** Properties that are listed the same way as arguments. */
const IMPLICIT_ARGS: Record<string, ArgInfo> = {
  durable: { short: 'D', type: 'boolean' },
  'auto-delete': { short: 'AD', type: 'boolean' },
  internal: { short: 'I', type: 'boolean' },
  exclusive: { short: 'Excl', type: 'boolean' },
  'delayed retry': { short: 'DR', type: 'string' },
}

const ALL_ARGS: Record<string, ArgInfo> = { ...IMPLICIT_ARGS, ...KNOWN_ARGS }

export interface FeatureTag {
  short: string
  title: string
}

interface WithArguments {
  arguments?: AmqpTable
  durable?: boolean
  auto_delete?: boolean
  exclusive?: boolean
  internal?: boolean
  delayed_retry?: string
}

/** The short feature tags shown in queue and exchange lists, like `fmt_features_short`. */
export function featureTags(obj: WithArguments): FeatureTag[] {
  const known: Record<string, AmqpValue> = {}
  const other: AmqpTable = {}
  for (const [key, value] of Object.entries(obj.arguments ?? {})) {
    if (key in KNOWN_ARGS) {
      // Single active consumer is the only boolean argument, and is only worth showing when set.
      if (value !== false) known[key] = value
    } else {
      other[key] = value
    }
  }
  if (obj.durable) known.durable = true
  if (obj.auto_delete) known['auto-delete'] = true
  if (obj.exclusive) known.exclusive = true
  if (obj.internal) known.internal = true
  if (obj.delayed_retry !== undefined) known['delayed retry'] = obj.delayed_retry === 'disabled' ? 'not enabled' : obj.delayed_retry

  const tags: FeatureTag[] = []
  for (const [key, info] of Object.entries(ALL_ARGS)) {
    if (key in known) tags.push({ short: info.short, title: `${key}: ${fmtAmqpValue(known[key])}` })
  }
  if (Object.keys(other).length > 0) tags.push({ short: 'Args', title: fmtTableFlat(other) })
  return tags
}

export function fmtAmqpValue(value: AmqpValue | undefined): string {
  if (value === undefined) return ''
  if (value === null) return 'null'
  if (Array.isArray(value)) return `[${value.map(fmtAmqpValue).join(',')}]`
  if (typeof value === 'object') return `(${fmtTableFlat(value)})`
  return String(value)
}

export function fmtTableFlat(table: AmqpTable): string {
  return Object.entries(table)
    .map(([key, value]) => `${key}: ${fmtAmqpValue(value)}`)
    .join(', ')
}
