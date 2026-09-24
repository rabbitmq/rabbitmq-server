import type { ArgType } from '../../components/ArgumentsEditor'

interface Shortcut {
  key: string
  label: string
  type: ArgType
}

export interface QueueTypeInfo {
  label: string
  /** Declaration fields that the type requires, as `QUEUE_TYPE[t].params` in the classic UI. */
  params: { durable?: boolean; auto_delete?: boolean }
  canGet: boolean
  canPurge: boolean
  argumentShortcuts: Shortcut[]
}

const s = (key: string, label: string, type: ArgType): Shortcut => ({ key, label, type })

const CLASSIC_ARGS = [
  s('x-expires', 'Auto expire', 'number'),
  s('x-message-ttl', 'Message TTL', 'number'),
  s('x-overflow', 'Overflow behaviour', 'string'),
  s('x-single-active-consumer', 'Single active consumer', 'boolean'),
  s('x-dead-letter-exchange', 'Dead letter exchange', 'string'),
  s('x-dead-letter-routing-key', 'Dead letter routing key', 'string'),
  s('x-max-length', 'Max length', 'number'),
  s('x-max-length-bytes', 'Max length bytes', 'number'),
  s('x-max-priority', 'Maximum priority', 'number'),
  s('x-queue-leader-locator', 'Leader locator', 'string'),
]

const QUORUM_ARGS = [
  s('x-expires', 'Auto expire', 'number'),
  s('x-message-ttl', 'Message TTL', 'number'),
  s('x-overflow', 'Overflow behaviour', 'string'),
  s('x-single-active-consumer', 'Single active consumer', 'boolean'),
  s('x-dead-letter-exchange', 'Dead letter exchange', 'string'),
  s('x-dead-letter-routing-key', 'Dead letter routing key', 'string'),
  s('x-max-length', 'Max length', 'number'),
  s('x-max-length-bytes', 'Max length bytes', 'number'),
  s('x-delivery-limit', 'Delivery limit', 'number'),
  s('x-quorum-initial-group-size', 'Initial cluster size', 'number'),
  s('x-quorum-target-group-size', 'Target cluster size', 'number'),
  s('x-dead-letter-strategy', 'Dead letter strategy', 'string'),
  s('x-queue-leader-locator', 'Leader locator', 'string'),
  s('x-consumer-disconnected-timeout', 'Consumer disconnected timeout', 'number'),
  s('x-delayed-retry-type', 'Delayed retry type', 'string'),
  s('x-delayed-retry-min', 'Delayed retry min', 'number'),
  s('x-delayed-retry-max', 'Delayed retry max', 'number'),
  s('x-member-placement-tag', 'Member placement tag', 'string'),
]

const STREAM_ARGS = [
  s('x-max-length-bytes', 'Max length bytes', 'number'),
  s('x-max-age', 'Max time retention', 'string'),
  s('x-stream-max-segment-size-bytes', 'Max segment size in bytes', 'number'),
  s('x-stream-filter-size-bytes', 'Filter size (per chunk) in bytes', 'number'),
  s('x-initial-cluster-size', 'Initial cluster size', 'number'),
  s('x-queue-leader-locator', 'Leader locator', 'string'),
]

/** The queue types that can be declared from the UI, from `QUEUE_TYPE` in the classic UI's global.js. */
export const QUEUE_TYPES: Record<string, QueueTypeInfo> = {
  classic: { label: 'Classic', params: {}, canGet: true, canPurge: true, argumentShortcuts: CLASSIC_ARGS },
  quorum: { label: 'Quorum', params: { durable: true, auto_delete: false }, canGet: true, canPurge: true, argumentShortcuts: QUORUM_ARGS },
  stream: { label: 'Stream', params: { durable: true, auto_delete: false }, canGet: false, canPurge: false, argumentShortcuts: STREAM_ARGS },
}

const MQTT_QOS0: QueueTypeInfo = { label: 'MQTT QoS0', params: {}, canGet: false, canPurge: false, argumentShortcuts: [] }

export function queueTypeInfo(type: string | undefined): QueueTypeInfo {
  if (type === 'rabbit_mqtt_qos0_queue') return MQTT_QOS0
  return QUEUE_TYPES[type ?? 'classic'] ?? QUEUE_TYPES.classic
}
