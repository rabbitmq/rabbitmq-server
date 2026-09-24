import type { AmqpTable, GarbageCollection, MessageStats, RateDetails } from './common'

export type QueueType = 'classic' | 'quorum' | 'stream' | string

export interface ConsumerDetails {
  arguments: AmqpTable
  ack_required: boolean
  active: boolean
  activity_status: string
  channel_details: {
    connection_name: string
    name: string
    node: string
    number: number
    peer_host: string
    peer_port: number | string
    user: string
  }
  consumer_tag: string
  consumer_timeout?: number
  exclusive: boolean
  prefetch_count: number
  queue: { name: string; vhost: string }
}

export interface QueueDeliveries {
  channel_details: ConsumerDetails['channel_details']
  stats: MessageStats
}

export interface QueueIncoming {
  exchange: { name: string; vhost: string }
  stats: MessageStats
}

export interface Queue {
  name: string
  vhost: string
  type: QueueType
  durable: boolean
  auto_delete: boolean
  exclusive?: boolean
  internal?: boolean
  internal_owner?: boolean | { name: string; vhost: string; kind: string }
  arguments: AmqpTable
  node?: string
  state?: string
  idle_since?: string
  policy?: string | null
  operator_policy?: string | null
  effective_policy_definition?: AmqpTable
  consumers?: number
  consumer_capacity?: number
  consumer_utilisation?: number | null
  owner_pid_details?: { name: string; peer_host?: string; peer_port?: number }
  publishers?: number
  delayed_retry?: string
  exclusive_consumer_tag?: string | null
  single_active_consumer_tag?: string | null
  memory?: number
  messages?: number
  messages_details?: RateDetails
  messages_ready?: number
  messages_ready_details?: RateDetails
  messages_unacknowledged?: number
  messages_unacknowledged_details?: RateDetails
  messages_ram?: number
  messages_persistent?: number
  messages_delayed?: number
  messages_dlx?: number
  message_bytes?: number
  message_bytes_ready?: number
  message_bytes_unacknowledged?: number
  message_bytes_ram?: number
  message_bytes_persistent?: number
  message_bytes_dlx?: number
  head_message_timestamp?: number | null
  message_stats?: MessageStats
  reductions?: number
  reductions_details?: RateDetails
  garbage_collection?: GarbageCollection
  consumer_details?: ConsumerDetails[]
  deliveries?: QueueDeliveries[]
  incoming?: QueueIncoming[]
  leader?: string
  members?: string[]
  online?: string[]
  delivery_limit?: number
  open_files?: Record<string, number>
  messages_by_priority?: Record<string, number>
  messages_ready_returned?: number
  next_delayed_at?: number
  last_delayed_at?: number
  segments?: number
  first_timestamp?: number
  readers?: Record<string, number>
  storage_version?: number
}

export interface GetMessagesRequest {
  count: number
  ackmode: 'ack_requeue_true' | 'reject_requeue_true' | 'ack_requeue_false' | 'reject_requeue_false'
  encoding: 'auto' | 'base64'
  truncate?: number
}

export interface RetrievedMessage {
  payload_bytes: number
  redelivered: boolean
  exchange: string
  routing_key: string
  message_count: number
  properties: AmqpTable & { headers?: AmqpTable }
  payload: string
  payload_encoding: 'string' | 'base64'
}

export interface Binding {
  source: string
  vhost: string
  destination: string
  destination_type: 'queue' | 'exchange'
  routing_key: string
  arguments: AmqpTable
  properties_key: string
}
