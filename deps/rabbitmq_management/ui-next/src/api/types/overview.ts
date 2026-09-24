import type { MessageStats, RateDetails, RatesMode } from './common'

export interface ExchangeType {
  name: string
  description: string
  enabled: boolean
}

export interface Listener {
  node: string
  protocol: string
  ip_address: string
  port: number
  tls: boolean
}

export interface WebContext {
  node: string
  description: string
  path: string
  port: string | number
  ip?: string
  tls?: boolean
  protocol?: string
}

export interface QueueTotals {
  messages?: number
  messages_details?: RateDetails
  messages_ready?: number
  messages_ready_details?: RateDetails
  messages_unacknowledged?: number
  messages_unacknowledged_details?: RateDetails
}

export interface ChurnRates {
  connection_created?: number
  connection_created_details?: RateDetails
  connection_closed?: number
  connection_closed_details?: RateDetails
  channel_created?: number
  channel_created_details?: RateDetails
  channel_closed?: number
  channel_closed_details?: RateDetails
  queue_declared?: number
  queue_declared_details?: RateDetails
  queue_created?: number
  queue_created_details?: RateDetails
  queue_deleted?: number
  queue_deleted_details?: RateDetails
}

export interface ObjectTotals {
  connections: number
  channels: number
  exchanges: number
  queues: number
  consumers: number
}

export interface Overview {
  management_version: string
  rates_mode: RatesMode
  sample_retention_policies: Record<'global' | 'basic' | 'detailed', number[]>
  exchange_types: ExchangeType[]
  product_version?: string
  product_name?: string
  rabbitmq_version: string
  cluster_name: string
  cluster_tags?: Record<string, string>
  node_tags?: Record<string, string>
  erlang_version: string
  erlang_full_version: string
  crypto_lib_version?: string
  disable_stats: boolean
  default_queue_type?: string
  is_op_policy_updating_enabled: boolean
  enable_queue_totals: boolean
  require_definition_json_extension?: boolean
  message_stats?: MessageStats
  churn_rates?: ChurnRates
  queue_totals?: QueueTotals
  object_totals?: ObjectTotals
  statistics_db_event_queue?: number
  /** Present only for users with the monitoring tag. */
  node?: string
  listeners?: Listener[]
  contexts?: WebContext[]
}
