import type { AmqpTable, GarbageCollection, MessageStats, RateDetails } from './common'

export interface Connection {
  name: string
  node: string
  vhost: string
  user: string
  user_provided_name?: string
  container_id?: string
  protocol: string
  type?: string
  state?: string
  host?: string
  port?: number
  peer_host?: string
  peer_port?: number
  ssl: boolean
  ssl_protocol?: string | null
  ssl_key_exchange?: string | null
  ssl_cipher?: string | null
  ssl_hash?: string | null
  peer_cert_subject?: string | null
  peer_cert_issuer?: string | null
  peer_cert_validity?: string | null
  auth_mechanism?: string
  channels?: number
  channel_max?: number
  frame_max?: number
  timeout?: number
  connected_at?: number
  client_properties?: AmqpTable
  recv_oct?: number
  recv_oct_details?: RateDetails
  send_oct?: number
  send_oct_details?: RateDetails
  recv_cnt?: number
  send_cnt?: number
  send_pend?: number
  reductions?: number
  reductions_details?: RateDetails
  garbage_collection?: GarbageCollection
}

export interface ChannelSummary {
  name: string
  number: number
  connection_name: string
  peer_host: string
  peer_port: number | string
  user: string
  node: string
}

export interface Channel {
  name: string
  number: number
  node: string
  vhost: string
  user: string
  state?: string
  confirm?: boolean
  transactional?: boolean
  consumer_count?: number
  prefetch_count?: number
  global_prefetch_count?: number
  messages_unacknowledged?: number
  messages_unconfirmed?: number
  messages_uncommitted?: number
  acks_uncommitted?: number
  pending_raft_commands?: number
  cached_segments?: number
  idle_since?: string
  connection_details?: { name: string; peer_host: string; peer_port: number | string }
  message_stats?: MessageStats
  reductions?: number
  reductions_details?: RateDetails
  garbage_collection?: GarbageCollection
  consumer_details?: import('./queues').ConsumerDetails[]
  publishes?: { exchange: { name: string; vhost: string }; stats: MessageStats }[]
  deliveries?: { queue: { name: string; vhost: string }; stats: MessageStats }[]
}

export interface AmqpLink {
  handle: number
  link_name: string
  target_address?: string | null
  source_address?: string | null
  queue_name?: string | null
  snd_settle_mode?: string
  rcv_settle_mode?: string
  max_message_size?: number | string
  delivery_count?: number
  credit?: number
  unconfirmed_messages?: number
  send_settled?: boolean
}

export interface AmqpSession {
  channel_number: number
  handle_max: number
  next_incoming_id: number
  incoming_window: number
  next_outgoing_id: number
  remote_incoming_window: number
  remote_outgoing_window: number
  outgoing_unsettled_deliveries: number
  incoming_links: AmqpLink[]
  outgoing_links: AmqpLink[]
}
