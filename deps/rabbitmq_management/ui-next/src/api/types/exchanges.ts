import type { AmqpTable, MessageStats } from './common'

export interface ExchangePublishStats {
  channel_details?: { name: string; connection_name: string; number: number; peer_host: string; peer_port: number | string; user: string; node: string }
  exchange?: { name: string; vhost: string }
  stats: MessageStats
}

export interface Exchange {
  name: string
  vhost: string
  type: string
  durable: boolean
  auto_delete: boolean
  internal: boolean
  arguments: AmqpTable
  policy?: string | null
  user_who_performed_action?: string
  message_stats?: MessageStats
  incoming?: ExchangePublishStats[]
  outgoing?: ExchangePublishStats[]
}

export interface PublishRequest {
  routing_key: string
  payload: string
  payload_encoding: 'string' | 'base64'
  properties: AmqpTable
}
