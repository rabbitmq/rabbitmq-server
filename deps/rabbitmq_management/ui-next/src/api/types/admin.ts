import type { AmqpTable, MessageStats, RateDetails } from './common'

export interface Whoami {
  name: string
  tags: string[]
  is_internal_user: boolean
  login_session_timeout?: number
}

export interface LoginResponse {
  token: { type: 'basic' | 'bearer'; value: string }
  user: Whoami
}

export interface Vhost {
  name: string
  description?: string
  tags?: string[]
  default_queue_type?: string
  protected_from_deletion?: boolean
  tracing?: boolean
  metadata?: { description?: string; tags?: string[]; default_queue_type?: string }
  cluster_state?: Record<string, string>
  messages?: number
  messages_details?: RateDetails
  messages_ready?: number
  messages_ready_details?: RateDetails
  messages_unacknowledged?: number
  messages_unacknowledged_details?: RateDetails
  recv_oct?: number
  recv_oct_details?: RateDetails
  send_oct?: number
  send_oct_details?: RateDetails
  message_stats?: MessageStats
}

export interface User {
  name: string
  tags: string[]
  password_hash?: string
  hashing_algorithm?: string
  limits?: Record<string, number>
}

export interface Permission {
  user: string
  vhost: string
  configure: string
  write: string
  read: string
}

export interface TopicPermission {
  user: string
  vhost: string
  exchange: string
  write: string
  read: string
}

export type PolicyApplyTo = 'all' | 'queues' | 'classic_queues' | 'quorum_queues' | 'streams' | 'exchanges'

export interface Policy {
  vhost: string
  name: string
  pattern: string
  'apply-to': PolicyApplyTo
  definition: AmqpTable
  priority: number
}

export interface VhostLimits {
  vhost: string
  value: Record<string, number>
}

export interface UserLimits {
  user: string
  value: Record<string, number>
}

export interface FeatureFlag {
  name: string
  desc: string
  doc_url: string
  state: 'enabled' | 'disabled' | 'state_changing' | 'unavailable'
  stability: 'required' | 'stable' | 'experimental'
  provided_by: string
  experiment_level?: string
}

export interface DeprecatedFeature {
  name: string
  desc: string
  doc_url: string
  state: string
  deprecation_phase: 'permitted_by_default' | 'denied_by_default' | 'disconnected' | 'removed' | string
  provided_by: string
}

export interface ClusterName {
  name: string
}

export type Extension = { javascript?: string | string[]; css?: string | string[] } | []
