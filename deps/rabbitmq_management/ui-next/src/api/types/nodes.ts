import type { RateDetails } from './common'
import type { ExchangeType, WebContext } from './overview'

export interface Application {
  name: string
  description: string
  version: string
}

export interface ClusterLink {
  name: string
  peer_addr: string
  peer_port: number
  sock_addr: string
  sock_port: number
  recv_bytes: number
  recv_bytes_details?: RateDetails
  send_bytes: number
  send_bytes_details?: RateDetails
  stats?: {
    recv_bytes?: number
    recv_bytes_details?: RateDetails
    send_bytes?: number
    send_bytes_details?: RateDetails
  }
}

export interface RegistryEntry {
  name: string
  description: string
  enabled?: boolean
}

export interface NodeMemory {
  total?: { rss: number; allocated: number; erlang: number; strategy?: string }
  [category: string]: number | { rss: number; allocated: number; erlang: number; strategy?: string } | undefined
}

export interface ClusterNode {
  name: string
  type: string
  running: boolean
  being_drained?: boolean
  os_pid?: string
  uptime?: number
  run_queue?: number
  processors?: number
  rates_mode?: string
  partitions?: string[]
  mem_used?: number
  mem_used_details?: RateDetails
  mem_limit?: number
  mem_alarm?: boolean
  mem_calculation_strategy?: string
  disk_free?: number
  disk_free_details?: RateDetails
  disk_free_limit?: number
  disk_free_alarm?: boolean
  fd_used?: number
  fd_used_details?: RateDetails
  fd_total?: number
  sockets_used?: number
  sockets_total?: number
  proc_used?: number
  proc_used_details?: RateDetails
  proc_total?: number
  net_ticktime?: number
  rabbitmq_version?: string
  erlang_version?: string
  erlang_full_version?: string
  crypto_lib_version?: string
  enabled_plugins?: string[]
  applications?: Application[]
  exchange_types?: ExchangeType[]
  auth_mechanisms?: RegistryEntry[]
  contexts?: WebContext[]
  log_files?: string[]
  config_files?: string[]
  db_dir?: string
  cluster_links?: ClusterLink[]
  memory?: NodeMemory
  binary?: Record<string, number>
  // Rates of the node's persistence, I/O and churn counters, keyed by metric.
  [metric: `${string}_details`]: RateDetails | undefined
}
