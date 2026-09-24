import type { Vhost } from '../../api/types/admin'
import type { ClusterNode } from '../../api/types/nodes'
import type { Overview } from '../../api/types/overview'

export interface AttentionItem {
  severity: 'bad' | 'warn'
  message: string
  link?: { to: string; params?: Record<string, string> }
}

/** Collects the warnings that the classic UI shows as banners on the overview and node pages. */
export function attentionItems(input: {
  overview: Overview | undefined
  nodes: ClusterNode[] | undefined
  vhosts: Pick<Vhost, 'name' | 'cluster_state'>[] | undefined
  alarmsCheck: { status: string; reason?: string } | undefined
  /** With `disable_stats`, nodes report no statistics at all, which is expected. */
  statsDisabled?: boolean
}): AttentionItem[] {
  const items: AttentionItem[] = []
  for (const node of input.nodes ?? []) {
    const link = { to: '/nodes/$name', params: { name: node.name } }
    if (!node.running) {
      items.push({ severity: 'bad', message: `Node ${node.name} is not running`, link })
      continue
    }
    if (node.mem_alarm) items.push({ severity: 'bad', message: `Memory alarm on ${node.name}: publishers are blocked`, link })
    if (node.disk_free_alarm) items.push({ severity: 'bad', message: `Disk alarm on ${node.name}: publishers are blocked`, link })
    if (node.partitions && node.partitions.length > 0) {
      items.push({ severity: 'bad', message: `Network partition: ${node.name} was partitioned from ${node.partitions.join(', ')}`, link })
    }
    if (node.os_pid === undefined && !input.statsDisabled) {
      items.push({ severity: 'warn', message: `Node statistics are not yet available for ${node.name}; if this persists, check that rabbitmq_management_agent is enabled`, link })
    }
    if (node.being_drained) items.push({ severity: 'warn', message: `${node.name} is in maintenance mode`, link })
  }

  const ticktimes = new Set((input.nodes ?? []).map((n) => n.net_ticktime).filter((t) => t !== undefined))
  if (ticktimes.size > 1) {
    items.push({ severity: 'bad', message: `Nodes have different net_ticktime values (${[...ticktimes].join(', ')}), which can lead to false partition detection` })
  }

  for (const vhost of input.vhosts ?? []) {
    for (const [node, state] of Object.entries(vhost.cluster_state ?? {})) {
      if (state !== 'running') {
        items.push({
          severity: 'warn',
          message: `Virtual host ${vhost.name} experienced an error on node ${node} and may be inaccessible`,
          link: { to: '/vhosts/$name', params: { name: vhost.name } },
        })
      }
    }
  }

  const backlog = input.overview?.statistics_db_event_queue ?? 0
  if (backlog > 1000) {
    items.push({
      severity: 'warn',
      message: `The management statistics database has a backlog of ${backlog} events; if it keeps growing, so will its memory use`,
    })
  }

  // Without node statistics, the alarms health check is the only source of alarms.
  if ((input.nodes === undefined || input.statsDisabled) && input.alarmsCheck?.status === 'failed') {
    items.push({ severity: 'bad', message: `Resource alarm in effect: ${input.alarmsCheck.reason ?? 'publishers are blocked'}` })
  }
  return items
}
