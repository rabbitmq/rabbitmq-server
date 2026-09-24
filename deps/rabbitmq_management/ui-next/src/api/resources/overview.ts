import { queryOptions } from '@tanstack/react-query'
import { apiFetch, seg } from '../client'
import type { Overview } from '../types/overview'
import type { ClusterNode } from '../types/nodes'
import type { ClusterName, Extension, LoginResponse, Vhost, Whoami } from '../types/admin'
import { rangeParams, type ChartRange } from '../../charts/range'

export const whoamiQuery = () =>
  queryOptions({
    queryKey: ['whoami'],
    queryFn: ({ signal }) => apiFetch<Whoami>('whoami', { signal }),
    staleTime: Infinity,
  })

/** The overview as read once at start-up, for settings such as `disable_stats` and `rates_mode`. */
export const bootstrapOverviewQuery = () =>
  queryOptions({
    queryKey: ['overview', 'bootstrap'],
    queryFn: ({ signal }) => apiFetch<Overview>('overview', { signal }),
    staleTime: Infinity,
  })

export const overviewQuery = (range: ChartRange | undefined) =>
  queryOptions({
    queryKey: ['overview', 'charts', range ?? null],
    queryFn: ({ signal }) =>
      apiFetch<Overview>('overview', { signal, params: range ? rangeParams(['lengths', 'msg_rates'], range) : undefined }),
  })

export const vhostNamesQuery = () =>
  queryOptions({
    queryKey: ['vhosts', 'names'],
    queryFn: ({ signal }) => apiFetch<Vhost[]>('vhosts', { signal, params: { columns: 'name' } }),
  })

export const extensionsQuery = () =>
  queryOptions({
    queryKey: ['extensions'],
    queryFn: ({ signal }) => apiFetch<Extension[]>('extensions', { signal }),
    staleTime: Infinity,
  })

export const nodesQuery = () =>
  queryOptions({
    queryKey: ['nodes'],
    queryFn: ({ signal }) => apiFetch<ClusterNode[]>('nodes', { signal }),
  })

export const nodeQuery = (name: string, range: ChartRange | undefined, extra?: { memory?: boolean; binary?: boolean }) =>
  queryOptions({
    queryKey: ['nodes', name, range ?? null, extra ?? null],
    queryFn: ({ signal }) =>
      apiFetch<ClusterNode>(`nodes/${seg(name)}`, {
        signal,
        params: { ...(range ? rangeParams(['node_stats'], range) : {}), ...extra },
      }),
  })

export const clusterNameQuery = () =>
  queryOptions({
    queryKey: ['cluster-name'],
    queryFn: ({ signal }) => apiFetch<ClusterName>('cluster-name', { signal }),
  })

export const setClusterName = (name: string) => apiFetch<void>('cluster-name', { method: 'PUT', body: { name } })

export async function login(username: string, password: string): Promise<LoginResponse> {
  return apiFetch<LoginResponse>('login', {
    method: 'POST',
    body: new URLSearchParams({ username, password }),
    anonymous: true,
  })
}

export async function logout(): Promise<void> {
  await apiFetch<void>('login', { method: 'DELETE', anonymous: true }).catch(() => undefined)
}

export type HealthCheckStatus = { status: 'ok' } | { status: 'failed'; reason?: string; [key: string]: unknown }

export const healthCheckQuery = (check: string) =>
  queryOptions({
    queryKey: ['health', check],
    queryFn: async ({ signal }): Promise<HealthCheckStatus> => {
      try {
        return await apiFetch<HealthCheckStatus>(`health/checks/${check}`, { signal })
      } catch (err) {
        // A failed check is reported as 503 with a JSON body.
        const status = (err as { status?: number }).status
        if (status === 503) return { status: 'failed', reason: (err as Error).message }
        throw err
      }
    },
  })

export const resetStats = (node?: string) => apiFetch<void>(node ? `reset/${seg(node)}` : 'reset', { method: 'DELETE' })
