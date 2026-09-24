import { queryOptions } from '@tanstack/react-query'
import { apiFetch, seg } from '../client'
import type { Paginated } from '../types/common'
import type { AmqpSession, Channel, Connection } from '../types/connections'
import { listQueryParams, pageLocally, vhostScoped, type ListParams } from './list'
import { rangeParams, type ChartRange } from '../../charts/range'

export const connectionListQuery = (vhost: string, params: ListParams) =>
  queryOptions({
    queryKey: ['connections', 'list', vhost, params],
    queryFn: ({ signal }) =>
      apiFetch<Paginated<Connection> | Connection[]>(vhost === '' ? 'connections' : `vhosts/${seg(vhost)}/connections`, {
        signal,
        params: vhost === '' ? listQueryParams(params) : undefined,
      }).then((result) => (Array.isArray(result) ? pageLocally(result, params) : result)),
  })

export const connectionQuery = (name: string, range: ChartRange | undefined) =>
  queryOptions({
    queryKey: ['connections', 'detail', name, range ?? null],
    queryFn: ({ signal }) =>
      apiFetch<Connection>(`connections/${seg(name)}`, { signal, params: range ? rangeParams(['data_rates'], range) : undefined }),
  })

export const connectionChannelsQuery = (name: string) =>
  queryOptions({
    queryKey: ['channels', 'of-connection', name],
    queryFn: ({ signal }) => apiFetch<Channel[]>(`connections/${seg(name)}/channels`, { signal }),
  })

export const connectionSessionsQuery = (name: string) =>
  queryOptions({
    queryKey: ['connections', 'sessions', name],
    queryFn: ({ signal }) => apiFetch<AmqpSession[]>(`connections/${seg(name)}/sessions`, { signal }),
  })

export const closeConnection = (name: string, reason: string) =>
  apiFetch<void>(`connections/${seg(name)}`, { method: 'DELETE', headers: reason ? { 'X-Reason': reason } : {} })

export const channelListQuery = (vhost: string, params: ListParams) =>
  queryOptions({
    queryKey: ['channels', 'list', vhost, params],
    queryFn: ({ signal }) =>
      apiFetch<Paginated<Channel> | Channel[]>(vhost === '' ? 'channels' : vhostScoped('vhosts', vhost) + '/channels', {
        signal,
        params: vhost === '' ? listQueryParams(params) : undefined,
      }).then((result) => (Array.isArray(result) ? pageLocally(result, params) : result)),
  })

export const channelQuery = (name: string, range: ChartRange | undefined) =>
  queryOptions({
    queryKey: ['channels', 'detail', name, range ?? null],
    queryFn: ({ signal }) =>
      apiFetch<Channel>(`channels/${seg(name)}`, { signal, params: range ? rangeParams(['msg_rates', 'data_rates'], range) : undefined }),
  })
