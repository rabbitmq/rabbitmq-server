import { queryOptions } from '@tanstack/react-query'
import { apiFetch, seg } from '../client'
import type { AmqpTable, Paginated } from '../types/common'
import type { Exchange, PublishRequest } from '../types/exchanges'
import type { Binding } from '../types/queues'
import { listQueryParams, vhostScoped, type ListParams } from './list'
import { rangeParams, type ChartRange } from '../../charts/range'

export const exchangeListQuery = (vhost: string, params: ListParams) =>
  queryOptions({
    queryKey: ['exchanges', 'list', vhost, params],
    queryFn: ({ signal }) =>
      apiFetch<Paginated<Exchange>>(vhostScoped('exchanges', vhost), { signal, params: listQueryParams(params) }),
  })

export const exchangeNamesQuery = (vhost: string) =>
  queryOptions({
    queryKey: ['exchanges', 'names', vhost],
    queryFn: ({ signal }) => apiFetch<Pick<Exchange, 'name' | 'type'>[]>(vhostScoped('exchanges', vhost), { signal, params: { columns: 'name,type' } }),
    staleTime: 30_000,
  })

export const exchangeQuery = (vhost: string, name: string, range: ChartRange | undefined) =>
  queryOptions({
    queryKey: ['exchanges', 'detail', vhost, name, range ?? null],
    queryFn: ({ signal }) =>
      apiFetch<Exchange>(`exchanges/${seg(vhost)}/${seg(name)}`, { signal, params: range ? rangeParams(['msg_rates'], range) : undefined }),
  })

export const exchangeBindingsQuery = (vhost: string, name: string, direction: 'source' | 'destination') =>
  queryOptions({
    queryKey: ['bindings', 'exchange', vhost, name, direction],
    queryFn: ({ signal }) => apiFetch<Binding[]>(`exchanges/${seg(vhost)}/${seg(name)}/bindings/${direction}`, { signal }),
  })

export interface DeclareExchange {
  type: string
  durable: boolean
  auto_delete: boolean
  internal: boolean
  arguments: AmqpTable
}

export const declareExchange = (vhost: string, name: string, body: DeclareExchange) =>
  apiFetch<void>(`exchanges/${seg(vhost)}/${seg(name)}`, { method: 'PUT', body })

export const deleteExchange = (vhost: string, name: string) =>
  apiFetch<void>(`exchanges/${seg(vhost)}/${seg(name)}`, { method: 'DELETE' })

export const publishMessage = (vhost: string, exchange: string, body: PublishRequest) =>
  apiFetch<{ routed: boolean }>(`exchanges/${seg(vhost)}/${seg(exchange === '' ? 'amq.default' : exchange)}/publish`, {
    method: 'POST',
    body: { ...body, vhost, name: exchange },
  })
