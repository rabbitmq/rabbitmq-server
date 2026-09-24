import { queryOptions } from '@tanstack/react-query'
import { apiFetch, seg } from '../client'
import type { AmqpTable, Paginated } from '../types/common'
import type { Binding, GetMessagesRequest, Queue, RetrievedMessage } from '../types/queues'
import type { ConsumerDetails } from '../types/queues'
import { listQueryParams, vhostScoped, type ListParams } from './list'
import { rangeParams, type ChartRange } from '../../charts/range'

export const queueListQuery = (vhost: string, params: ListParams) =>
  queryOptions({
    queryKey: ['queues', 'list', vhost, params],
    queryFn: ({ signal }) => apiFetch<Paginated<Queue>>(vhostScoped('queues', vhost), { signal, params: listQueryParams(params) }),
  })

export const queueQuery = (vhost: string, name: string, range: ChartRange | undefined) =>
  queryOptions({
    queryKey: ['queues', 'detail', vhost, name, range ?? null],
    queryFn: ({ signal }) =>
      apiFetch<Queue>(`queues/${seg(vhost)}/${seg(name)}`, {
        signal,
        params: range ? rangeParams(['lengths', 'msg_rates', 'data_rates'], range) : undefined,
      }),
  })

export const queueBindingsQuery = (vhost: string, name: string) =>
  queryOptions({
    queryKey: ['bindings', 'queue', vhost, name],
    queryFn: ({ signal }) => apiFetch<Binding[]>(`queues/${seg(vhost)}/${seg(name)}/bindings`, { signal }),
  })

export interface DeclareQueue {
  durable: boolean
  auto_delete: boolean
  arguments: AmqpTable
  node?: string
}

export const declareQueue = (vhost: string, name: string, body: DeclareQueue) =>
  apiFetch<void>(`queues/${seg(vhost)}/${seg(name)}`, { method: 'PUT', body })

export const deleteQueue = (vhost: string, name: string, opts: { ifEmpty?: boolean; ifUnused?: boolean } = {}) =>
  apiFetch<void>(`queues/${seg(vhost)}/${seg(name)}`, {
    method: 'DELETE',
    params: { 'if-empty': opts.ifEmpty || undefined, 'if-unused': opts.ifUnused || undefined },
  })

export const purgeQueue = (vhost: string, name: string) =>
  apiFetch<void>(`queues/${seg(vhost)}/${seg(name)}/contents`, { method: 'DELETE' })

export const getMessages = (vhost: string, name: string, request: GetMessagesRequest) =>
  apiFetch<RetrievedMessage[]>(`queues/${seg(vhost)}/${seg(name)}/get`, { method: 'POST', body: { ...request, vhost, name } })

export const consumersQuery = (vhost: string) =>
  queryOptions({
    queryKey: ['consumers', vhost],
    queryFn: ({ signal }) => apiFetch<ConsumerDetails[]>(vhostScoped('consumers', vhost), { signal }),
  })

export const addBinding = (
  vhost: string,
  source: string,
  destinationType: 'q' | 'e',
  destination: string,
  body: { routing_key: string; arguments: AmqpTable },
) => apiFetch<void>(`bindings/${seg(vhost)}/e/${seg(source)}/${destinationType}/${seg(destination)}`, { method: 'POST', body })

export const deleteBinding = (binding: Binding) =>
  apiFetch<void>(
    `bindings/${seg(binding.vhost)}/e/${seg(binding.source)}/${binding.destination_type === 'queue' ? 'q' : 'e'}/${seg(binding.destination)}/${seg(binding.properties_key)}`,
    { method: 'DELETE' },
  )
