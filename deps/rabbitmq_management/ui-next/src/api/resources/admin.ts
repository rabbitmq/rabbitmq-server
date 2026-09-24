import { queryOptions } from '@tanstack/react-query'
import { apiFetch, apiResponse, seg } from '../client'
import type { AmqpTable, Paginated } from '../types/common'
import type {
  DeprecatedFeature,
  FeatureFlag,
  Permission,
  Policy,
  PolicyApplyTo,
  TopicPermission,
  User,
  UserLimits,
  Vhost,
  VhostLimits,
} from '../types/admin'
import { listQueryParams, pageLocally, type ListParams } from './list'
import { rangeParams, type ChartRange } from '../../charts/range'

// Paginated requests to this endpoint fail on current brokers, because
// rabbit_queue_type:vhosts_with_dqt/1 is applied to the pagination envelope.
// The list is small, so it is fetched whole and paged here, as the classic UI does.
export const vhostListQuery = (params: ListParams) =>
  queryOptions({
    queryKey: ['vhosts', 'list', params],
    queryFn: ({ signal }) => apiFetch<Vhost[]>('vhosts', { signal }).then((vhosts) => pageLocally(vhosts, params)),
  })

export const vhostQuery = (name: string, range: ChartRange | undefined) =>
  queryOptions({
    queryKey: ['vhosts', 'detail', name, range ?? null],
    queryFn: ({ signal }) =>
      apiFetch<Vhost>(`vhosts/${seg(name)}`, {
        signal,
        params: range ? rangeParams(['lengths', 'msg_rates', 'data_rates'], range) : undefined,
      }),
  })

export interface DeclareVhost {
  description: string
  tags: string
  default_queue_type?: string
}

export const declareVhost = (name: string, body: DeclareVhost) => apiFetch<void>(`vhosts/${seg(name)}`, { method: 'PUT', body })
export const deleteVhost = (name: string) => apiFetch<void>(`vhosts/${seg(name)}`, { method: 'DELETE' })
export const restartVhost = (name: string, node: string) =>
  apiFetch<void>(`vhosts/${seg(name)}/start/${seg(node)}`, { method: 'POST' })

export const userListQuery = (params: ListParams) =>
  queryOptions({
    queryKey: ['users', 'list', params],
    queryFn: ({ signal }) => apiFetch<Paginated<User>>('users', { signal, params: listQueryParams(params) }),
  })

export const userNamesQuery = () =>
  queryOptions({
    queryKey: ['users', 'names'],
    queryFn: ({ signal }) => apiFetch<Pick<User, 'name'>[]>('users', { signal, params: { columns: 'name' } }),
  })

export const userQuery = (name: string) =>
  queryOptions({
    queryKey: ['users', 'detail', name],
    queryFn: ({ signal }) => apiFetch<User>(`users/${seg(name)}`, { signal }),
  })

export interface PutUser {
  tags: string
  password?: string
  /** An empty hash creates a user that cannot log in with a password. */
  password_hash?: string
  hashing_algorithm?: string
}

/** Returns true when the user was created, false when an existing user was updated. */
export async function putUser(name: string, body: PutUser): Promise<boolean> {
  const response = await apiResponse(`users/${seg(name)}`, { method: 'PUT', body })
  return response.status === 201
}

export const deleteUser = (name: string) => apiFetch<void>(`users/${seg(name)}`, { method: 'DELETE' })

export const allPermissionsQuery = () =>
  queryOptions({
    queryKey: ['permissions', 'all'],
    queryFn: ({ signal }) => apiFetch<Permission[]>('permissions', { signal }),
  })

export const permissionsQuery = (scope: { user: string } | { vhost: string }) =>
  queryOptions({
    queryKey: ['permissions', scope],
    queryFn: ({ signal }) =>
      apiFetch<Permission[]>('user' in scope ? `users/${seg(scope.user)}/permissions` : `vhosts/${seg(scope.vhost)}/permissions`, { signal }),
  })

export const topicPermissionsQuery = (scope: { user: string } | { vhost: string }) =>
  queryOptions({
    queryKey: ['topic-permissions', scope],
    queryFn: ({ signal }) =>
      apiFetch<TopicPermission[]>(
        'user' in scope ? `users/${seg(scope.user)}/topic-permissions` : `vhosts/${seg(scope.vhost)}/topic-permissions`,
        { signal },
      ),
  })

export const setPermission = (vhost: string, user: string, body: Pick<Permission, 'configure' | 'write' | 'read'>) =>
  apiFetch<void>(`permissions/${seg(vhost)}/${seg(user)}`, { method: 'PUT', body })

export const clearPermission = (vhost: string, user: string) =>
  apiFetch<void>(`permissions/${seg(vhost)}/${seg(user)}`, { method: 'DELETE' })

export const setTopicPermission = (vhost: string, user: string, body: Pick<TopicPermission, 'exchange' | 'write' | 'read'>) =>
  apiFetch<void>(`topic-permissions/${seg(vhost)}/${seg(user)}`, { method: 'PUT', body })

export const clearTopicPermission = (vhost: string, user: string, exchange: string) =>
  apiFetch<void>(`topic-permissions/${seg(vhost)}/${seg(user)}/${seg(exchange)}`, { method: 'DELETE' })

export type PolicyKind = 'policies' | 'operator-policies'

export const policiesQuery = (kind: PolicyKind, vhost: string) =>
  queryOptions({
    queryKey: [kind, vhost],
    queryFn: ({ signal }) => apiFetch<Policy[]>(vhost === '' ? kind : `${kind}/${seg(vhost)}`, { signal }),
  })

export const policyQuery = (kind: PolicyKind, vhost: string, name: string) =>
  queryOptions({
    queryKey: [kind, vhost, name],
    queryFn: ({ signal }) => apiFetch<Policy>(`${kind}/${seg(vhost)}/${seg(name)}`, { signal }),
  })

export interface PutPolicy {
  pattern: string
  'apply-to': PolicyApplyTo
  priority: number
  definition: AmqpTable
}

export const putPolicy = (kind: PolicyKind, vhost: string, name: string, body: PutPolicy) =>
  apiFetch<void>(`${kind}/${seg(vhost)}/${seg(name)}`, { method: 'PUT', body })

export const deletePolicy = (kind: PolicyKind, vhost: string, name: string) =>
  apiFetch<void>(`${kind}/${seg(vhost)}/${seg(name)}`, { method: 'DELETE' })

export const vhostLimitsQuery = () =>
  queryOptions({
    queryKey: ['vhost-limits'],
    queryFn: ({ signal }) => apiFetch<VhostLimits[]>('vhost-limits', { signal }),
  })

export const userLimitsQuery = (user?: string) =>
  queryOptions({
    queryKey: ['user-limits', user ?? null],
    queryFn: ({ signal }) => apiFetch<UserLimits[]>(user ? `user-limits/${seg(user)}` : 'user-limits', { signal }),
  })

export const setVhostLimit = (vhost: string, name: string, value: number) =>
  apiFetch<void>(`vhost-limits/${seg(vhost)}/${seg(name)}`, { method: 'PUT', body: { value } })
export const clearVhostLimit = (vhost: string, name: string) =>
  apiFetch<void>(`vhost-limits/${seg(vhost)}/${seg(name)}`, { method: 'DELETE' })
export const setUserLimit = (user: string, name: string, value: number) =>
  apiFetch<void>(`user-limits/${seg(user)}/${seg(name)}`, { method: 'PUT', body: { value } })
export const clearUserLimit = (user: string, name: string) =>
  apiFetch<void>(`user-limits/${seg(user)}/${seg(name)}`, { method: 'DELETE' })

export const featureFlagsQuery = () =>
  queryOptions({
    queryKey: ['feature-flags'],
    queryFn: ({ signal }) => apiFetch<FeatureFlag[]>('feature-flags', { signal }),
  })

export const enableFeatureFlag = (name: string) =>
  apiFetch<void>(`feature-flags/${seg(name)}/enable`, { method: 'PUT', body: {} })

export const deprecatedFeaturesQuery = (usedOnly: boolean) =>
  queryOptions({
    queryKey: ['deprecated-features', usedOnly],
    queryFn: ({ signal }) => apiFetch<DeprecatedFeature[]>(usedOnly ? 'deprecated-features/used' : 'deprecated-features', { signal }),
  })

export async function exportDefinitions(vhost: string): Promise<Blob> {
  const response = await apiResponse(vhost === '' ? 'definitions' : `definitions/${seg(vhost)}`)
  return response.blob()
}

export const importDefinitions = (vhost: string, file: File) => {
  const form = new FormData()
  form.append('file', file)
  return apiFetch<void>(vhost === '' ? 'definitions' : `definitions/${seg(vhost)}`, { method: 'POST', body: form })
}
