import type { QueryParams } from '../client'
import type { Paginated } from '../types/common'

export const DEFAULT_PAGE_SIZE = 100
/** The server rejects larger pages, see `?MAX_PAGE_SIZE` in rabbit_mgmt_util. */
export const MAX_PAGE_SIZE = 500

/** The parameters that rabbit_mgmt_util understands for paginated list endpoints. */
export interface ListParams {
  page: number
  page_size: number
  name?: string
  use_regex?: boolean
  sort?: string
  sort_reverse?: boolean
  /** Comma-separated dotted field paths to project, so that responses stay small. */
  columns?: string
}

export function listQueryParams(params: ListParams): QueryParams {
  return {
    page: params.page,
    page_size: params.page_size,
    name: params.name,
    use_regex: params.name ? params.use_regex : undefined,
    sort: params.sort,
    sort_reverse: params.sort ? params.sort_reverse : undefined,
    columns: params.columns,
  }
}

/** `queues` or `queues/<vhost>`: list endpoints are scoped by an optional vhost segment. */
export function vhostScoped(resource: string, vhost: string): string {
  return vhost === '' ? resource : `${resource}/${encodeURIComponent(vhost)}`
}

/**
 * Filters, sorts and pages a complete list the way the server does for
 * paginated endpoints. This is only correct for a list that holds the whole
 * data set, such as the per-vhost connection and channel endpoints, which are
 * not paginated.
 */
export function pageLocally<T extends { name: string }>(result: T[], params: ListParams): Paginated<T> {
  let items = result
  if (params.name) {
    const needle = params.name.toLowerCase()
    let matches: (name: string) => boolean = (name) => name.toLowerCase().includes(needle)
    if (params.use_regex) {
      try {
        const re = new RegExp(params.name, 'i')
        matches = (name) => re.test(name)
      } catch {
        // An invalid expression falls back to a substring match, as in the classic UI.
      }
    }
    items = items.filter((item) => matches(item.name))
  }
  if (params.sort) {
    const path = params.sort.split('.')
    const value = (item: T) => path.reduce<unknown>((obj, key) => (obj as Record<string, unknown> | undefined)?.[key], item)
    items = [...items].sort((a, b) => compare(value(a), value(b)))
    if (params.sort_reverse) items.reverse()
  }
  const start = (params.page - 1) * params.page_size
  return {
    total_count: result.length,
    filtered_count: items.length,
    item_count: Math.min(params.page_size, Math.max(0, items.length - start)),
    page: params.page,
    page_size: params.page_size,
    page_count: Math.max(1, Math.ceil(items.length / params.page_size)),
    items: items.slice(start, start + params.page_size),
  }
}

function compare(a: unknown, b: unknown): number {
  if (a === b) return 0
  if (a === undefined || a === null) return -1
  if (b === undefined || b === null) return 1
  if (typeof a === 'number' && typeof b === 'number') return a - b
  return String(a).localeCompare(String(b))
}
