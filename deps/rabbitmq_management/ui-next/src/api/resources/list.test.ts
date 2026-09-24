import { describe, expect, it } from 'vitest'
import { listQueryParams, pageLocally, vhostScoped } from './list'
import { validateListSearch } from '../../components/listState'

describe('list parameters', () => {
  it('only sends the regex flag with a filter, and the direction with a sort key', () => {
    expect(listQueryParams({ page: 2, page_size: 50, use_regex: true, sort_reverse: true })).toEqual({
      page: 2,
      page_size: 50,
      name: undefined,
      use_regex: undefined,
      sort: undefined,
      sort_reverse: undefined,
      columns: undefined,
    })
  })

  it('scopes endpoints by an encoded vhost', () => {
    expect(vhostScoped('queues', '')).toBe('queues')
    expect(vhostScoped('queues', '/')).toBe('queues/%2F')
  })

  it('validates URL search parameters', () => {
    expect(validateListSearch({ page: '3', page_size: '-1', name: '', use_regex: 'true', sort: 'messages', sort_reverse: 'false' })).toEqual({
      page: 3,
      page_size: undefined,
      name: undefined,
      use_regex: true,
      sort: 'messages',
      sort_reverse: false,
    })
  })
})

describe('pageLocally', () => {
  const items = [
    { name: 'b', messages: 3 },
    { name: 'a', messages: 1 },
    { name: 'c', messages: 2 },
    { name: 'ab', messages: 5 },
  ]

  it('filters, sorts and pages the whole list', () => {
    const page = pageLocally(items, { page: 1, page_size: 2, name: 'b', sort: 'messages', sort_reverse: true })
    expect(page.items.map((i) => i.name)).toEqual(['ab', 'b'])
    expect(page).toMatchObject({ total_count: 4, filtered_count: 2, page_count: 1, item_count: 2 })
  })

  it('supports regular expressions and falls back to a substring on invalid ones', () => {
    expect(pageLocally(items, { page: 1, page_size: 10, name: '^a', use_regex: true }).items.map((i) => i.name)).toEqual(['a', 'ab'])
    expect(pageLocally(items, { page: 1, page_size: 10, name: '(', use_regex: true }).filtered_count).toBe(0)
  })

  it('reports later pages', () => {
    const page = pageLocally(items, { page: 2, page_size: 3 })
    expect(page).toMatchObject({ page: 2, item_count: 1, page_count: 2 })
  })
})
