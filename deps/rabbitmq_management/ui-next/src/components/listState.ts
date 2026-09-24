import { useCallback, useMemo } from 'react'
import { useNavigate, useSearch } from '@tanstack/react-router'
import { DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE, type ListParams } from '../api/resources/list'
import { getPref, setPref } from '../prefs/storage'

/** The list state kept in the URL, so that a filtered, sorted page can be shared as a link. */
export interface ListSearch {
  page?: number
  page_size?: number
  name?: string
  use_regex?: boolean
  sort?: string
  sort_reverse?: boolean
}

const positiveInt = (value: unknown): number | undefined => {
  const n = typeof value === 'number' ? value : typeof value === 'string' ? parseInt(value, 10) : NaN
  return Number.isInteger(n) && n > 0 ? n : undefined
}

const bool = (value: unknown): boolean | undefined =>
  value === true || value === 'true' ? true : value === false || value === 'false' ? false : undefined

const text = (value: unknown): string | undefined => (typeof value === 'string' && value !== '' ? value : undefined)

export function validateListSearch(search: Record<string, unknown>): ListSearch {
  return {
    page: positiveInt(search.page),
    page_size: positiveInt(search.page_size),
    name: text(search.name),
    use_regex: bool(search.use_regex),
    sort: text(search.sort),
    sort_reverse: bool(search.sort_reverse),
  }
}

export function isValidRegex(pattern: string): boolean {
  try {
    new RegExp(pattern)
    return true
  } catch {
    return false
  }
}

export interface ListState {
  search: ListSearch
  params: ListParams
  regexError: boolean
  update: (patch: ListSearch, options?: { replace?: boolean }) => void
}

/**
 * Reads a list's page, filter and sort from the URL. The page size is also kept
 * in the classic UI's `<context>_current_page_size` pref, so that it survives
 * navigation.
 */
export function useListState(context: string, defaultSort?: { sort: string; sort_reverse?: boolean }): ListState {
  const search = useSearch({ strict: false }) as ListSearch
  const navigate = useNavigate()

  const storedSize = positiveInt(getPref(`${context}_current_page_size`))
  const pageSize = Math.min(search.page_size ?? storedSize ?? DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE)
  const regexError = search.use_regex === true && search.name !== undefined && !isValidRegex(search.name)

  const params = useMemo<ListParams>(
    () => ({
      page: search.page ?? 1,
      page_size: pageSize,
      name: search.name,
      // An invalid expression is sent as a plain substring filter, as the classic UI does.
      use_regex: search.use_regex && !regexError,
      sort: search.sort ?? defaultSort?.sort,
      sort_reverse: search.sort !== undefined ? search.sort_reverse : defaultSort?.sort_reverse,
    }),
    [search, pageSize, regexError, defaultSort?.sort, defaultSort?.sort_reverse],
  )

  const update = useCallback(
    (patch: ListSearch, options: { replace?: boolean } = {}) => {
      if (patch.page_size !== undefined) setPref(`${context}_current_page_size`, String(patch.page_size))
      void navigate({
        to: '.',
        search: ((prev: ListSearch) => clean({ ...prev, ...patch })) as never,
        replace: options.replace,
      })
    },
    [context, navigate],
  )

  return { search, params, regexError, update }
}

function clean(search: ListSearch): ListSearch {
  const result: ListSearch = {}
  for (const [key, value] of Object.entries(search) as [keyof ListSearch, unknown][]) {
    if (value !== undefined && value !== '' && !(key === 'page' && value === 1) && !(key === 'use_regex' && value === false)) {
      ;(result as Record<string, unknown>)[key] = value
    }
  }
  return result
}
