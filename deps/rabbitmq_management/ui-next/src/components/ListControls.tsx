import { useEffect, useState } from 'react'
import { ApiError } from '../api/errors'
import type { Paginated } from '../api/types/common'
import { MAX_PAGE_SIZE } from '../api/resources/list'
import { fmtNum } from '../format/numbers'
import type { ListState } from './listState'
import styles from './ListControls.module.css'

interface ListControlsProps {
  state: ListState
  page: Paginated<unknown> | undefined
  noun: string
  /** The list query's error; a page past the end sends the user back to page 1. */
  error?: unknown
  children?: React.ReactNode
}

/** Paging and name filtering controls for a server-paginated list, like `paginate_ui`. */
export function ListControls({ state, page, noun, error, children }: ListControlsProps) {
  const { search, params, regexError, update } = state
  const [filter, setFilter] = useState(search.name ?? '')
  // Follow the URL when it changes from outside the box, for example on back navigation.
  const [urlName, setUrlName] = useState(search.name)
  if (urlName !== search.name) {
    setUrlName(search.name)
    setFilter(search.name ?? '')
  }

  // A page past the end, for example after choosing another vhost, is rejected
  // by the server, or comes back empty from a list paged in the browser.
  const outOfRange =
    (error instanceof ApiError && error.error === 'page_out_of_range') || (page !== undefined && page.page > page.page_count)
  useEffect(() => {
    if (outOfRange) update({ page: 1 }, { replace: true })
  }, [outOfRange, update])

  // Typing in the filter replaces the history entry after a short pause.
  useEffect(() => {
    if (filter === (search.name ?? '')) return
    const timer = window.setTimeout(() => update({ name: filter, page: 1 }, { replace: true }), 300)
    return () => window.clearTimeout(timer)
  }, [filter, search.name, update])

  const pageCount = Math.max(1, page?.page_count ?? 1)
  const first = page && page.filtered_count > 0 ? (page.page - 1) * page.page_size + 1 : 0
  const last = page ? first + page.item_count - (page.item_count > 0 ? 1 : 0) : 0

  return (
    <div className={styles.bar} data-testid="list-controls">
      <label className={styles.field}>
        <span>Filter</span>
        <input
          type="search"
          value={filter}
          placeholder="Name"
          onChange={(event) => setFilter(event.target.value)}
          aria-invalid={regexError}
          data-testid="list-filter"
        />
      </label>
      <label className={styles.field}>
        <input
          type="checkbox"
          checked={search.use_regex ?? false}
          onChange={(event) => update({ use_regex: event.target.checked, page: 1 })}
          data-testid="list-regex"
        />
        <span>Regex</span>
      </label>
      {regexError ? <span className={styles.error}>Invalid expression: filtering as plain text</span> : null}

      <span className={styles.counts} data-testid="list-counts">
        {page ? (
          <>
            {page.filtered_count === 0 ? `No ${noun}` : `${fmtNum(first)}–${fmtNum(last)} of ${fmtNum(page.filtered_count)} ${noun}`}
            {page.filtered_count !== page.total_count ? ` (filtered from ${fmtNum(page.total_count)})` : null}
          </>
        ) : null}
      </span>

      <div className={styles.paging}>
        <button
          type="button"
          className="btn btn-small"
          disabled={params.page <= 1}
          onClick={() => update({ page: params.page - 1 })}
          aria-label="Previous page"
        >
          ‹
        </button>
        <label className={styles.field}>
          <span>Page</span>
          <select value={params.page} onChange={(event) => update({ page: Number(event.target.value) })} data-testid="list-page">
            {Array.from({ length: pageCount }, (_, i) => i + 1).map((n) => (
              <option key={n} value={n}>
                {n}
              </option>
            ))}
          </select>
          <span>of {pageCount}</span>
        </label>
        <button
          type="button"
          className="btn btn-small"
          disabled={params.page >= pageCount}
          onClick={() => update({ page: params.page + 1 })}
          aria-label="Next page"
        >
          ›
        </button>
        <label className={styles.field}>
          <span>Page size</span>
          <select
            value={params.page_size}
            onChange={(event) => update({ page_size: Number(event.target.value), page: 1 })}
            data-testid="list-page-size"
          >
            {[...new Set([25, 50, 100, 250, MAX_PAGE_SIZE, params.page_size])]
              .sort((a, b) => a - b)
              .map((n) => (
                <option key={n} value={n}>
                  {n}
                </option>
              ))}
          </select>
        </label>
        {children}
      </div>
    </div>
  )
}
