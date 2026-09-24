import type { ReactNode } from 'react'
import { getPref, setPref } from '../prefs/storage'

export interface ColumnSpec<T> {
  /** Stable identifier; for optional columns it matches the classic UI's column key. */
  id: string
  header: ReactNode
  group?: string
  cell: (row: T) => ReactNode
  /** The server-side sort key, a dotted path such as `message_stats.publish_details.rate`. */
  sortKey?: string
  numeric?: boolean
  /** The fields the cell reads, requested through the `columns` query parameter. */
  fields?: string[]
  optional?: { label: string; defaultVisible: boolean }
}

/** Column visibility, stored under the classic UI's `column-<mode>-<key>` prefs. */
export function isColumnVisible(mode: string, column: ColumnSpec<unknown>): boolean {
  if (!column.optional) return true
  const pref = getPref(`column-${mode}-${column.id}`)
  return pref === null ? column.optional.defaultVisible : pref === 'true'
}

export function setColumnVisible(mode: string, id: string, visible: boolean) {
  setPref(`column-${mode}-${id}`, String(visible))
}

export function columnsParam<T>(columns: ColumnSpec<T>[], always: string[]): string {
  const fields = new Set(always)
  for (const column of columns) for (const field of column.fields ?? []) fields.add(field)
  return [...fields].join(',')
}
