import { useMemo, type ReactNode } from 'react'
import {
  flexRender,
  getCoreRowModel,
  useReactTable,
  type ColumnDef,
  type SortingState,
} from '@tanstack/react-table'
import { usePrefsVersion } from '../prefs/storage'
import { isColumnVisible, type ColumnSpec } from './columns'

export interface SortState {
  sort?: string
  sort_reverse?: boolean
}

interface DataTableProps<T> {
  mode: string
  columns: ColumnSpec<T>[]
  rows: T[]
  rowKey: (row: T) => string
  sort?: SortState
  /** Sorting is done by the server: this only reports the requested key. */
  onSortChange?: (sort: SortState) => void
  empty?: ReactNode
}

/**
 * A table whose sorting and paging are done by the server. Sorting a fetched
 * page in the browser would sort that page rather than the data, so the table
 * never does it.
 */
export function DataTable<T>({ mode, columns, rows, rowKey, sort, onSortChange, empty }: DataTableProps<T>) {
  usePrefsVersion()
  const visible = columns.filter((column) => isColumnVisible(mode, column as ColumnSpec<unknown>))

  const defs = useMemo(() => toColumnDefs(visible), [visible])
  const sorting: SortingState = useMemo(() => {
    const column = visible.find((c) => c.sortKey !== undefined && c.sortKey === sort?.sort)
    return column ? [{ id: column.id, desc: sort?.sort_reverse ?? false }] : []
  }, [visible, sort])

  // TanStack Table returns functions that the React Compiler cannot memoize, so
  // it skips this component; nothing here relies on memoization for correctness.
  // eslint-disable-next-line react-hooks/incompatible-library
  const table = useReactTable({
    data: rows,
    columns: defs,
    state: { sorting },
    manualSorting: true,
    manualPagination: true,
    enableSortingRemoval: false,
    getRowId: (row) => rowKey(row),
    getCoreRowModel: getCoreRowModel(),
  })

  const bySpec = new Map(visible.map((column) => [column.id, column]))
  const hasGroups = visible.some((column) => column.group)

  const toggleSort = (spec: ColumnSpec<T>) => {
    if (!spec.sortKey || !onSortChange) return
    const reverse = sort?.sort === spec.sortKey ? !sort.sort_reverse : false
    onSortChange({ sort: spec.sortKey, sort_reverse: reverse })
  }

  return (
    <div className="table-wrap">
      <table className="list" data-testid={`${mode}-table`}>
        <thead>
          {table.getHeaderGroups().map((headerGroup, level) => {
            return (
              <tr key={headerGroup.id} className={level === 0 && hasGroups ? 'group' : undefined}>
                {headerGroup.headers.map((header) => {
                  const spec = bySpec.get(header.column.id)
                  if (header.isPlaceholder) return <th key={header.id} colSpan={header.colSpan} />
                  if (!spec) {
                    return (
                      <th key={header.id} colSpan={header.colSpan}>
                        {flexRender(header.column.columnDef.header, header.getContext())}
                      </th>
                    )
                  }
                  const sortable = spec.sortKey !== undefined && onSortChange !== undefined
                  const direction = header.column.getIsSorted()
                  return (
                    <th
                      key={header.id}
                      colSpan={header.colSpan}
                      className={spec.numeric ? 'num' : undefined}
                      aria-sort={direction === 'asc' ? 'ascending' : direction === 'desc' ? 'descending' : undefined}
                      data-testid={`${mode}-header-${spec.id}`}
                    >
                      {sortable ? (
                        <button type="button" className="sort-button" onClick={() => toggleSort(spec)}>
                          {flexRender(header.column.columnDef.header, header.getContext())}
                          {direction ? <span aria-hidden="true">{direction === 'desc' ? ' ▼' : ' ▲'}</span> : null}
                        </button>
                      ) : (
                        flexRender(header.column.columnDef.header, header.getContext())
                      )}
                    </th>
                  )
                })}
              </tr>
            )
          })}
        </thead>
        <tbody>
          {table.getRowModel().rows.length === 0 ? (
            <tr>
              <td colSpan={Math.max(1, visible.length)} className="empty">
                {empty ?? 'No items'}
              </td>
            </tr>
          ) : (
            table.getRowModel().rows.map((row) => (
              <tr key={row.id} data-testid={`${mode}-row`}>
                {row.getVisibleCells().map((cell) => (
                  <td key={cell.id} className={bySpec.get(cell.column.id)?.numeric ? 'num' : undefined}>
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </td>
                ))}
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  )
}

function toColumnDefs<T>(columns: ColumnSpec<T>[]): ColumnDef<T>[] {
  const leaf = (column: ColumnSpec<T>): ColumnDef<T> => ({
    id: column.id,
    header: () => column.header,
    cell: (context) => column.cell(context.row.original),
  })
  const hasGroups = columns.some((column) => column.group)
  if (!hasGroups) return columns.map(leaf)
  const defs: ColumnDef<T>[] = []
  let current: { group: string; columns: ColumnDef<T>[] } | undefined
  for (const column of columns) {
    const group = column.group ?? ''
    if (!current || current.group !== group) {
      current = { group, columns: [] }
      defs.push({ id: `group-${defs.length}-${group}`, header: () => group, columns: current.columns })
    }
    current.columns.push(leaf(column))
  }
  return defs
}
