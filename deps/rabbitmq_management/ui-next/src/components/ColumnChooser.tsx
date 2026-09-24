import { useEffect, useRef, useState } from 'react'
import { isColumnVisible, setColumnVisible, type ColumnSpec } from './columns'
import styles from './ColumnChooser.module.css'

export function ColumnChooser<T>({ mode, columns }: { mode: string; columns: ColumnSpec<T>[] }) {
  const [open, setOpen] = useState(false)
  const ref = useRef<HTMLDivElement>(null)
  const optional = columns.filter((column) => column.optional)

  useEffect(() => {
    if (!open) return
    const close = (event: MouseEvent) => {
      if (ref.current && !ref.current.contains(event.target as Node)) setOpen(false)
    }
    document.addEventListener('mousedown', close)
    return () => document.removeEventListener('mousedown', close)
  }, [open])

  if (optional.length === 0) return null
  const groups = [...new Set(optional.map((column) => column.group ?? 'Overview'))]

  return (
    <div className={styles.wrap} ref={ref}>
      <button
        type="button"
        className="btn btn-small"
        aria-expanded={open}
        onClick={() => setOpen(!open)}
        data-testid="column-chooser"
      >
        Columns
      </button>
      {open ? (
        <div className={styles.popover} role="dialog" aria-label="Columns">
          {groups.map((group) => (
            <fieldset key={group} className={styles.group}>
              <legend>{group}</legend>
              {optional
                .filter((column) => (column.group ?? 'Overview') === group)
                .map((column) => (
                  <label key={column.id} className={styles.option}>
                    <input
                      type="checkbox"
                      checked={isColumnVisible(mode, column as ColumnSpec<unknown>)}
                      onChange={(event) => setColumnVisible(mode, column.id, event.target.checked)}
                      data-testid={`column-${column.id}`}
                    />
                    {column.optional?.label}
                  </label>
                ))}
            </fieldset>
          ))}
        </div>
      ) : null}
    </div>
  )
}
