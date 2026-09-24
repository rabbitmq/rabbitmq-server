import type { AmqpTable, AmqpValue } from '../api/types/common'

export type ArgType = 'string' | 'number' | 'boolean' | 'list' | 'json'

export interface ArgRow {
  id: number
  key: string
  value: string
  type: ArgType
}

let nextId = 1
export const emptyArgRow = (): ArgRow => ({ id: nextId++, key: '', value: '', type: 'string' })

interface ArgumentsEditorProps {
  rows: ArgRow[]
  onChange: (rows: ArgRow[]) => void
  shortcuts?: { key: string; label: string; type: ArgType }[]
  testId?: string
}

/**
 * Key, value and type rows, the equivalent of the classic UI's multifield
 * inputs. A new empty row appears as soon as the last one is filled in.
 */
export function ArgumentsEditor({ rows, onChange, shortcuts, testId = 'arguments' }: ArgumentsEditorProps) {
  const last = rows.at(-1)
  const all = last === undefined || last.key !== '' || last.value !== '' ? [...rows, emptyArgRow()] : rows

  const set = (id: number, patch: Partial<ArgRow>) => onChange(all.map((row) => (row.id === id ? { ...row, ...patch } : row)))
  const remove = (id: number) => onChange(all.filter((row) => row.id !== id))

  const addShortcut = (shortcut: { key: string; type: ArgType }) => {
    if (all.some((row) => row.key === shortcut.key)) return
    const filled = all.filter((row) => row.key !== '' || row.value !== '')
    onChange([...filled, { ...emptyArgRow(), key: shortcut.key, value: shortcut.type === 'boolean' ? 'true' : '', type: shortcut.type }])
  }

  return (
    <div data-testid={testId}>
      {all.map((row, i) => (
        <div key={row.id} className="row args-row">
          <input
            type="text"
            aria-label="Argument name"
            value={row.key}
            placeholder="name"
            onChange={(event) => set(row.id, { key: event.target.value })}
            data-testid={`${testId}-key-${i}`}
          />
          <span>=</span>
          {row.type === 'boolean' ? (
            <select aria-label="Argument value" value={row.value} onChange={(event) => set(row.id, { value: event.target.value })}>
              <option value="true">true</option>
              <option value="false">false</option>
            </select>
          ) : (
            <input
              type="text"
              aria-label="Argument value"
              value={row.value}
              placeholder="value"
              onChange={(event) => set(row.id, { value: event.target.value })}
              data-testid={`${testId}-value-${i}`}
            />
          )}
          <select
            aria-label="Argument type"
            value={row.type}
            onChange={(event) => {
              const type = event.target.value as ArgType
              set(row.id, { type, value: type === 'boolean' ? 'true' : row.value })
            }}
          >
            <option value="string">String</option>
            <option value="number">Number</option>
            <option value="boolean">Boolean</option>
            <option value="list">List</option>
            <option value="json">JSON</option>
          </select>
          {i < all.length - 1 ? (
            <button type="button" className="btn btn-small" aria-label={`Remove ${row.key || 'argument'}`} onClick={() => remove(row.id)}>
              ×
            </button>
          ) : (
            <span className="btn btn-small" aria-hidden="true" style={{ visibility: 'hidden' }}>
              ×
            </span>
          )}
        </div>
      ))}
      {shortcuts && shortcuts.length > 0 ? (
        <div className="hint">
          Add{' '}
          {shortcuts.map((shortcut, i) => (
            <span key={shortcut.key}>
              {i > 0 ? ' | ' : null}
              <button type="button" className="btn-link" onClick={() => addShortcut(shortcut)} title={shortcut.key}>
                {shortcut.label}
              </button>
            </span>
          ))}
        </div>
      ) : null}
    </div>
  )
}

export function rowsToTable(rows: ArgRow[]): AmqpTable {
  const table: AmqpTable = {}
  for (const row of rows) {
    if (row.key === '') continue
    table[row.key] = parseValue(row)
  }
  return table
}

function parseValue(row: ArgRow): AmqpValue {
  switch (row.type) {
    case 'number': {
      const n = Number(row.value)
      if (row.value.trim() === '' || Number.isNaN(n)) throw new Error(`${row.key}: "${row.value}" is not a number`)
      return n
    }
    case 'boolean':
      return row.value === 'true'
    case 'list':
      return row.value
        .split(',')
        .map((item) => item.trim())
        .filter((item) => item !== '')
    case 'json':
      try {
        return JSON.parse(row.value) as AmqpValue
      } catch {
        throw new Error(`${row.key}: the value is not valid JSON`)
      }
    default:
      return row.value
  }
}

/** Only lists of strings can round-trip through the comma-separated list type. */
const isStringList = (value: AmqpValue): value is string[] => Array.isArray(value) && value.every((item) => typeof item === 'string')

export function tableToRows(table: AmqpTable | undefined): ArgRow[] {
  return Object.entries(table ?? {}).map(([key, value]): ArgRow => {
    const row = { ...emptyArgRow(), key }
    if (typeof value === 'number') return { ...row, value: String(value), type: 'number' }
    if (typeof value === 'boolean') return { ...row, value: String(value), type: 'boolean' }
    if (typeof value === 'string') return { ...row, value, type: 'string' }
    if (isStringList(value)) return { ...row, value: value.join(', '), type: 'list' }
    return { ...row, value: JSON.stringify(value), type: 'json' }
  })
}
