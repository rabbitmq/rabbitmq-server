import { useState, type FormEvent } from 'react'
import { putPolicy, type PolicyKind } from '../../api/resources/admin'
import type { Policy, PolicyApplyTo } from '../../api/types/admin'
import { useAppData } from '../../app/context'
import { useNotify } from '../../app/notifications'
import { useSelectedVhost } from '../../app/vhost'
import { errorMessage } from '../../api/errors'
import { ArgumentsEditor, emptyArgRow, rowsToTable, tableToRows, type ArgRow, type ArgType } from '../../components/ArgumentsEditor'
import { useApiMutation } from '../../components/mutation'

type Shortcut = { key: string; label: string; type: ArgType }
const s = (key: string, label: string, type: ArgType): Shortcut => ({ key, label, type })

const USER_POLICY_SHORTCUTS: [string, Shortcut[]][] = [
  [
    'Queues [All types]',
    [
      s('max-length', 'Max length', 'number'),
      s('max-length-bytes', 'Max length bytes', 'number'),
      s('overflow', 'Overflow behaviour', 'string'),
      s('expires', 'Auto expire', 'number'),
      s('dead-letter-exchange', 'Dead letter exchange', 'string'),
      s('dead-letter-routing-key', 'Dead letter routing key', 'string'),
      s('message-ttl', 'Message TTL', 'number'),
      s('consumer-timeout', 'Consumer timeout', 'number'),
      s('queue-leader-locator', 'Leader locator', 'string'),
    ],
  ],
  [
    'Queues [Quorum]',
    [
      s('delivery-limit', 'Delivery limit', 'number'),
      s('dead-letter-strategy', 'Dead letter strategy', 'string'),
      s('consumer-disconnected-timeout', 'Consumer disconnected timeout', 'number'),
      s('delayed-retry-type', 'Delayed retry type', 'string'),
      s('delayed-retry-min', 'Delayed retry min', 'number'),
      s('delayed-retry-max', 'Delayed retry max', 'number'),
      s('member-placement-tag', 'Member placement tag', 'string'),
    ],
  ],
  ['Streams', [s('max-age', 'Max age', 'string'), s('stream-filter-size-bytes', 'Filter size in bytes', 'number')]],
  ['Exchanges', [s('alternate-exchange', 'Alternate exchange', 'string')]],
  ['Federation', [s('federation-upstream-set', 'Federation upstream set', 'string'), s('federation-upstream', 'Federation upstream', 'string')]],
]

const OPERATOR_POLICY_SHORTCUTS: [string, Shortcut[]][] = [
  [
    'Queues [Classic]',
    [
      s('expires', 'Auto expire', 'number'),
      s('max-length', 'Max length', 'number'),
      s('max-length-bytes', 'Max length bytes', 'number'),
      s('message-ttl', 'Message TTL', 'number'),
      s('overflow', 'Length limit overflow behaviour', 'string'),
    ],
  ],
  [
    'Queues [Quorum]',
    [
      s('delivery-limit', 'Delivery limit', 'number'),
      s('expires', 'Auto expire', 'number'),
      s('max-in-memory-bytes', 'Max in-memory bytes', 'number'),
      s('max-in-memory-length', 'Max in-memory length', 'number'),
      s('max-length', 'Max length', 'number'),
      s('max-length-bytes', 'Max length bytes', 'number'),
      s('message-ttl', 'Message TTL', 'number'),
      s('target-group-size', 'Target group size', 'number'),
      s('overflow', 'Length limit overflow behaviour', 'string'),
      s('member-placement-tag', 'Member placement tag', 'string'),
    ],
  ],
  ['Queues [Streams]', [s('max-length-bytes', 'Max length bytes', 'number')]],
]

const APPLY_TO: Record<PolicyKind, [PolicyApplyTo, string][]> = {
  policies: [
    ['all', 'Exchanges and queues'],
    ['exchanges', 'Exchanges'],
    ['queues', 'Queues'],
    ['classic_queues', 'Classic Queues'],
    ['quorum_queues', 'Quorum Queues'],
    ['streams', 'Stream Queues'],
  ],
  'operator-policies': [
    ['queues', 'Queues'],
    ['classic_queues', 'Classic Queues'],
    ['quorum_queues', 'Quorum Queues'],
    ['streams', 'Stream Queues'],
  ],
}

export function PolicyForm({ kind, initial, onSaved }: { kind: PolicyKind; initial?: Policy; onSaved?: () => void }) {
  const { vhosts } = useAppData()
  const notify = useNotify()
  const [selectedVhost] = useSelectedVhost()
  const [vhost, setVhost] = useState(initial?.vhost ?? (selectedVhost || vhosts[0]?.name || '/'))
  const [name, setName] = useState(initial?.name ?? '')
  const [pattern, setPattern] = useState(initial?.pattern ?? '')
  const [applyTo, setApplyTo] = useState<PolicyApplyTo>(initial?.['apply-to'] ?? APPLY_TO[kind][0][0])
  const [priority, setPriority] = useState(initial ? String(initial.priority) : '')
  const [definition, setDefinition] = useState<ArgRow[]>(tableToRows(initial?.definition))
  const put = useApiMutation<{ vhost: string; name: string; body: Parameters<typeof putPolicy>[3] }>({
    mutationFn: ({ vhost, name, body }) => putPolicy(kind, vhost, name, body),
    success: (_, v) => `Policy ${v.name} saved`,
    invalidate: [[kind], ['queues'], ['exchanges']],
    onSuccess: () => onSaved?.(),
  })

  const submit = (event: FormEvent) => {
    event.preventDefault()
    let table
    try {
      table = rowsToTable(definition)
    } catch (err) {
      notify('error', errorMessage(err))
      return
    }
    if (Object.keys(table).length === 0) return notify('error', 'A policy needs a definition.')
    const prio = priority.trim() === '' ? 0 : parseInt(priority, 10)
    if (Number.isNaN(prio)) return notify('error', 'Priority must be a number.')
    put.mutate({ vhost, name, body: { pattern, 'apply-to': applyTo, priority: prio, definition: table } })
  }

  const shortcuts = kind === 'policies' ? USER_POLICY_SHORTCUTS : OPERATOR_POLICY_SHORTCUTS
  return (
    <form className="form" onSubmit={submit} data-testid={`${kind}-form`}>
      {vhosts.length > 1 && !initial ? (
        <>
          <label htmlFor={`${kind}-vhost`}>Virtual host</label>
          <select id={`${kind}-vhost`} value={vhost} onChange={(e) => setVhost(e.target.value)}>
            {vhosts.map((v) => (
              <option key={v.name} value={v.name}>
                {v.name}
              </option>
            ))}
          </select>
        </>
      ) : null}
      <label htmlFor={`${kind}-name`}>Name</label>
      <input id={`${kind}-name`} type="text" required value={name} onChange={(e) => setName(e.target.value)} readOnly={initial !== undefined} data-testid="policy-name" />
      <label htmlFor={`${kind}-pattern`}>Pattern</label>
      <input id={`${kind}-pattern`} type="text" required value={pattern} onChange={(e) => setPattern(e.target.value)} data-testid="policy-pattern" />
      <label htmlFor={`${kind}-apply-to`}>Apply to</label>
      <select id={`${kind}-apply-to`} value={applyTo} onChange={(e) => setApplyTo(e.target.value as PolicyApplyTo)}>
        {APPLY_TO[kind].map(([value, label]) => (
          <option key={value} value={value}>
            {label}
          </option>
        ))}
      </select>
      <label htmlFor={`${kind}-priority`}>Priority</label>
      <input id={`${kind}-priority`} type="text" inputMode="numeric" value={priority} onChange={(e) => setPriority(e.target.value)} />
      <span className="label">Definition</span>
      <div>
        <ArgumentsEditor rows={definition} onChange={setDefinition} testId="policy-definition" />
        {shortcuts.map(([group, items]) => (
          <ArgumentsEditorShortcuts key={group} group={group} items={items} rows={definition} onChange={setDefinition} />
        ))}
      </div>
      <div className="actions">
        <button type="submit" className="btn btn-primary" disabled={put.isPending} data-testid="policy-submit">
          {initial ? 'Update policy' : 'Add / update policy'}
        </button>
      </div>
    </form>
  )
}

function ArgumentsEditorShortcuts({ group, items, rows, onChange }: { group: string; items: Shortcut[]; rows: ArgRow[]; onChange: (rows: ArgRow[]) => void }) {
  const add = (item: Shortcut) => {
    if (rows.some((row) => row.key === item.key)) return
    const filled = rows.filter((row) => row.key !== '' || row.value !== '')
    onChange([...filled, { ...emptyArgRow(), key: item.key, value: item.type === 'boolean' ? 'true' : '', type: item.type }])
  }
  return (
    <div className="hint">
      <strong>{group}:</strong>{' '}
      {items.map((item, i) => (
        <span key={item.key}>
          {i > 0 ? ' | ' : null}
          <button type="button" className="btn-link" onClick={() => add(item)} title={item.key}>
            {item.label}
          </button>
        </span>
      ))}
    </div>
  )
}
