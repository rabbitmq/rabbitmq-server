import { fmtBytes, fmtPercent } from '../../format/numbers'
import styles from './MemoryBreakdown.module.css'

type Group = 'classic' | 'quorum' | 'stream' | 'conn' | 'table' | 'proc' | 'system' | 'binary' | 'unused'

/** Category labels and colour groups, from `MEMORY_STATISTICS` and `BINARY_STATISTICS` in the classic UI. */
const CATEGORIES: Record<string, [Group, string]> = {
  queue_procs: ['classic', 'Classic queues'],
  quorum_queue_procs: ['quorum', 'Quorum queues'],
  quorum_queue_dlx_procs: ['quorum', 'Dead letter workers'],
  stream_queue_procs: ['stream', 'Stream queues'],
  stream_queue_replica_reader_procs: ['stream', 'Stream queues (replica reader)'],
  stream_queue_coordinator_procs: ['stream', 'Stream queues (coordinator)'],
  binary: ['binary', 'Binaries'],
  connection_readers: ['conn', 'Connection readers'],
  connection_writers: ['conn', 'Connection writers'],
  connection_channels: ['conn', 'Connection channels'],
  connection_other: ['conn', 'Connections (other)'],
  mnesia: ['table', 'Mnesia'],
  msg_index: ['table', 'Message store index'],
  mgmt_db: ['table', 'Management database'],
  metrics: ['table', 'Metrics'],
  quorum_ets: ['table', 'Quorum queue ETS tables'],
  metadata_store_ets: ['table', 'Metadata store ETS tables'],
  other_ets: ['table', 'Other ETS tables'],
  metadata_store: ['proc', 'Metadata store'],
  plugins: ['proc', 'Plugins'],
  other_proc: ['proc', 'Other process memory'],
  code: ['system', 'Code'],
  atom: ['system', 'Atoms'],
  other_system: ['system', 'Other system'],
  other: ['system', 'Other binary references'],
  allocated_unused: ['unused', 'Allocated unused'],
  reserved_unallocated: ['unused', 'Unallocated reserved by the OS'],
}

interface BreakdownProps {
  values: Record<string, unknown>
  caption?: string
  testId?: string
}

export function MemoryBreakdown({ values, caption, testId }: BreakdownProps) {
  const entries = Object.entries(values)
    .filter((entry): entry is [string, number] => typeof entry[1] === 'number' && entry[1] > 0)
    .sort((a, b) => b[1] - a[1])
  const total = entries.reduce((sum, [, v]) => sum + v, 0)
  if (entries.length === 0) return <p className="muted">No data</p>
  return (
    <div className="stack" data-testid={testId}>
      {caption ? <div className="muted">{caption}</div> : null}
      <div className={styles.bar} role="img" aria-label="Memory use by category">
        {entries.map(([key, value]) => (
          <span
            key={key}
            className={`${styles.segment} ${styles[CATEGORIES[key]?.[0] ?? 'system']}`}
            style={{ width: `${(value / total) * 100}%` }}
            title={`${CATEGORIES[key]?.[1] ?? key}: ${fmtBytes(value)}`}
          />
        ))}
      </div>
      <div className="table-wrap">
        <table className="list">
          <thead>
            <tr>
              <th>Category</th>
              <th className="num">Size</th>
              <th className="num">Share</th>
            </tr>
          </thead>
          <tbody>
            {entries.map(([key, value]) => (
              <tr key={key}>
                <td>
                  <span className={`${styles.swatch} ${styles[CATEGORIES[key]?.[0] ?? 'system']}`} /> {CATEGORIES[key]?.[1] ?? key}
                </td>
                <td className="num">{fmtBytes(value)}</td>
                <td className="num">{fmtPercent(value / total)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
