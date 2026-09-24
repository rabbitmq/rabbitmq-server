import type { ReactNode } from 'react'
import type { AmqpTable, AmqpValue, RateDetails } from '../api/types/common'
import { featureTags } from '../format/args'
import { fmtBytes, fmtNum, fmtRate, fmtRateBytes, fmtRateNum, UNKNOWN } from '../format/numbers'
import { fmtTimestamp } from '../format/time'

type Num = number | null | undefined

export function Unknown() {
  return <span className="unknown">{UNKNOWN}</span>
}

export function Num({ value }: { value: Num }) {
  if (value === undefined || value === null) return <Unknown />
  return <span className="num">{fmtNum(value)}</span>
}

export function Bytes({ value }: { value: Num }) {
  if (value === undefined || value === null) return <Unknown />
  return <span className="num">{fmtBytes(value)}</span>
}

/** A rate from a `*_details` object; blank when the metric is absent, as in the classic UI. */
export function Rate({ details, bytes = false }: { details: RateDetails | undefined; bytes?: boolean }) {
  if (!details) return null
  return <span className="num">{bytes ? fmtRateBytes(details.rate) : fmtRate(details.rate)}</span>
}

/** Shows whether a depth is growing or shrinking, from a rate such as `messages_details.rate`. */
export function Trend({ details }: { details: RateDetails | undefined }) {
  if (!details || Math.abs(details.rate) < 0.05) return null
  const growing = details.rate > 0
  const text = `${growing ? '+' : ''}${fmtRateNum(details.rate)}/s`
  return (
    <span
      className="nowrap"
      style={{ color: growing ? 'var(--warn)' : 'var(--ok)', fontSize: '0.85em', marginLeft: '0.35rem' }}
      title={growing ? `Growing by ${text}` : `Shrinking by ${text}`}
      data-testid="trend"
    >
      {growing ? '▲' : '▼'} {text}
    </span>
  )
}

export function Bool({ value }: { value: boolean | undefined }) {
  if (value === undefined) return <Unknown />
  return <span aria-label={value ? 'yes' : 'no'}>{value ? '●' : '○'}</span>
}

export function Timestamp({ value }: { value: number | null | undefined }) {
  return <span className="nowrap">{fmtTimestamp(value)}</span>
}

export function FeatureTags({ obj }: { obj: Parameters<typeof featureTags>[0] }) {
  const tags = featureTags(obj)
  return (
    <>
      {tags.map((tag) => (
        <abbr key={tag.short} className="tag" title={tag.title}>
          {tag.short}
        </abbr>
      ))}
    </>
  )
}

export function PolicyTags({ policy, operatorPolicy }: { policy?: string | null; operatorPolicy?: string | null }) {
  return (
    <>
      {policy ? (
        <abbr className="tag" title={`Policy: ${policy}`}>
          {policy}
        </abbr>
      ) : null}
      {operatorPolicy ? (
        <abbr className="tag" title={`Operator policy: ${operatorPolicy}`}>
          {operatorPolicy}
        </abbr>
      ) : null}
    </>
  )
}

export function AmqpTableView({ table }: { table: AmqpTable | unknown[] | undefined }) {
  // Erlang encodes an empty proplist as [] rather than {}.
  if (!table || Array.isArray(table) || Object.keys(table).length === 0) return null
  return (
    <table className="facts">
      <tbody>
        {Object.entries(table).map(([key, value]) => (
          <tr key={key}>
            <th>{key}:</th>
            <td>
              <AmqpValueView value={value} />
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

function AmqpValueView({ value }: { value: AmqpValue }): ReactNode {
  if (value === null) return <span className="muted">null</span>
  if (Array.isArray(value)) {
    return (
      <span>
        [
        {value.map((item, i) => (
          <span key={i}>
            {i > 0 ? ', ' : null}
            <AmqpValueView value={item} />
          </span>
        ))}
        ]
      </span>
    )
  }
  if (typeof value === 'object') return <AmqpTableView table={value} />
  if (typeof value === 'boolean') return <span>{String(value)}</span>
  return <span className="mono">{String(value)}</span>
}

export function Facts({ rows }: { rows: [ReactNode, ReactNode][] }) {
  return (
    <table className="facts">
      <tbody>
        {rows.map(([label, value], i) => (
          <tr key={i}>
            <th>{label}</th>
            <td>{value}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}
