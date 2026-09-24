import type { ReactNode } from 'react'
import { RateChart, type ChartSeries } from '../../charts/RateChart'

interface ChartBlockProps {
  title: ReactNode
  series: ChartSeries[]
  kind: 'rate' | 'gauge'
  bytes?: boolean
  testId?: string
  idle?: ReactNode
}

export function ChartBlock({ title, series, kind, bytes, testId, idle = 'Currently idle' }: ChartBlockProps) {
  return (
    <div>
      <h3>{title}</h3>
      {series.length > 0 ? <RateChart series={series} kind={kind} bytes={bytes} testId={testId} /> : <p className="muted">{idle}</p>}
    </div>
  )
}
