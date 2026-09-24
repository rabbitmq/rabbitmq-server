import { useMemo } from 'react'
import type { ChartConfiguration } from 'chart.js'
import type { RateDetails } from '../api/types/common'
import { fmtAxis, fmtBytes, fmtBytesAxis, fmtNum, fmtRate, fmtRateBytes } from '../format/numbers'
import { fmtTime } from '../format/time'
import { gaugePoints, hasSamples, ratePoints } from './series'
import { useChart } from './useChart'
import styles from './RateChart.module.css'

export interface ChartSeries {
  key: string
  label: string
  details: RateDetails | undefined
  value?: number
}

interface RateChartProps {
  series: ChartSeries[]
  /** `rate` plots counters as per-second rates; `gauge` plots values as sampled. */
  kind: 'rate' | 'gauge'
  bytes?: boolean
  height?: number
  testId?: string
}

const COLOURS = ['--chart-1', '--chart-2', '--chart-3', '--chart-4', '--chart-5', '--chart-6', '--chart-7', '--chart-8']

function cssVar(name: string): string {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim() || '#888'
}

/**
 * A line chart with a legend of current values. When there are no samples, for
 * example right after start-up, only the values are shown rather than an empty frame.
 */
export function RateChart({ series, kind, bytes = false, height = 180, testId }: RateChartProps) {
  const present = series.filter((s) => s.details !== undefined)
  const withSamples = hasSamples(present.map((s) => s.details))
  const current = (s: ChartSeries) => (kind === 'rate' ? s.details?.rate : (s.value ?? s.details?.samples?.[0]?.sample))
  const fmtCurrent = (value: number | undefined) =>
    value === undefined ? '' : kind === 'rate' ? (bytes ? fmtRateBytes(value) : fmtRate(value)) : bytes ? fmtBytes(value) : fmtNum(value)

  if (present.length === 0) return null

  return (
    <div className={styles.wrap} data-testid={testId}>
      {withSamples ? <ChartCanvas series={present} kind={kind} bytes={bytes} height={height} /> : null}
      <ul className={styles.legend}>
        {present.map((s) => (
          <li key={s.key}>
            <span className={styles.swatch} style={{ background: `var(${COLOURS[present.indexOf(s) % COLOURS.length]})` }} />
            <span className={styles.label}>{s.label}</span>
            <span className={`num ${styles.value}`} data-testid={testId ? `${testId}-${s.key}` : undefined}>
              {fmtCurrent(current(s))}
            </span>
          </li>
        ))}
      </ul>
    </div>
  )
}

function ChartCanvas({ series, kind, bytes, height }: Required<Omit<RateChartProps, 'testId'>>) {
  const config = useMemo<ChartConfiguration<'line', { x: number; y: number }[]>>(() => {
    const grid = cssVar('--chart-grid')
    const muted = cssVar('--text-muted')
    return {
      type: 'line',
      data: {
        datasets: series.map((s, i) => {
          const colour = cssVar(COLOURS[i % COLOURS.length])
          return {
            label: s.label,
            data: kind === 'rate' ? ratePoints(s.details) : gaugePoints(s.details),
            borderColor: colour,
            backgroundColor: colour,
            borderWidth: 1.5,
            pointRadius: 0,
            tension: 0.25,
          }
        }),
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        interaction: { mode: 'index', intersect: false },
        scales: {
          x: {
            type: 'linear',
            grid: { color: grid },
            ticks: { color: muted, maxTicksLimit: 6, callback: (value) => fmtTime(Number(value)) },
          },
          y: {
            beginAtZero: true,
            grid: { color: grid },
            ticks: {
              color: muted,
              maxTicksLimit: 5,
              callback: (value, _index, ticks) => {
                const max = Math.max(...ticks.map((t) => Number(t.value)))
                const n = Number(value)
                return bytes ? fmtBytesAxis(n) : fmtAxis(n, max)
              },
            },
          },
        },
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              title: (items) => (items[0] ? fmtTime(items[0].parsed.x ?? 0) : ''),
              label: (item) => {
                const y = item.parsed.y ?? 0
                const text = kind === 'rate' ? (bytes ? fmtRateBytes(y) : fmtRate(y)) : bytes ? fmtBytes(y) : fmtNum(y)
                return `${item.dataset.label}: ${text}`
              },
            },
          },
        },
      },
    }
  }, [series, kind, bytes])

  const canvasRef = useChart(config)
  return (
    <div className={styles.canvas} style={{ height }}>
      <canvas ref={canvasRef} role="img" aria-label={series.map((s) => s.label).join(', ')} />
    </div>
  )
}
