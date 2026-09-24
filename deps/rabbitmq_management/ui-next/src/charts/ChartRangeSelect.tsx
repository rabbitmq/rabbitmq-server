import { useAppData } from '../app/context'
import { usePref } from '../prefs/storage'
import { chartRanges, DEFAULT_CHART_RANGE, isChartRange, type ChartRange } from './range'

/** The chart range shared by every chart, stored in the classic UI's `chart-range` pref. */
export function useChartRange(type: 'global' | 'basic' = 'basic'): ChartRange | undefined {
  const { overview, stats } = useAppData()
  const [pref] = usePref('chart-range', DEFAULT_CHART_RANGE)
  if (stats.disabled) return undefined
  const options = chartRanges(overview.sample_retention_policies, type)
  const value = isChartRange(pref) ? pref : DEFAULT_CHART_RANGE
  // A range the retention policy cannot serve falls back to the longest one it can.
  return options.some((o) => o.value === value) ? value : (options.at(-1)?.value ?? DEFAULT_CHART_RANGE)
}

export function ChartRangeSelect({ type = 'basic' }: { type?: 'global' | 'basic' }) {
  const { overview } = useAppData()
  const [, setPref] = usePref('chart-range', DEFAULT_CHART_RANGE)
  const value = useChartRange(type)
  const options = chartRanges(overview.sample_retention_policies, type)
  if (value === undefined) return null
  return (
    <select
      aria-label="Chart range"
      value={value}
      onChange={(event) => setPref(event.target.value)}
      onClick={(event) => event.stopPropagation()}
      data-testid="chart-range"
    >
      {options.map((option) => (
        <option key={option.value} value={option.value}>
          {option.label}
        </option>
      ))}
    </select>
  )
}
