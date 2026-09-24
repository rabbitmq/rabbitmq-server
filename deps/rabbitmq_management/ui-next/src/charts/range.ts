import type { Overview } from '../api/types/overview'

/** A chart range as the classic UI stores it in the `chart-range` pref: `<age>|<increment>` in seconds. */
export type ChartRange = `${number}|${number}`

export const DEFAULT_CHART_RANGE: ChartRange = '60|5'

const DEFAULT_RANGES: [number, ChartRange, string][] = [
  [60, '60|5', 'Last minute'],
  [600, '600|5', 'Last ten minutes'],
  [3600, '3600|60', 'Last hour'],
  [28800, '28800|600', 'Last eight hours'],
  [86400, '86400|1800', 'Last day'],
]

export interface ChartRangeOption {
  value: ChartRange
  label: string
}

/**
 * The ranges that the sample retention policy can serve, as `setup_chart_ranges`
 * computes them. `global` covers the overview; `basic` covers everything else.
 */
export function chartRanges(policies: Overview['sample_retention_policies'] | undefined, type: 'global' | 'basic'): ChartRangeOption[] {
  const retained = policies?.[type] ?? []
  const options = DEFAULT_RANGES.filter(([age]) => age === 60 || retained.includes(age)).map(([, value, label]) => ({ value, label }))
  return options
}

export type RangeKind = 'lengths' | 'msg_rates' | 'data_rates' | 'node_stats'

export function rangeParams(kinds: RangeKind[], range: ChartRange): Record<string, number> {
  const [age, incr] = range.split('|').map(Number)
  const params: Record<string, number> = {}
  for (const kind of kinds) {
    params[`${kind}_age`] = age
    params[`${kind}_incr`] = incr
  }
  return params
}

export function isChartRange(value: string): value is ChartRange {
  return /^\d+\|\d+$/.test(value)
}
