export const UNKNOWN = '?'

type Num = number | null | undefined

const POWERS = ['', 'k', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y']

/**
 * Scales `num` by the SI prefix that suits `max`, as `fmt_si_prefix` does in the
 * classic UI. Unlike that function, zero is returned as `0` rather than `0 `.
 */
export function fmtSiPrefix(num: number, max: number, binary: boolean, allowFractions: boolean): string {
  if (num === 0) return '0'
  const thousand = binary ? 1024 : 1000
  let n = num
  let m = max
  let power = 0
  while (m > thousand && power < POWERS.length - 1) {
    n /= thousand
    m /= thousand
    power++
  }
  let suffix = POWERS[power]
  if (power !== 0 && binary) suffix = suffix.toUpperCase() + 'i'
  const digits = (power !== 0 || allowFractions) && m <= 10 ? 1 : 0
  return `${n.toFixed(digits)} ${suffix}`.trimEnd()
}

export function fmtNum(num: Num): string {
  if (num === undefined || num === null || Number.isNaN(num)) return UNKNOWN
  return Math.round(num).toLocaleString('en-US')
}

export function fmtRateNum(num: Num): string {
  if (num === undefined || num === null || Number.isNaN(num)) return UNKNOWN
  const abs = Math.abs(num)
  if (abs < 1) return num.toFixed(2)
  if (abs < 10) return num.toFixed(1)
  return fmtNum(num)
}

export function fmtRate(num: Num): string {
  return `${fmtRateNum(num)}/s`
}

export function fmtBytes(bytes: Num): string {
  if (bytes === undefined || bytes === null || Number.isNaN(bytes)) return UNKNOWN
  const scaled = fmtSiPrefix(bytes, bytes, true, false)
  // Without a prefix, the number still needs a space before the unit.
  return scaled.includes(' ') ? `${scaled}B` : `${scaled} B`
}

export function fmtRateBytes(num: Num): string {
  return `${fmtBytes(num)}/s`
}

export function fmtPercent(ratio: Num): string {
  if (ratio === undefined || ratio === null || Number.isNaN(ratio)) return UNKNOWN
  return `${Math.round(ratio * 100)}%`
}

export function fmtAxis(num: number, max: number): string {
  return fmtSiPrefix(num, max, false, true)
}

export function fmtBytesAxis(num: number): string {
  return fmtBytes(Math.round(num))
}

export type Severity = 'green' | 'yellow' | 'red'

export const FD_THRESHOLDS: [number, Severity][] = [
  [0.95, 'red'],
  [0.8, 'yellow'],
]
export const SOCKETS_THRESHOLDS: [number, Severity][] = [
  [1.0, 'red'],
  [0.8, 'yellow'],
]
export const PROCESS_THRESHOLDS: [number, Severity][] = [
  [0.75, 'red'],
  [0.5, 'yellow'],
]

/** Picks the colour for a usage ratio, as `fmt_color` does in the classic UI. */
export function severityFor(ratio: number, thresholds: [number, Severity][]): Severity {
  for (const [threshold, severity] of thresholds) {
    if (ratio >= threshold) return severity
  }
  return 'green'
}
