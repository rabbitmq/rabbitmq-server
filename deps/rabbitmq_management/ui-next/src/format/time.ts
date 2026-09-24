import { UNKNOWN } from './numbers'

const pad = (n: number) => String(n).padStart(2, '0')

export function fmtDateParts(date: Date): [string, string] {
  return [
    `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`,
    `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`,
  ]
}

export function fmtTimestamp(ts: number | null | undefined): string {
  if (ts === undefined || ts === null) return UNKNOWN
  return fmtDateParts(new Date(ts)).join(' ')
}

export function fmtTime(ts: number): string {
  return fmtDateParts(new Date(ts))[1]
}

/** Formats an uptime in milliseconds as `Xd Yh`, `Xh Ym` or `Xm Ys`. */
export function fmtUptime(millis: number | undefined): string {
  if (millis === undefined) return UNKNOWN
  const total = Math.floor(millis / 1000)
  const sec = total % 60
  const min = Math.floor(total / 60) % 60
  const hour = Math.floor(total / 3600) % 24
  const day = Math.floor(total / 86400)
  if (day > 0) return `${day}d ${hour}h`
  if (hour > 0) return `${hour}h ${min}m`
  return `${min}m ${sec}s`
}

export function fmtMillis(millis: number): string {
  return `${Math.round(millis / 1000)}s`
}
