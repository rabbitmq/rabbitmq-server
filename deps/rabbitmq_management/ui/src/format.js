const FD_THRESHOLDS = [
  [0.95, 'red'],
  [0.8, 'yellow']
]
const PROCESS_THRESHOLDS = [
  [0.75, 'red'],
  [0.5, 'yellow']
]

function isNumeric(value) {
  return typeof value === 'number' && Number.isFinite(value)
}

function usage(used, total, thresholds = []) {
  if (!isNumeric(used) || !isNumeric(total) || total <= 0) {
    return { ratio: 0, level: 'green' }
  }
  const ratio = Math.min(1, Math.max(0, used / total))
  const level = thresholds.find(([threshold]) => ratio >= threshold)?.[1] ?? 'green'
  return { ratio, level }
}

function formatBytes(bytes) {
  if (!isNumeric(bytes)) return '—'
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']
  let value = bytes
  let unitIndex = 0
  while (Math.abs(value) >= 1024 && unitIndex < units.length - 1) {
    value /= 1024
    unitIndex += 1
  }
  const precision = unitIndex === 0 ? 0 : 1
  return `${value.toFixed(precision)} ${units[unitIndex]}`
}

function formatUptime(ms) {
  if (!isNumeric(ms)) return '—'
  const uptime = Math.floor(ms / 1000)
  const sec = uptime % 60
  const min = Math.floor(uptime / 60) % 60
  const hour = Math.floor(uptime / 3600) % 24
  const day = Math.floor(uptime / 86400)

  if (day > 0) return `${day}d ${hour}h`
  if (hour > 0) return `${hour}h ${min}m`
  return `${min}m ${sec}s`
}

function formatNumber(n, decimals = 0) {
  if (!isNumeric(n)) return '—'
  return n.toLocaleString('en-US', { minimumFractionDigits: decimals, maximumFractionDigits: decimals })
}

function formatCount(n) {
  return formatNumber(n, 0)
}

export {
  isNumeric,
  usage,
  formatBytes,
  formatUptime,
  formatNumber,
  formatCount,
  FD_THRESHOLDS,
  PROCESS_THRESHOLDS
}
