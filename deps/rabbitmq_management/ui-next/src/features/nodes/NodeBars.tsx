import type { ClusterNode } from '../../api/types/nodes'
import { UsageBar } from '../../components/UsageBar'
import {
  FD_THRESHOLDS,
  fmtAxis,
  fmtBytes,
  PROCESS_THRESHOLDS,
  severityFor,
  SOCKETS_THRESHOLDS,
} from '../../format/numbers'

function CountBar({ used, total, thresholds, label }: { used?: number; total?: number; thresholds: typeof FD_THRESHOLDS; label: string }) {
  if (typeof used !== 'number') return <span className="unknown">?</span>
  return (
    <UsageBar
      used={used}
      limit={total}
      severity={total ? severityFor(used / total, thresholds) : 'green'}
      label={fmtAxis(used, used)}
      detail={total !== undefined ? `${fmtAxis(total, total)} available` : undefined}
      testId={label}
    />
  )
}

export function FdBar({ node }: { node: ClusterNode }) {
  return <CountBar used={node.fd_used} total={node.fd_total} thresholds={FD_THRESHOLDS} label="fd-bar" />
}

export function SocketsBar({ node }: { node: ClusterNode }) {
  return <CountBar used={node.sockets_used} total={node.sockets_total} thresholds={SOCKETS_THRESHOLDS} label="sockets-bar" />
}

export function ProcessBar({ node }: { node: ClusterNode }) {
  return <CountBar used={node.proc_used} total={node.proc_total} thresholds={PROCESS_THRESHOLDS} label="proc-bar" />
}

export function MemoryBar({ node }: { node: ClusterNode }) {
  if (typeof node.mem_limit !== 'number') return <span>{fmtBytes(node.mem_used)}</span>
  return (
    <UsageBar
      used={node.mem_used}
      limit={node.mem_limit}
      severity={node.mem_alarm ? 'red' : 'green'}
      label={`${fmtBytes(node.mem_used)}${node.mem_alarm ? ' — alarm' : ''}`}
      detail={`${fmtBytes(node.mem_limit)} high watermark`}
      testId="memory-bar"
    />
  )
}

/** Disk space is shown inverted: the bar fills as free space approaches the low watermark. */
export function DiskBar({ node }: { node: ClusterNode }) {
  if (typeof node.disk_free_limit !== 'number') return <span className="muted">(not available)</span>
  const free = node.disk_free ?? 0
  return (
    <UsageBar
      used={node.disk_free_limit}
      limit={Math.max(free, 1)}
      severity={node.disk_free_alarm ? 'red' : 'green'}
      label={`${fmtBytes(node.disk_free)}${node.disk_free_alarm ? ' — alarm' : ''}`}
      detail={`${fmtBytes(node.disk_free_limit)} low watermark`}
      testId="disk-bar"
    />
  )
}
