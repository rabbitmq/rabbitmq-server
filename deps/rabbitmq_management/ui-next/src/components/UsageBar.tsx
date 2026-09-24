import type { Severity } from '../format/numbers'
import styles from './UsageBar.module.css'

interface UsageBarProps {
  used: number | undefined
  limit: number | undefined
  severity: Severity
  label: string
  detail?: string
  testId?: string
}

/** A horizontal usage bar, the equivalent of `node_stat_bar` in the classic UI. */
export function UsageBar({ used, limit, severity, label, detail, testId }: UsageBarProps) {
  const ratio = used !== undefined && limit ? Math.min(1, used / limit) : 0
  return (
    <div className={styles.wrap} data-testid={testId}>
      <div
        className={styles.track}
        role="meter"
        aria-valuemin={0}
        aria-valuemax={limit ?? 0}
        aria-valuenow={used ?? 0}
        aria-label={`${label}${detail ? `, ${detail}` : ''}`}
      >
        <div className={`${styles.fill} ${styles[severity]}`} style={{ width: `${ratio * 100}%` }} />
        <span className={styles.text}>{label}</span>
      </div>
      {detail ? <div className={styles.detail}>{detail}</div> : null}
    </div>
  )
}
