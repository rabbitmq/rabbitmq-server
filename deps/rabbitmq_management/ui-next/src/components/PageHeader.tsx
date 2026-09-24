import { useEffect, type ReactNode } from 'react'
import styles from './PageHeader.module.css'

interface PageHeaderProps {
  kind?: string
  title: ReactNode
  documentTitle: string
  meta?: ReactNode
  actions?: ReactNode
}

export function PageHeader({ kind, title, documentTitle, meta, actions }: PageHeaderProps) {
  useEffect(() => {
    document.title = `${documentTitle} - RabbitMQ Management`
  }, [documentTitle])
  return (
    <div className={styles.header}>
      <div>
        {kind ? <div className={styles.kind}>{kind}</div> : null}
        <h1 data-testid="page-title">{title}</h1>
        {meta ? <div className={styles.meta}>{meta}</div> : null}
      </div>
      {actions ? <div className={styles.actions}>{actions}</div> : null}
    </div>
  )
}
