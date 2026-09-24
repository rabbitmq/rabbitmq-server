import { useId, type ReactNode } from 'react'
import { useBooleanPref } from '../prefs/storage'
import styles from './Section.module.css'

interface SectionProps {
  /** Stable identifier, used to remember whether the section is open. */
  id: string
  title: ReactNode
  defaultOpen?: boolean
  actions?: ReactNode
  children: ReactNode
}

/**
 * A collapsible panel. The content is not mounted while the section is closed,
 * so queries inside a closed section do not run.
 */
export function Section({ id, title, defaultOpen = true, actions, children }: SectionProps) {
  const [open, setOpen] = useBooleanPref(`next.section.${id}`, defaultOpen)
  const contentId = useId()
  return (
    <section className={styles.section} data-testid={`section-${id}`}>
      <header className={styles.header}>
        <h2>
          <button
            type="button"
            className={styles.toggle}
            aria-expanded={open}
            aria-controls={contentId}
            onClick={() => setOpen(!open)}
          >
            <span className={styles.chevron} aria-hidden="true">
              {open ? '▾' : '▸'}
            </span>
            {title}
          </button>
        </h2>
        {open && actions ? <div className={styles.actions}>{actions}</div> : null}
      </header>
      {open ? (
        <div id={contentId} className={styles.body}>
          {children}
        </div>
      ) : null}
    </section>
  )
}
