import { createContext, useCallback, useContext, useMemo, useRef, useState, type ReactNode } from 'react'
import styles from './notifications.module.css'

export type NotificationKind = 'info' | 'success' | 'error'

interface Notification {
  id: number
  kind: NotificationKind
  message: string
}

interface NotificationApi {
  notify: (kind: NotificationKind, message: string) => void
}

const NotificationContext = createContext<NotificationApi | null>(null)

const TIMEOUT: Record<NotificationKind, number> = { info: 5000, success: 4000, error: 10000 }

export function NotificationProvider({ children }: { children: ReactNode }) {
  const [items, setItems] = useState<Notification[]>([])
  const nextId = useRef(1)

  const dismiss = useCallback((id: number) => setItems((current) => current.filter((item) => item.id !== id)), [])

  const notify = useCallback(
    (kind: NotificationKind, message: string) => {
      const id = nextId.current++
      setItems((current) => [...current.slice(-4), { id, kind, message }])
      window.setTimeout(() => dismiss(id), TIMEOUT[kind])
    },
    [dismiss],
  )

  const api = useMemo(() => ({ notify }), [notify])

  return (
    <NotificationContext.Provider value={api}>
      {children}
      <div className={styles.stack} data-testid="notifications">
        {items.map((item) => (
          <div
            key={item.id}
            className={`${styles.item} ${styles[item.kind]}`}
            role={item.kind === 'error' ? 'alert' : 'status'}
            data-testid={`notification-${item.kind}`}
          >
            <span>{item.message}</span>
            <button type="button" className={styles.close} aria-label="Dismiss" onClick={() => dismiss(item.id)}>
              ×
            </button>
          </div>
        ))}
      </div>
    </NotificationContext.Provider>
  )
}

export function useNotify(): NotificationApi['notify'] {
  const api = useContext(NotificationContext)
  if (!api) throw new Error('useNotify must be used inside NotificationProvider')
  return api.notify
}
