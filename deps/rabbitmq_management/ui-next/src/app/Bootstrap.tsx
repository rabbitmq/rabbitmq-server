import { useEffect, useMemo, useState, type ReactNode } from 'react'
import { useQueries, useQueryClient } from '@tanstack/react-query'
import { onUnauthorized } from '../api/client'
import { bootstrapOverviewQuery, extensionsQuery, logout, vhostNamesQuery, whoamiQuery } from '../api/resources/overview'
import { buildAccess } from '../auth/access'
import { LoginPage } from '../auth/LoginPage'
import { clearSession, hasSession, setSessionExpiryIfRequired } from '../auth/session'
import { ErrorMessage, Loading } from '../components/Status'
import { usePref } from '../prefs/storage'
import { AppDataContext, statsModeOf, type AppData } from './context'

export function SessionGate({ children }: { children: ReactNode }) {
  // Re-render on login and logout, in this tab or in the classic UI in another tab.
  const [credentials] = usePref('credentials', '')
  usePref('session_expiry', '')
  const queryClient = useQueryClient()

  useEffect(() => {
    onUnauthorized(() => {
      clearSession()
      queryClient.clear()
    })
    return () => onUnauthorized(undefined)
  }, [queryClient])

  // Cached data belongs to the credentials it was fetched with, and another user
  // may have logged in, possibly through the classic UI in another tab.
  const [cachedFor, setCachedFor] = useState(credentials)
  if (cachedFor !== credentials) {
    queryClient.clear()
    setCachedFor(credentials)
  }

  const authenticated = credentials !== '' && hasSession()
  if (!authenticated) return <LoginPage />
  return <Bootstrap key={credentials}>{children}</Bootstrap>
}

function Bootstrap({ children }: { children: ReactNode }) {
  const queryClient = useQueryClient()
  const [whoami, overview, vhosts, extensions] = useQueries({
    queries: [whoamiQuery(), bootstrapOverviewQuery(), { ...vhostNamesQuery(), staleTime: 30_000 }, extensionsQuery()],
  })

  useEffect(() => {
    if (whoami.data) setSessionExpiryIfRequired(whoami.data.login_session_timeout)
  }, [whoami.data])

  const data = useMemo<AppData | undefined>(() => {
    if (!whoami.data || !overview.data || !vhosts.data) return undefined
    const scripts = new Set(
      (extensions.data ?? []).flatMap((ext) => (Array.isArray(ext) ? [] : [ext.javascript ?? []].flat())),
    )
    return {
      access: buildAccess(whoami.data, vhosts.data),
      overview: overview.data,
      stats: statsModeOf(overview.data),
      vhosts: vhosts.data,
      extensionScripts: scripts,
    }
  }, [whoami.data, overview.data, vhosts.data, extensions.data])

  const error = whoami.error ?? overview.error ?? vhosts.error
  if (error && !data) {
    return (
      <div className="stack" style={{ padding: '2rem', maxWidth: '48rem' }}>
        <ErrorMessage error={error} what="the management API" />
        <div className="row">
          <button type="button" className="btn" onClick={() => void Promise.all([whoami.refetch(), overview.refetch(), vhosts.refetch()])}>
            Retry
          </button>
          <button
            type="button"
            className="btn"
            onClick={() => {
              void logout()
              clearSession()
              queryClient.clear()
            }}
            data-testid="bootstrap-logout"
          >
            Log out
          </button>
        </div>
      </div>
    )
  }
  if (!data) return <Loading label="Connecting to RabbitMQ…" />
  return <AppDataContext.Provider value={data}>{children}</AppDataContext.Provider>
}
