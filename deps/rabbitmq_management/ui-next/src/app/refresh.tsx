import { createContext, useCallback, useContext, useMemo, useState, type ReactNode } from 'react'
import { useRouterState } from '@tanstack/react-router'
import { usePref } from '../prefs/storage'

export const REFRESH_OPTIONS: { value: string; label: string }[] = [
  { value: '5000', label: 'Refresh every 5 seconds' },
  { value: '10000', label: 'Refresh every 10 seconds' },
  { value: '30000', label: 'Refresh every 30 seconds' },
  { value: '', label: 'Do not refresh' },
]

/** Data that rarely changes, such as users and policies, is refreshed at most this often. */
const TOPOLOGY_INTERVAL = 30_000

interface RefreshState {
  interval: number | false
  setInterval: (value: string) => void
  intervalPref: string
  paused: boolean
  togglePaused: () => void
}

const RefreshContext = createContext<RefreshState | null>(null)

export function RefreshProvider({ children }: { children: ReactNode }) {
  // Same key and format as the classic UI: milliseconds, or '' for "do not refresh".
  const [intervalPref, setIntervalPref] = usePref('interval', '5000')
  const routeId = useRouterState({ select: (state) => state.matches.at(-1)?.routeId ?? '' })
  // Pausing is sticky per screen, so that returning to a paused screen keeps it still.
  const [pausedRoutes, setPausedRoutes] = useState<ReadonlySet<string>>(new Set())
  const paused = pausedRoutes.has(routeId)

  const togglePaused = useCallback(() => {
    setPausedRoutes((current) => {
      const next = new Set(current)
      if (next.has(routeId)) next.delete(routeId)
      else next.add(routeId)
      return next
    })
  }, [routeId])

  const value = useMemo<RefreshState>(() => {
    const parsed = parseInt(intervalPref, 10)
    return {
      interval: Number.isFinite(parsed) && parsed > 0 ? parsed : false,
      intervalPref,
      setInterval: setIntervalPref,
      paused,
      togglePaused,
    }
  }, [intervalPref, setIntervalPref, paused, togglePaused])

  return <RefreshContext.Provider value={value}>{children}</RefreshContext.Provider>
}

export function useRefresh(): RefreshState {
  const state = useContext(RefreshContext)
  if (!state) throw new Error('useRefresh must be used inside RefreshProvider')
  return state
}

export type RefreshKind = 'stats' | 'topology'

/** The `refetchInterval` for a query. Polling stops while the document is hidden. */
export function useRefetchInterval(kind: RefreshKind = 'stats'): number | false {
  const { interval, paused } = useRefresh()
  if (paused || interval === false) return false
  return kind === 'topology' ? Math.max(interval, TOPOLOGY_INTERVAL) : interval
}
