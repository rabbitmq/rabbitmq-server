import { createContext, useContext } from 'react'
import type { Access } from '../auth/access'
import type { Overview } from '../api/types/overview'
import type { Vhost } from '../api/types/admin'

export interface StatsMode {
  /** No statistics are collected at all: `disable_stats` in the overview. */
  disabled: boolean
  ratesMode: Overview['rates_mode']
  hasRates: boolean
  queueTotals: boolean
}

export interface AppData {
  access: Access
  /** The overview as read at start-up; its settings do not change while the UI runs. */
  overview: Overview
  stats: StatsMode
  vhosts: Vhost[]
  extensionScripts: Set<string>
}

export const AppDataContext = createContext<AppData | null>(null)

export function useAppData(): AppData {
  const data = useContext(AppDataContext)
  if (!data) throw new Error('useAppData must be used inside the authenticated application')
  return data
}

export function useAccess(): Access {
  return useAppData().access
}

export function useStatsMode(): StatsMode {
  return useAppData().stats
}

export function statsModeOf(overview: Overview): StatsMode {
  return {
    disabled: overview.disable_stats,
    ratesMode: overview.rates_mode,
    hasRates: !overview.disable_stats && overview.rates_mode !== 'none',
    queueTotals: overview.enable_queue_totals,
  }
}
