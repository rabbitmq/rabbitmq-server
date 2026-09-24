import { useCallback, useEffect, useSyncExternalStore } from 'react'

export type ColorScheme = 'auto' | 'light' | 'dark'

// Shared with the classic UI's theme switcher, which stores it without the `rabbitmq.` prefix.
const KEY = 'color-scheme'
const listeners = new Set<() => void>()

function read(): ColorScheme {
  const value = window.localStorage.getItem(KEY)
  return value === 'light' || value === 'dark' ? value : 'auto'
}

function subscribe(listener: () => void) {
  listeners.add(listener)
  return () => listeners.delete(listener)
}

export function useColorScheme(): [ColorScheme, (scheme: ColorScheme) => void] {
  const scheme = useSyncExternalStore(subscribe, read)
  const setScheme = useCallback((next: ColorScheme) => {
    window.localStorage.setItem(KEY, next)
    for (const listener of listeners) listener()
  }, [])
  useEffect(() => {
    if (scheme === 'auto') delete document.documentElement.dataset.theme
    else document.documentElement.dataset.theme = scheme
  }, [scheme])
  return [scheme, setScheme]
}
