import { useCallback, useSyncExternalStore } from 'react'

// The classic UI stores every preference in localStorage under this prefix
// (see priv/www/js/prefs.js). Sharing the keys means that credentials and
// preferences carry over between the two UIs.
const PREFIX = 'rabbitmq.'

const listeners = new Set<() => void>()
let version = 0

function notify() {
  version++
  for (const listener of listeners) listener()
}

function subscribe(listener: () => void) {
  listeners.add(listener)
  window.addEventListener('storage', listener)
  return () => {
    listeners.delete(listener)
    window.removeEventListener('storage', listener)
  }
}

window.addEventListener('storage', () => version++)

export function getPref(key: string): string | null {
  return window.localStorage.getItem(PREFIX + key)
}

export function setPref(key: string, value: string) {
  window.localStorage.setItem(PREFIX + key, value)
  notify()
}

export function clearPref(key: string) {
  window.localStorage.removeItem(PREFIX + key)
  notify()
}

export function usePref(key: string, defaultValue: string): [string, (value: string) => void] {
  const value = useSyncExternalStore(subscribe, () => getPref(key)) ?? defaultValue
  const update = useCallback((next: string) => setPref(key, next), [key])
  return [value, update]
}

export function useBooleanPref(key: string, defaultValue: boolean): [boolean, (value: boolean) => void] {
  const [value, setValue] = usePref(key, String(defaultValue))
  const update = useCallback((next: boolean) => setValue(String(next)), [setValue])
  return [value === 'true', update]
}

export function usePrefsVersion(): number {
  return useSyncExternalStore(subscribe, () => version)
}
