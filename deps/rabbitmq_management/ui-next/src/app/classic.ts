/** The classic UI, which is served one level above this one. In development it is the broker itself. */
export const CLASSIC_BASE = import.meta.env.DEV ? (import.meta.env.VITE_CLASSIC_UI_URL ?? 'http://localhost:15672/') : '../'

export function classicHref(hash: string = window.location.hash): string {
  const path = hash.replace(/^#/, '').split('?')[0] || '/'
  return `${CLASSIC_BASE}#${path}`
}
