import { clearPref, getPref, setPref } from '../prefs/storage'

export type AuthScheme = 'Basic' | 'Bearer'

const CREDENTIALS = 'credentials'
const AUTH_SCHEME = 'auth-scheme'
const SESSION_EXPIRY = 'session_expiry'
const AUTH_RESOURCE = 'auth_resource'

// Matches DEFAULT_HARD_LOGIN_SESSION_TIMEOUT in the classic UI: 8 hours.
const DEFAULT_SESSION_TIMEOUT_MINUTES = 480

export function hasSession(now = Date.now()): boolean {
  if (getPref(CREDENTIALS) === null || getPref(AUTH_SCHEME) === null) return false
  const expiry = getPref(SESSION_EXPIRY)
  return expiry === null || now < parseInt(expiry, 10)
}

export function authorizationHeader(): string | null {
  return hasSession() ? `${getPref(AUTH_SCHEME)} ${getPref(CREDENTIALS)}` : null
}

export function storeSession(scheme: AuthScheme, credentials: string) {
  clearPref(SESSION_EXPIRY)
  setPref(CREDENTIALS, credentials)
  setPref(AUTH_SCHEME, scheme)
}

/**
 * Sets the hard session expiry once per login, as `set_session_expiry_if_required`
 * does in the classic UI, so that a session started there keeps its expiry here.
 */
export function setSessionExpiryIfRequired(loginSessionTimeout: number | undefined, now = Date.now()) {
  if (getPref(SESSION_EXPIRY) !== null) return
  const minutes = Number.isFinite(loginSessionTimeout) ? Number(loginSessionTimeout) : DEFAULT_SESSION_TIMEOUT_MINUTES
  setPref(SESSION_EXPIRY, String(now + minutes * 60_000))
}

export function clearSession() {
  clearPref(CREDENTIALS)
  clearPref(AUTH_SCHEME)
  clearPref(SESSION_EXPIRY)
  clearPref(AUTH_RESOURCE)
}
