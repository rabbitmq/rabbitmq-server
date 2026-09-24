import { describe, expect, it } from 'vitest'
import { authorizationHeader, clearSession, hasSession, setSessionExpiryIfRequired, storeSession } from './session'

describe('session', () => {
  it('uses the classic UI storage keys, so that both UIs share a login', () => {
    storeSession('Basic', 'abc')
    expect(window.localStorage.getItem('rabbitmq.credentials')).toBe('abc')
    expect(window.localStorage.getItem('rabbitmq.auth-scheme')).toBe('Basic')
    expect(authorizationHeader()).toBe('Basic abc')
  })

  it('expires after the login session timeout', () => {
    storeSession('Bearer', 'token')
    setSessionExpiryIfRequired(10, 0)
    expect(hasSession(9 * 60_000)).toBe(true)
    expect(hasSession(11 * 60_000)).toBe(false)
  })

  it('keeps an existing expiry, like set_session_expiry_if_required', () => {
    storeSession('Basic', 'abc')
    setSessionExpiryIfRequired(10, 0)
    setSessionExpiryIfRequired(60, 0)
    expect(window.localStorage.getItem('rabbitmq.session_expiry')).toBe(String(10 * 60_000))
  })

  it('clears everything on logout', () => {
    storeSession('Basic', 'abc')
    clearSession()
    expect(hasSession()).toBe(false)
    expect(authorizationHeader()).toBeNull()
  })
})
