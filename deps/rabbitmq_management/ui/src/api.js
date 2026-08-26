const AUTH_SCHEME_KEY = 'rmq_auth_scheme'
const AUTH_VALUE_KEY = 'rmq_auth_value'

function getAuthHeader() {
  const scheme = sessionStorage.getItem(AUTH_SCHEME_KEY)
  const value = sessionStorage.getItem(AUTH_VALUE_KEY)
  if (!scheme || !value) return null
  return `${scheme} ${value}`
}

function setAuth(scheme, value) {
  sessionStorage.setItem(AUTH_SCHEME_KEY, scheme)
  sessionStorage.setItem(AUTH_VALUE_KEY, value)
}

function clearAuth() {
  sessionStorage.removeItem(AUTH_SCHEME_KEY)
  sessionStorage.removeItem(AUTH_VALUE_KEY)
}

function isAuthenticated() {
  return getAuthHeader() !== null
}

class UnauthorizedError extends Error {}
class ForbiddenError extends Error {}

async function authorizedFetch(path, options = {}) {
  const authHeader = getAuthHeader()
  if (!authHeader) throw new UnauthorizedError('not logged in')

  const response = await fetch(path, {
    ...options,
    headers: {
      ...options.headers,
      Authorization: authHeader
    }
  })

  if (response.status === 401) {
    // A missing tag (e.g. `monitoring`) and an invalid session both come back as
    // 401 with an identical status line — the only way to tell them apart is the
    // JSON body's `error` field: `not_authorised` (permission denial, credentials
    // are fine) vs. `not_authorized` (credentials themselves were rejected). See
    // rabbit_web_dispatch_access_control:not_authorised/3 and :343.
    const body = await response.json().catch(() => null)
    if (body?.error === 'not_authorised') {
      throw new ForbiddenError(body.reason ?? `${path} requires additional permissions`)
    }
    clearAuth()
    throw new UnauthorizedError('session rejected by server')
  }

  if (response.status === 403) {
    throw new ForbiddenError(`${path} requires additional permissions`)
  }

  if (!response.ok) {
    throw new Error(`${path} returned ${response.status}`)
  }

  return response.json()
}

async function login(username, password) {
  const value = btoa(`${username}:${password}`)
  setAuth('Basic', value)

  try {
    const whoami = await authorizedFetch('/api/whoami')
    return whoami
  } catch (err) {
    clearAuth()
    throw err
  }
}

function logout() {
  clearAuth()
}

function fetchOverview({ lengthsAge = 60, lengthsIncr = 5, msgRatesAge = 60, msgRatesIncr = 5 } = {}) {
  const params = new URLSearchParams({
    lengths_age: lengthsAge,
    lengths_incr: lengthsIncr,
    msg_rates_age: msgRatesAge,
    msg_rates_incr: msgRatesIncr
  })
  return authorizedFetch(`/api/overview?${params.toString()}`)
}

function fetchNodes() {
  return authorizedFetch('/api/nodes')
}

export { login, logout, isAuthenticated, fetchOverview, fetchNodes, UnauthorizedError, ForbiddenError }
