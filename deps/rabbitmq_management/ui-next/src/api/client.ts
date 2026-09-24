import { authorizationHeader } from '../auth/session'
import { ApiError, ForbiddenError, NotFoundError, UnauthorizedError } from './errors'

export type QueryParams = Record<string, string | number | boolean | null | undefined>

/**
 * The UI is served from `<prefix>/next/`, so the API is always one level up.
 * Resolving it against the document means that `management.path_prefix` needs
 * no build-time configuration.
 */
export function apiBase(): URL {
  return new URL('../api/', document.baseURI)
}

/** Encodes one path segment, like `esc` in the classic UI. */
export const seg = (value: string) => encodeURIComponent(value)

export function apiUrl(path: string, params?: QueryParams): string {
  const url = new URL(path.replace(/^\//, ''), apiBase())
  for (const [key, value] of Object.entries(params ?? {})) {
    if (value !== undefined && value !== null && value !== '') url.searchParams.set(key, String(value))
  }
  return url.toString()
}

let unauthorizedHandler: (() => void) | undefined
let suspended = 0

/** Registered by the session provider, so that a rejected session returns to the login form. */
export function onUnauthorized(handler: (() => void) | undefined) {
  unauthorizedHandler = handler
}

/**
 * Runs `fn` without ending the session on 401 responses, for operations that
 * replace the credentials, such as changing one's own password.
 */
export async function withSessionChecksSuspended<T>(fn: () => Promise<T>): Promise<T> {
  suspended++
  try {
    return await fn()
  } finally {
    suspended--
  }
}

/** A 401 only ends the session if it was sent with the credentials that are still current. */
function rejectSession(sentWith: string | undefined) {
  if (suspended === 0 && sentWith === (authorizationHeader() ?? undefined)) unauthorizedHandler?.()
}

export interface ApiRequest {
  method?: 'GET' | 'PUT' | 'POST' | 'DELETE'
  params?: QueryParams
  body?: unknown
  headers?: Record<string, string>
  signal?: AbortSignal
  /** Skip the Authorization header, for the login request itself. */
  anonymous?: boolean
}

export async function apiFetch<T>(path: string, request: ApiRequest = {}): Promise<T> {
  const response = await apiResponse(path, request)
  if (response.status === 204) return undefined as T
  const text = await response.text()
  return (text === '' ? undefined : JSON.parse(text)) as T
}

export async function apiResponse(path: string, request: ApiRequest = {}): Promise<Response> {
  const { method = 'GET', params, body, signal, anonymous = false } = request
  const headers: Record<string, string> = { ...request.headers }
  if (!anonymous) {
    const auth = authorizationHeader()
    if (auth === null) {
      unauthorizedHandler?.()
      throw new UnauthorizedError('not_authorized', 'The session has expired', path)
    }
    headers.Authorization = auth
  }
  let payload: BodyInit | undefined
  if (body instanceof URLSearchParams || body instanceof FormData) {
    payload = body
  } else if (body !== undefined) {
    headers['Content-Type'] = 'application/json'
    payload = JSON.stringify(body)
  }

  const response = await fetch(apiUrl(path, params), { method, headers, body: payload, signal })
  if (response.ok) return response
  throw await toError(response, path, anonymous ? undefined : headers.Authorization)
}

async function toError(response: Response, path: string, sentWith: string | undefined): Promise<ApiError> {
  const body = (await response.json().catch(() => null)) as { error?: string; reason?: unknown } | null
  const error = body?.error
  const reason = typeof body?.reason === 'string' ? body.reason : undefined
  switch (response.status) {
    case 401:
      // A missing tag and rejected credentials both return 401. Only the error
      // field tells them apart: `not_authorised` is a permission denial, see
      // rabbit_web_dispatch_access_control.
      if (error === 'not_authorised') return new ForbiddenError(401, error, reason, path)
      if (sentWith !== undefined) rejectSession(sentWith)
      return new UnauthorizedError(error, reason, path)
    case 403:
      return new ForbiddenError(403, error, reason, path)
    case 404:
      return new NotFoundError(error, reason, path)
    default:
      return new ApiError(response.status, error, reason, path)
  }
}
