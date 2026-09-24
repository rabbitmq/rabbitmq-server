import { afterEach, describe, expect, it, vi } from 'vitest'
import { http, HttpResponse } from 'msw'
import { server } from '../../test/server'
import { apiFetch, apiUrl, onUnauthorized } from './client'
import { ForbiddenError, NotFoundError, UnauthorizedError } from './errors'
import { storeSession } from '../auth/session'

afterEach(() => onUnauthorized(undefined))

describe('apiUrl', () => {
  it('resolves the API one level above the UI and drops empty parameters', () => {
    expect(apiUrl('queues/%2F', { page: 1, name: '', sort: undefined })).toBe('http://localhost:3000/api/queues/%2F?page=1')
  })
})

describe('apiFetch', () => {
  it('sends the stored credentials', async () => {
    storeSession('Basic', 'Z3Vlc3Q6Z3Vlc3Q=')
    let seen: string | null = null
    server.use(
      http.get('*/api/whoami', ({ request }) => {
        seen = request.headers.get('authorization')
        return HttpResponse.json({ name: 'guest', tags: [], is_internal_user: true })
      }),
    )
    await apiFetch('whoami')
    expect(seen).toBe('Basic Z3Vlc3Q6Z3Vlc3Q=')
  })

  it('treats a 401 with not_authorised as a permission denial and keeps the session', async () => {
    storeSession('Basic', 'x')
    const handler = vi.fn()
    onUnauthorized(handler)
    server.use(http.get('*/api/nodes', () => HttpResponse.json({ error: 'not_authorised', reason: 'Not monitor user' }, { status: 401 })))
    await expect(apiFetch('nodes')).rejects.toBeInstanceOf(ForbiddenError)
    expect(handler).not.toHaveBeenCalled()
  })

  it('ends the session on rejected credentials', async () => {
    storeSession('Basic', 'x')
    const handler = vi.fn()
    onUnauthorized(handler)
    server.use(http.get('*/api/overview', () => HttpResponse.json({ error: 'not_authorized', reason: 'Not_Authorized' }, { status: 401 })))
    await expect(apiFetch('overview')).rejects.toBeInstanceOf(UnauthorizedError)
    expect(handler).toHaveBeenCalledOnce()
  })

  it('does not call the API without a session', async () => {
    const handler = vi.fn()
    onUnauthorized(handler)
    await expect(apiFetch('overview')).rejects.toBeInstanceOf(UnauthorizedError)
    expect(handler).toHaveBeenCalledOnce()
  })

  it('maps 404 and returns undefined for 204', async () => {
    storeSession('Basic', 'x')
    server.use(
      http.get('*/api/queues/%2F/missing', () => HttpResponse.json({ error: 'Object Not Found', reason: 'Not Found' }, { status: 404 })),
      http.delete('*/api/queues/%2F/q', () => new HttpResponse(null, { status: 204 })),
    )
    await expect(apiFetch('queues/%2F/missing')).rejects.toBeInstanceOf(NotFoundError)
    await expect(apiFetch('queues/%2F/q', { method: 'DELETE' })).resolves.toBeUndefined()
  })
})
