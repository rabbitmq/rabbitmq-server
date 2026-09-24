import { http, HttpResponse } from 'msw'
import extensions from './fixtures/extensions.json'
import nodes from './fixtures/nodes.json'
import overview from './fixtures/overview.json'
import queuesPage from './fixtures/queues-page.json'
import vhosts from './fixtures/vhosts.json'

export interface BrokerOptions {
  tags?: string[]
  /** Credentials that the login endpoint accepts. */
  password?: string
  disableStats?: boolean
  onQueues?: (url: URL) => void
}

/** Handlers for a single-node broker, built from responses captured from a real one. */
export function brokerHandlers({ tags = ['administrator'], password = 'guest', disableStats = false, onQueues }: BrokerOptions = {}) {
  const monitoring = tags.includes('administrator') || tags.includes('monitoring')
  const settings = { ...overview, disable_stats: disableStats }
  return [
    http.post('*/api/login', async ({ request }) => {
      const form = new URLSearchParams(await request.text())
      if (form.get('password') !== password) {
        return HttpResponse.json({ error: 'not_authorized', reason: 'Not_Authorized' }, { status: 401 })
      }
      const user = { name: form.get('username'), tags, is_internal_user: true }
      return HttpResponse.json({ token: { type: 'basic', value: btoa(`${form.get('username')}:${password}`) }, user })
    }),
    http.get('*/api/whoami', () => HttpResponse.json({ name: 'guest', tags, is_internal_user: true })),
    http.get('*/api/overview', () => HttpResponse.json(settings)),
    http.get('*/api/vhosts', () => HttpResponse.json(vhosts)),
    http.get('*/api/extensions', () => HttpResponse.json(extensions)),
    http.get('*/api/nodes', () =>
      monitoring ? HttpResponse.json(nodes) : HttpResponse.json({ error: 'not_authorised', reason: 'Not monitor user' }, { status: 401 }),
    ),
    http.get('*/api/health/checks/alarms', () => HttpResponse.json({ status: 'ok' })),
    http.get('*/api/queues', ({ request }) => {
      onQueues?.(new URL(request.url))
      return HttpResponse.json(queuesPage)
    }),
  ]
}
