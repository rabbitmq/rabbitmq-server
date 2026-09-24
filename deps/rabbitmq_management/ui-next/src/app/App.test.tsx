import { describe, expect, it } from 'vitest'
import { screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'
import { renderApp } from '../../test/renderApp'
import { server } from '../../test/server'
import queuesPage from '../../test/fixtures/queues-page.json'

describe('the application', () => {
  it('logs in with basic authentication and stores the session for both UIs', async () => {
    renderApp('#/', { loggedIn: false })
    await userEvent.type(await screen.findByTestId('login-username'), 'guest')
    await userEvent.type(screen.getByTestId('login-password'), 'guest')
    await userEvent.click(screen.getByTestId('login-submit'))
    expect(await screen.findByTestId('page-title')).toHaveTextContent('Overview')
    expect(window.localStorage.getItem('rabbitmq.credentials')).toBe(btoa('guest:guest'))
    expect(window.localStorage.getItem('rabbitmq.session_expiry')).not.toBeNull()
  })

  it('reports a failed login', async () => {
    renderApp('#/', { loggedIn: false })
    await userEvent.type(await screen.findByTestId('login-username'), 'guest')
    await userEvent.type(screen.getByTestId('login-password'), 'wrong')
    await userEvent.click(screen.getByTestId('login-submit'))
    expect(await screen.findByTestId('login-error')).toHaveTextContent('Login failed')
  })

  it('shows an administrator the whole navigation and the nodes of the cluster', async () => {
    renderApp('#/')
    expect(await screen.findByTestId('nav-admin')).toBeInTheDocument()
    expect(await screen.findByTestId('node-link')).toHaveTextContent(/^rabbit@/)
    expect(await screen.findByTestId('attention-none')).toBeInTheDocument()
  })

  it('leaves out what a management user cannot use', async () => {
    renderApp('#/', { tags: ['management'] })
    expect(await screen.findByTestId('page-title')).toHaveTextContent('Overview')
    // Without node data, only the alarms health check can be reported on.
    expect(await screen.findByTestId('attention-none')).toHaveTextContent('No resource alarms in effect.')
    expect(screen.queryByTestId('section-overview-nodes')).not.toBeInTheDocument()
    expect(screen.queryByTestId('section-overview-export')).not.toBeInTheDocument()
    // Admin still leads to Policies and Limits, but not to Users.
    await userEvent.click(screen.getByTestId('nav-admin'))
    expect(await screen.findByTestId('page-title')).toHaveTextContent('Policies')
    expect(screen.queryByTestId('nav-admin-users')).not.toBeInTheDocument()
  })

  it('refuses an administrator screen reached by URL', async () => {
    renderApp('#/users', { tags: ['management'] })
    expect(await screen.findByTestId('not-permitted')).toHaveTextContent('administrator')
  })

  it('renders an empty state for an unknown route', async () => {
    renderApp('#/no/such/screen')
    expect(await screen.findByTestId('not-found')).toBeInTheDocument()
  })
})

describe('the queue list', () => {
  it('asks the server for one page, and for sorting, rather than sorting in the browser', async () => {
    const requests: URL[] = []
    renderApp('#/queues', { onQueues: (url) => requests.push(url) })
    const table = await screen.findByTestId('queues-table')
    await waitFor(() => expect(within(table).getAllByTestId('queues-row')).toHaveLength(4))
    expect(requests[0].searchParams.get('page')).toBe('1')
    expect(requests[0].searchParams.get('page_size')).toBe('100')
    // Only the fields the visible columns need are requested.
    expect(requests[0].searchParams.get('columns')).toContain('messages_ready')

    await userEvent.click(within(screen.getByTestId('queues-header-msgs-total')).getByRole('button', { name: 'Total' }))
    await waitFor(() => expect(requests.at(-1)?.searchParams.get('sort')).toBe('messages'))
    expect(requests.at(-1)?.searchParams.get('sort_reverse')).toBe('false')
    expect(window.location.hash).toContain('sort=messages')
  })

  it('returns to the first page when the requested page no longer exists', async () => {
    const pages: string[] = []
    renderApp('#/queues?page=9')
    server.use(
      http.get('*/api/queues', ({ request }) => {
        const page = new URL(request.url).searchParams.get('page') ?? ''
        pages.push(page)
        return page === '1'
          ? HttpResponse.json(queuesPage)
          : HttpResponse.json({ error: 'page_out_of_range', reason: 'Page out of range' }, { status: 400 })
      }),
    )
    await waitFor(() => expect(screen.getAllByTestId('queues-row')).toHaveLength(4))
    expect(pages[0]).toBe('9')
    expect(window.location.hash).not.toContain('page=9')
  })

  it('removes rate columns when statistics are disabled', async () => {
    renderApp('#/queues', { disableStats: true })
    await screen.findByTestId('queues-table')
    expect(screen.queryByTestId('queues-header-rate-incoming')).not.toBeInTheDocument()
    expect(screen.queryByTestId('queues-header-msgs-total')).not.toBeInTheDocument()
  })
})

describe('the session', () => {
  it('forgets the previous user when other credentials are stored, for example by the classic UI', async () => {
    renderApp('#/')
    expect(await screen.findByTestId('nav-admin')).toBeInTheDocument()
    server.use(http.get('*/api/whoami', () => HttpResponse.json({ name: 'mgmt', tags: ['management'], is_internal_user: true })))
    window.localStorage.setItem('rabbitmq.credentials', btoa('mgmt:mgmt'))
    window.dispatchEvent(new StorageEvent('storage', { key: 'rabbitmq.credentials' }))
    await waitFor(() => expect(screen.getByTestId('current-user')).toHaveTextContent('mgmt'))
  })
})
