import { render } from '@testing-library/react'
import { App } from '../src/app/App'
import { storeSession } from '../src/auth/session'
import { server } from './server'
import { brokerHandlers, type BrokerOptions } from './handlers'

/** Renders the whole application at a hash route against the fixture broker. */
export function renderApp(route: string, options: BrokerOptions & { loggedIn?: boolean } = {}) {
  server.use(...brokerHandlers(options))
  if (options.loggedIn ?? true) storeSession('Basic', btoa('guest:guest'))
  window.location.hash = route
  return render(<App />)
}
