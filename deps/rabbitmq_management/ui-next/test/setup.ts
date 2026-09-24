import '@testing-library/jest-dom/vitest'
import { afterAll, afterEach, beforeAll } from 'vitest'
import { cleanup } from '@testing-library/react'
import { server } from './server'

beforeAll(() => {
  server.listen({ onUnhandledRequest: 'error' })
  // jsdom does not implement scrolling, which the router's scroll restoration calls.
  window.scrollTo = () => undefined
})
afterEach(() => {
  cleanup()
  server.resetHandlers()
  window.localStorage.clear()
})
afterAll(() => server.close())
