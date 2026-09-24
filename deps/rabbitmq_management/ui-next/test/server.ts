import { setupServer } from 'msw/node'

/** Tests register their own handlers with `server.use`; an unhandled request fails the test. */
export const server = setupServer()
