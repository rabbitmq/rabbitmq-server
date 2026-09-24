export class ApiError extends Error {
  readonly status: number
  readonly error: string | undefined
  readonly reason: string | undefined

  constructor(status: number, error: string | undefined, reason: string | undefined, path: string) {
    super(reason ?? error ?? `${path} returned ${status}`)
    this.name = 'ApiError'
    this.status = status
    this.error = error
    this.reason = reason
  }
}

/** The credentials were rejected: the session is over. */
export class UnauthorizedError extends ApiError {
  constructor(error: string | undefined, reason: string | undefined, path: string) {
    super(401, error, reason, path)
    this.name = 'UnauthorizedError'
  }
}

/** The credentials are valid but the user lacks a tag or a permission. */
export class ForbiddenError extends ApiError {
  constructor(status: number, error: string | undefined, reason: string | undefined, path: string) {
    super(status, error, reason, path)
    this.name = 'ForbiddenError'
  }
}

export class NotFoundError extends ApiError {
  constructor(error: string | undefined, reason: string | undefined, path: string) {
    super(404, error, reason, path)
    this.name = 'NotFoundError'
  }
}

export function errorMessage(err: unknown): string {
  if (err instanceof ApiError) return err.reason ?? err.message
  if (err instanceof Error) return err.message
  return String(err)
}
