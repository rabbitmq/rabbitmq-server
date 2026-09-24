import type { ReactNode } from 'react'
import { ApiError, ForbiddenError, NotFoundError, errorMessage } from '../api/errors'

export function Loading({ label = 'Loading…' }: { label?: string }) {
  return (
    <div className="empty" role="status" data-testid="loading">
      {label}
    </div>
  )
}

export function ErrorMessage({ error, what }: { error: unknown; what?: string }) {
  if (error instanceof ForbiddenError) {
    return (
      <div className="callout callout-warn" data-testid="error-forbidden">
        {what ? `${what}: ` : ''}not permitted for this user. {error.reason ? `(${error.reason})` : null}
      </div>
    )
  }
  if (error instanceof NotFoundError) {
    return (
      <div className="callout callout-warn" data-testid="error-not-found">
        {what ?? 'The object'} does not exist.
      </div>
    )
  }
  return (
    <div className="callout callout-bad" data-testid="error">
      {what ? `Could not load ${what}: ` : ''}
      {errorMessage(error)}
      {error instanceof ApiError ? <span className="muted"> (HTTP {error.status})</span> : null}
    </div>
  )
}

export function EmptyState({ children }: { children: ReactNode }) {
  return (
    <div className="empty" data-testid="empty">
      {children}
    </div>
  )
}

export function QueryState<T>({
  query,
  what,
  children,
}: {
  query: { data: T | undefined; error: unknown; isPending: boolean }
  what?: string
  children: (data: T) => ReactNode
}) {
  if (query.data !== undefined) {
    return (
      <>
        {query.error ? <ErrorMessage error={query.error} what={what} /> : null}
        {children(query.data)}
      </>
    )
  }
  if (query.error) return <ErrorMessage error={query.error} what={what} />
  return <Loading />
}
