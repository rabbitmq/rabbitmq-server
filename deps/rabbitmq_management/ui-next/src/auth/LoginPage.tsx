import { useState, type FormEvent } from 'react'
import { login } from '../api/resources/overview'
import { errorMessage } from '../api/errors'
import { storeSession } from './session'
import styles from './LoginPage.module.css'

export function LoginPage({ notice }: { notice?: string }) {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState<string | undefined>()
  const [busy, setBusy] = useState(false)

  const submit = async (event: FormEvent) => {
    event.preventDefault()
    setBusy(true)
    setError(undefined)
    try {
      const response = await login(username, password)
      // With `credential_encryption_secret` set, the server returns an encrypted bearer token.
      storeSession(response.token.type === 'bearer' ? 'Bearer' : 'Basic', response.token.value)
    } catch (err) {
      setError(errorMessage(err) === 'Not_Authorized' ? 'Login failed' : errorMessage(err))
      setBusy(false)
    }
  }

  return (
    <main className={styles.page}>
      <form className={styles.card} onSubmit={submit} data-testid="login-form">
        <div className={styles.brand}>
          <span className={styles.logo} aria-hidden="true" />
          <span>RabbitMQ Management</span>
        </div>
        {notice ? <div className="callout callout-warn">{notice}</div> : null}
        <label className={styles.field}>
          <span>Username</span>
          <input
            type="text"
            autoComplete="username"
            value={username}
            onChange={(event) => setUsername(event.target.value)}
            autoFocus
            required
            data-testid="login-username"
          />
        </label>
        <label className={styles.field}>
          <span>Password</span>
          <input
            type="password"
            autoComplete="current-password"
            value={password}
            onChange={(event) => setPassword(event.target.value)}
            required
            data-testid="login-password"
          />
        </label>
        {error ? (
          <div className="callout callout-bad" role="alert" data-testid="login-error">
            {error}
          </div>
        ) : null}
        <button type="submit" className="btn btn-primary" disabled={busy} data-testid="login-submit">
          {busy ? 'Logging in…' : 'Log in'}
        </button>
        <p className="hint">
          This is a preview of the next management UI. OAuth 2 sign-in is available in the <a href="../">classic UI</a>.
        </p>
      </form>
    </main>
  )
}
