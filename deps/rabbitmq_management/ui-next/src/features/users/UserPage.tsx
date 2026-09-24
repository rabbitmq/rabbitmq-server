import { useState, type FormEvent } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useNavigate, useParams } from '@tanstack/react-router'
import { deleteUser, permissionsQuery, putUser, userQuery } from '../../api/resources/admin'
import { withSessionChecksSuspended } from '../../api/client'
import { login } from '../../api/resources/overview'
import type { User } from '../../api/types/admin'
import { storeSession } from '../../auth/session'
import { useAccess } from '../../app/context'
import { useNotify } from '../../app/notifications'
import { useRefetchInterval } from '../../app/refresh'
import { confirmAction, useApiMutation } from '../../components/mutation'
import { PageHeader } from '../../components/PageHeader'
import { Section } from '../../components/Section'
import { ErrorMessage, Loading } from '../../components/Status'
import { Bool, Facts } from '../../components/Values'
import { PermissionsPanel, TopicPermissionsPanel } from '../shared/Permissions'
import { TagsInput } from './TagsInput'

export function UserPage() {
  const { name } = useParams({ from: '/users/$name' })
  const user = useQuery({ ...userQuery(name), refetchInterval: useRefetchInterval('topology') })
  const permissions = useQuery(permissionsQuery({ user: name }))
  const header = <PageHeader kind="User" title={name} documentTitle={`User ${name}`} />
  if (!user.data) return <>{header}{user.error ? <ErrorMessage error={user.error} what={`User ${name}`} /> : <Loading />}</>
  const u = user.data
  return (
    <>
      {header}
      {permissions.data?.length === 0 ? (
        <div className="callout callout-warn" data-testid="user-no-access">
          This user does not have permission to access any virtual hosts. Use "Set permission" below to grant access.
        </div>
      ) : null}
      <Section id="user-overview" title="Overview">
        <Facts
          rows={[
            ['Tags', u.tags.join(', ')],
            ['Can log in with password', <Bool value={(u.password_hash ?? '').length > 0} key="p" />],
          ]}
        />
      </Section>
      <Section id="user-permissions" title="Permissions">
        <PermissionsPanel scope={{ user: name }} />
      </Section>
      <Section id="user-topic-permissions" title="Topic permissions">
        <TopicPermissionsPanel scope={{ user: name }} />
      </Section>
      <Section id="user-update" title="Update this user" defaultOpen={false}>
        <UpdateUserForm user={u} />
      </Section>
      <Section id="user-delete" title="Delete this user" defaultOpen={false}>
        <DeleteUser name={name} />
      </Section>
    </>
  )
}

type PasswordMode = 'keep' | 'new' | 'none'

function UpdateUserForm({ user }: { user: User }) {
  const notify = useNotify()
  const access = useAccess()
  const navigate = useNavigate()
  const hasPassword = (user.password_hash ?? '').length > 0
  const [mode, setMode] = useState<PasswordMode>(hasPassword ? 'keep' : 'none')
  const [password, setPassword] = useState('')
  const [confirm, setConfirm] = useState('')
  const [tags, setTags] = useState(user.tags.join(','))
  const own = user.name === access.user.name
  const ownPasswordChange = mode === 'new' && own

  const update = useApiMutation({
    mutationFn: async () => {
      // Updating a user without a password or hash clears the password, so
      // keeping it means sending the current hash back.
      const body =
        mode === 'new'
          ? { tags, password }
          : mode === 'keep'
            ? { tags, password_hash: user.password_hash, hashing_algorithm: user.hashing_algorithm }
            : { tags }
      if (!ownPasswordChange) return putUser(user.name, body)
      // Requests still in flight with the old password will be rejected; that
      // must not end the session that the new login replaces.
      await withSessionChecksSuspended(async () => {
        await putUser(user.name, body)
        const response = await login(user.name, password)
        storeSession(response.token.type === 'bearer' ? 'Bearer' : 'Basic', response.token.value)
      })
    },
    success: `User ${user.name} updated`,
    invalidate: own ? [['users'], ['whoami']] : [['users']],
    onSuccess: () => navigate({ to: '/users' }),
  })

  const submit = (event: FormEvent) => {
    event.preventDefault()
    if (mode === 'new' && password === '') return notify('error', 'Please specify a password.')
    if (mode === 'new' && password !== confirm) return notify('error', 'Passwords do not match.')
    update.mutate()
  }

  return (
    <form className="form" onSubmit={submit} data-testid="update-user-form">
      <label htmlFor="update-password-mode">Password</label>
      <select id="update-password-mode" value={mode} onChange={(e) => setMode(e.target.value as PasswordMode)}>
        {hasPassword ? <option value="keep">Keep the current password</option> : null}
        <option value="new">Set a new password</option>
        <option value="none">No password</option>
      </select>
      {mode === 'new' ? (
        <>
          <span className="label">New password</span>
          <div className="row">
            <input type="password" aria-label="New password" value={password} onChange={(e) => setPassword(e.target.value)} autoComplete="new-password" />
            <input type="password" aria-label="Confirm new password" placeholder="confirm" value={confirm} onChange={(e) => setConfirm(e.target.value)} autoComplete="new-password" />
          </div>
        </>
      ) : null}
      {mode === 'none' ? <span className="full hint">The user will not be able to log in using a password.</span> : null}
      <label htmlFor="update-tags">Tags</label>
      <TagsInput id="update-tags" value={tags} onChange={setTags} />
      <div className="actions">
        <button type="submit" className="btn btn-primary" disabled={update.isPending} data-testid="update-user-submit">
          Update user
        </button>
      </div>
    </form>
  )
}

function DeleteUser({ name }: { name: string }) {
  const navigate = useNavigate()
  const remove = useApiMutation({
    mutationFn: () => deleteUser(name),
    success: `User ${name} deleted`,
    invalidate: [['users'], ['permissions']],
    onSuccess: () => navigate({ to: '/users' }),
  })
  return (
    <button
      type="button"
      className="btn btn-danger"
      disabled={remove.isPending}
      onClick={() => {
        if (confirmAction('Are you sure? This object cannot be recovered after deletion.')) remove.mutate()
      }}
      data-testid="delete-user"
    >
      Delete
    </button>
  )
}
