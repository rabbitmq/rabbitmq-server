import { useState, type FormEvent } from 'react'
import { keepPreviousData, useQuery } from '@tanstack/react-query'
import { allPermissionsQuery, putUser, userListQuery } from '../../api/resources/admin'
import type { User } from '../../api/types/admin'
import { useNotify } from '../../app/notifications'
import { useRefetchInterval } from '../../app/refresh'
import type { ColumnSpec } from '../../components/columns'
import { DataTable } from '../../components/DataTable'
import { ListControls } from '../../components/ListControls'
import { useListState } from '../../components/listState'
import { UserLink } from '../../components/Links'
import { useApiMutation } from '../../components/mutation'
import { PageHeader } from '../../components/PageHeader'
import { Section } from '../../components/Section'
import { ErrorMessage } from '../../components/Status'
import { Bool } from '../../components/Values'
import { TagsInput } from './TagsInput'

export function UsersPage() {
  const list = useListState('users')
  const interval = useRefetchInterval('topology')
  const users = useQuery({ ...userListQuery(list.params), refetchInterval: interval, placeholderData: keepPreviousData })
  const permissions = useQuery({ ...allPermissionsQuery(), refetchInterval: interval })

  const columns: ColumnSpec<User>[] = [
    { id: 'name', header: 'Name', sortKey: 'name', cell: (u) => <UserLink name={u.name} /> },
    { id: 'tags', header: 'Tags', sortKey: 'tags', cell: (u) => u.tags.join(', ') },
    {
      id: 'vhosts',
      header: 'Can access virtual hosts',
      cell: (u) => {
        const vhosts = (permissions.data ?? []).filter((p) => p.user === u.name).map((p) => p.vhost)
        if (!permissions.data) return null
        return vhosts.length > 0 ? vhosts.join(', ') : <span style={{ color: 'var(--warn)' }}>No access</span>
      },
    },
    { id: 'password', header: 'Has password', cell: (u) => <Bool value={(u.password_hash ?? '').length > 0} /> },
  ]

  return (
    <>
      <PageHeader title="Users" documentTitle="Users" />
      <Section id="users-list" title="All users">
        <ListControls state={list} page={users.data} noun="users" error={users.error} />
        {users.error ? <ErrorMessage error={users.error} what="users" /> : null}
        <DataTable
          mode="users"
          columns={columns}
          rows={users.data?.items ?? []}
          rowKey={(u) => u.name}
          sort={list.params}
          onSortChange={(sort) => list.update({ ...sort, page: 1 })}
          empty={users.isPending ? 'Loading…' : 'No users'}
        />
        <p className="hint">Only users in the internal database are listed.</p>
      </Section>
      <Section id="users-add" title="Add a user" defaultOpen={false}>
        <AddUserForm />
      </Section>
    </>
  )
}

function AddUserForm() {
  const notify = useNotify()
  const [name, setName] = useState('')
  const [hasPassword, setHasPassword] = useState(true)
  const [password, setPassword] = useState('')
  const [confirm, setConfirm] = useState('')
  const [tags, setTags] = useState('')
  const add = useApiMutation<void, boolean>({
    mutationFn: () => putUser(name, hasPassword ? { tags, password } : { tags }),
    invalidate: [['users'], ['permissions']],
    onSuccess: (created) => {
      notify(created ? 'success' : 'info', created ? `User ${name} added` : `Updated an existing user: '${name}'`)
      setName('')
      setPassword('')
      setConfirm('')
    },
  })
  const submit = (event: FormEvent) => {
    event.preventDefault()
    if (hasPassword && password === '') return notify('error', 'Please specify a password.')
    if (hasPassword && password !== confirm) return notify('error', 'Passwords do not match.')
    add.mutate()
  }
  return (
    <form className="form" onSubmit={submit} data-testid="add-user-form">
      <label htmlFor="user-name">Username</label>
      <input id="user-name" type="text" required value={name} onChange={(e) => setName(e.target.value)} data-testid="user-name" />
      <select aria-label="Password" value={hasPassword ? 'password' : 'none'} onChange={(e) => setHasPassword(e.target.value === 'password')} style={{ justifySelf: 'end' }}>
        <option value="password">Password</option>
        <option value="none">No password</option>
      </select>
      {hasPassword ? (
        <div className="row">
          <input type="password" aria-label="Password" value={password} onChange={(e) => setPassword(e.target.value)} autoComplete="new-password" data-testid="user-password" />
          <input type="password" aria-label="Confirm password" placeholder="confirm" value={confirm} onChange={(e) => setConfirm(e.target.value)} autoComplete="new-password" data-testid="user-password-confirm" />
        </div>
      ) : (
        <span className="muted">The user cannot log in using a password.</span>
      )}
      <label htmlFor="user-tags">Tags</label>
      <TagsInput id="user-tags" value={tags} onChange={setTags} />
      <div className="actions">
        <button type="submit" className="btn btn-primary" disabled={add.isPending} data-testid="add-user-submit">
          Add user
        </button>
      </div>
    </form>
  )
}
