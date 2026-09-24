import { useState, type FormEvent } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  clearUserLimit,
  clearVhostLimit,
  setUserLimit,
  setVhostLimit,
  userLimitsQuery,
  userNamesQuery,
  vhostLimitsQuery,
} from '../../api/resources/admin'
import { useAppData } from '../../app/context'
import { useNotify } from '../../app/notifications'
import { useRefetchInterval } from '../../app/refresh'
import { UserLink, VhostLink } from '../../components/Links'
import { confirmAction, useApiMutation } from '../../components/mutation'
import { PageHeader } from '../../components/PageHeader'
import { Section } from '../../components/Section'
import { ErrorMessage, Loading } from '../../components/Status'

interface LimitRow {
  owner: string
  name: string
  value: number
}

function LimitsTable({ rows, ownerLabel, owner, onClear }: { rows: LimitRow[]; ownerLabel: string; owner: (name: string) => React.ReactNode; onClear?: (row: LimitRow) => void }) {
  if (rows.length === 0) return <p className="muted">No limits</p>
  return (
    <div className="table-wrap">
      <table className="list">
        <thead>
          <tr>
            <th>{ownerLabel}</th>
            <th>Limit</th>
            <th className="num">Value</th>
            {onClear ? <th /> : null}
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={`${row.owner}|${row.name}`}>
              <td>{owner(row.owner)}</td>
              <td>{row.name}</td>
              <td className="num">{row.value}</td>
              {onClear ? (
                <td>
                  <button
                    type="button"
                    className="btn btn-small btn-danger"
                    onClick={() => {
                      if (confirmAction('Are you sure? This object cannot be recovered after deletion.')) onClear(row)
                    }}
                  >
                    Clear
                  </button>
                </td>
              ) : null}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

const flatten = (items: { value: Record<string, number> }[], owner: (item: never) => string): LimitRow[] =>
  items.flatMap((item) => Object.entries(item.value).map(([name, value]) => ({ owner: owner(item as never), name, value })))

export function LimitsPage() {
  const { access } = useAppData()
  const interval = useRefetchInterval('topology')
  const vhostLimits = useQuery({ ...vhostLimitsQuery(), refetchInterval: interval })
  // Non-administrators may only read their own user limits, as in dispatcher.js.
  const userLimits = useQuery({ ...userLimitsQuery(access.isAdministrator ? undefined : access.user.name), refetchInterval: interval })
  const clearVhost = useApiMutation<LimitRow>({ mutationFn: (r) => clearVhostLimit(r.owner, r.name), success: 'Limit cleared', invalidate: [['vhost-limits']] })
  const clearUser = useApiMutation<LimitRow>({ mutationFn: (r) => clearUserLimit(r.owner, r.name), success: 'Limit cleared', invalidate: [['user-limits']] })

  return (
    <>
      <PageHeader title="Limits" documentTitle="Limits" />
      <Section id="limits-vhost" title="Virtual host limits">
        {vhostLimits.error ? <ErrorMessage error={vhostLimits.error} what="virtual host limits" /> : null}
        {vhostLimits.data ? (
          <LimitsTable
            rows={flatten(vhostLimits.data, (l: { vhost: string }) => l.vhost)}
            ownerLabel="Virtual host"
            owner={(name) => (access.isAdministrator ? <VhostLink name={name} /> : name)}
            onClear={access.isAdministrator ? (row) => clearVhost.mutate(row) : undefined}
          />
        ) : (
          <Loading />
        )}
      </Section>
      {access.isAdministrator ? (
        <Section id="limits-vhost-set" title="Set / update a virtual host limit" defaultOpen={false}>
          <SetLimitForm kind="vhost" />
        </Section>
      ) : null}
      <Section id="limits-user" title="User limits">
        {userLimits.error ? <ErrorMessage error={userLimits.error} what="user limits" /> : null}
        {userLimits.data ? (
          <LimitsTable
            rows={flatten(userLimits.data, (l: { user: string }) => l.user)}
            ownerLabel="User"
            owner={(name) => (access.isAdministrator ? <UserLink name={name} /> : name)}
            onClear={access.isAdministrator ? (row) => clearUser.mutate(row) : undefined}
          />
        ) : (
          <Loading />
        )}
      </Section>
      {access.isAdministrator ? (
        <Section id="limits-user-set" title="Set / update a user limit" defaultOpen={false}>
          <SetLimitForm kind="user" />
        </Section>
      ) : null}
    </>
  )
}

const LIMIT_NAMES = { vhost: ['max-connections', 'max-queues'], user: ['max-connections', 'max-channels'] }

function SetLimitForm({ kind }: { kind: 'vhost' | 'user' }) {
  const { vhosts } = useAppData()
  const notify = useNotify()
  const users = useQuery({ ...userNamesQuery(), enabled: kind === 'user' })
  const owners = kind === 'vhost' ? vhosts.map((v) => v.name) : (users.data ?? []).map((u) => u.name)
  const [owner, setOwner] = useState('')
  const [name, setName] = useState(LIMIT_NAMES[kind][0])
  const [value, setValue] = useState('')
  const selected = owner || owners[0] || ''
  const set = useApiMutation<number>({
    mutationFn: (v) => (kind === 'vhost' ? setVhostLimit(selected, name, v) : setUserLimit(selected, name, v)),
    success: 'Limit set',
    invalidate: [[kind === 'vhost' ? 'vhost-limits' : 'user-limits']],
    onSuccess: () => setValue(''),
  })
  const submit = (event: FormEvent) => {
    event.preventDefault()
    const n = parseInt(value, 10)
    if (!/^-?\d+$/.test(value.trim()) || Number.isNaN(n)) return notify('error', 'The limit must be an integer.')
    set.mutate(n)
  }
  return (
    <form className="form" onSubmit={submit} data-testid={`set-${kind}-limit-form`}>
      <label htmlFor={`${kind}-limit-owner`}>{kind === 'vhost' ? 'Virtual host' : 'User'}</label>
      <select id={`${kind}-limit-owner`} value={selected} onChange={(e) => setOwner(e.target.value)}>
        {owners.map((o) => (
          <option key={o} value={o}>
            {o}
          </option>
        ))}
      </select>
      <label htmlFor={`${kind}-limit-name`}>Limit</label>
      <select id={`${kind}-limit-name`} value={name} onChange={(e) => setName(e.target.value)}>
        {LIMIT_NAMES[kind].map((n) => (
          <option key={n} value={n}>
            {n}
          </option>
        ))}
      </select>
      <label htmlFor={`${kind}-limit-value`}>Value</label>
      <input id={`${kind}-limit-value`} type="text" inputMode="numeric" required value={value} onChange={(e) => setValue(e.target.value)} />
      <div className="actions">
        <button type="submit" className="btn btn-primary" disabled={set.isPending || selected === ''}>
          Set / update limit
        </button>
      </div>
    </form>
  )
}
