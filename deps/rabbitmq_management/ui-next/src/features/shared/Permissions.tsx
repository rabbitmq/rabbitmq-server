import { useState, type FormEvent } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  clearPermission,
  clearTopicPermission,
  permissionsQuery,
  setPermission,
  setTopicPermission,
  topicPermissionsQuery,
  userNamesQuery,
} from '../../api/resources/admin'
import { exchangeNamesQuery } from '../../api/resources/exchanges'
import { useAppData } from '../../app/context'
import { useRefetchInterval } from '../../app/refresh'
import { UserLink, VhostLink } from '../../components/Links'
import { confirmAction, useApiMutation } from '../../components/mutation'
import { ErrorMessage, Loading } from '../../components/Status'
import { exchangeName } from '../../format/names'

type Scope = { user: string } | { vhost: string }

export function PermissionsPanel({ scope }: { scope: Scope }) {
  const byUser = 'user' in scope
  const permissions = useQuery({ ...permissionsQuery(scope), refetchInterval: useRefetchInterval('topology') })
  const clear = useApiMutation<{ vhost: string; user: string }>({
    mutationFn: ({ vhost, user }) => clearPermission(vhost, user),
    success: 'Permission cleared',
    invalidate: [['permissions']],
  })
  return (
    <div className="stack">
      <h3>Current permissions</h3>
      {permissions.error ? <ErrorMessage error={permissions.error} what="permissions" /> : null}
      {!permissions.data ? (
        <Loading />
      ) : permissions.data.length === 0 ? (
        <p className="muted">No permissions</p>
      ) : (
        <div className="table-wrap">
          <table className="list" data-testid="permissions-table">
            <thead>
              <tr>
                <th>{byUser ? 'Virtual host' : 'User'}</th>
                <th>Configure regexp</th>
                <th>Write regexp</th>
                <th>Read regexp</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {permissions.data.map((p) => (
                <tr key={`${p.user}|${p.vhost}`}>
                  <td>{byUser ? <VhostLink name={p.vhost} /> : <UserLink name={p.user} />}</td>
                  <td className="mono">{p.configure}</td>
                  <td className="mono">{p.write}</td>
                  <td className="mono">{p.read}</td>
                  <td>
                    <button
                      type="button"
                      className="btn btn-small btn-danger"
                      onClick={() => {
                        if (confirmAction('Are you sure? This object cannot be recovered after deletion.')) clear.mutate({ vhost: p.vhost, user: p.user })
                      }}
                    >
                      Clear
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <h3>Set permission</h3>
      <SetPermissionForm scope={scope} />
    </div>
  )
}

function useTargets(scope: Scope) {
  const { vhosts } = useAppData()
  const users = useQuery({ ...userNamesQuery(), enabled: 'vhost' in scope })
  return 'user' in scope ? vhosts.map((v) => v.name) : (users.data ?? []).map((u) => u.name)
}

function SetPermissionForm({ scope }: { scope: Scope }) {
  const byUser = 'user' in scope
  const targets = useTargets(scope)
  const [target, setTarget] = useState('')
  const [configure, setConfigure] = useState('.*')
  const [write, setWrite] = useState('.*')
  const [read, setRead] = useState('.*')
  const selected = target || targets[0] || ''
  const set = useApiMutation({
    mutationFn: () => (byUser ? setPermission(selected, scope.user, { configure, write, read }) : setPermission(scope.vhost, selected, { configure, write, read })),
    success: 'Permission set',
    invalidate: [['permissions']],
  })
  const submit = (event: FormEvent) => {
    event.preventDefault()
    set.mutate()
  }
  return (
    <form className="form" onSubmit={submit} data-testid="set-permission-form">
      <label htmlFor="perm-target">{byUser ? 'Virtual host' : 'User'}</label>
      <select id="perm-target" value={selected} onChange={(e) => setTarget(e.target.value)}>
        {targets.map((t) => (
          <option key={t} value={t}>
            {t}
          </option>
        ))}
      </select>
      <label htmlFor="perm-configure">Configure regexp</label>
      <input id="perm-configure" type="text" value={configure} onChange={(e) => setConfigure(e.target.value)} />
      <label htmlFor="perm-write">Write regexp</label>
      <input id="perm-write" type="text" value={write} onChange={(e) => setWrite(e.target.value)} />
      <label htmlFor="perm-read">Read regexp</label>
      <input id="perm-read" type="text" value={read} onChange={(e) => setRead(e.target.value)} />
      <div className="actions">
        <button type="submit" className="btn btn-primary" disabled={set.isPending || selected === ''} data-testid="set-permission-submit">
          Set permission
        </button>
      </div>
    </form>
  )
}

export function TopicPermissionsPanel({ scope }: { scope: Scope }) {
  const byUser = 'user' in scope
  const permissions = useQuery({ ...topicPermissionsQuery(scope), refetchInterval: useRefetchInterval('topology') })
  const clear = useApiMutation<{ vhost: string; user: string; exchange: string }>({
    mutationFn: ({ vhost, user, exchange }) => clearTopicPermission(vhost, user, exchange),
    success: 'Topic permission cleared',
    invalidate: [['topic-permissions']],
  })
  return (
    <div className="stack">
      <h3>Current topic permissions</h3>
      {permissions.error ? <ErrorMessage error={permissions.error} what="topic permissions" /> : null}
      {!permissions.data ? (
        <Loading />
      ) : permissions.data.length === 0 ? (
        <p className="muted">No topic permissions</p>
      ) : (
        <div className="table-wrap">
          <table className="list" data-testid="topic-permissions-table">
            <thead>
              <tr>
                <th>{byUser ? 'Virtual host' : 'User'}</th>
                <th>Exchange</th>
                <th>Write regexp</th>
                <th>Read regexp</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {permissions.data.map((p) => (
                <tr key={`${p.user}|${p.vhost}|${p.exchange}`}>
                  <td>{byUser ? <VhostLink name={p.vhost} /> : <UserLink name={p.user} />}</td>
                  <td>{exchangeName(p.exchange)}</td>
                  <td className="mono">{p.write}</td>
                  <td className="mono">{p.read}</td>
                  <td>
                    <button
                      type="button"
                      className="btn btn-small btn-danger"
                      onClick={() => {
                        if (confirmAction('Are you sure? This object cannot be recovered after deletion.')) {
                          clear.mutate({ vhost: p.vhost, user: p.user, exchange: p.exchange })
                        }
                      }}
                    >
                      Clear
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <h3>Set topic permission</h3>
      <SetTopicPermissionForm scope={scope} />
    </div>
  )
}

function SetTopicPermissionForm({ scope }: { scope: Scope }) {
  const byUser = 'user' in scope
  const targets = useTargets(scope)
  const [target, setTarget] = useState('')
  const selected = target || targets[0] || ''
  const vhost = byUser ? selected : scope.vhost
  const exchanges = useQuery({ ...exchangeNamesQuery(vhost), enabled: vhost !== '' })
  const topicExchanges = (exchanges.data ?? []).filter((x) => x.type === 'topic')
  const [exchange, setExchange] = useState('')
  const [write, setWrite] = useState('.*')
  const [read, setRead] = useState('.*')
  const selectedExchange = exchange || topicExchanges[0]?.name || ''
  const set = useApiMutation({
    mutationFn: () => setTopicPermission(vhost, byUser ? scope.user : selected, { exchange: selectedExchange, write, read }),
    success: 'Topic permission set',
    invalidate: [['topic-permissions']],
  })
  return (
    <form
      className="form"
      onSubmit={(event) => {
        event.preventDefault()
        set.mutate()
      }}
      data-testid="set-topic-permission-form"
    >
      <label htmlFor="tperm-target">{byUser ? 'Virtual host' : 'User'}</label>
      <select id="tperm-target" value={selected} onChange={(e) => { setTarget(e.target.value); setExchange('') }}>
        {targets.map((t) => (
          <option key={t} value={t}>
            {t}
          </option>
        ))}
      </select>
      <label htmlFor="tperm-exchange">Exchange</label>
      <select id="tperm-exchange" value={selectedExchange} onChange={(e) => setExchange(e.target.value)}>
        {topicExchanges.map((x) => (
          <option key={x.name} value={x.name}>
            {exchangeName(x.name)}
          </option>
        ))}
      </select>
      <label htmlFor="tperm-write">Write regexp</label>
      <input id="tperm-write" type="text" value={write} onChange={(e) => setWrite(e.target.value)} />
      <label htmlFor="tperm-read">Read regexp</label>
      <input id="tperm-read" type="text" value={read} onChange={(e) => setRead(e.target.value)} />
      <div className="actions">
        <button type="submit" className="btn btn-primary" disabled={set.isPending || selected === '' || selectedExchange === ''}>
          Set topic permission
        </button>
      </div>
    </form>
  )
}
