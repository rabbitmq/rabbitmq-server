import { useState, type FormEvent } from 'react'
import { keepPreviousData, useQuery } from '@tanstack/react-query'
import { allPermissionsQuery, declareVhost, restartVhost, vhostListQuery } from '../../api/resources/admin'
import type { Vhost } from '../../api/types/admin'
import { useAppData, type StatsMode } from '../../app/context'
import { useRefetchInterval } from '../../app/refresh'
import { ColumnChooser } from '../../components/ColumnChooser'
import type { ColumnSpec } from '../../components/columns'
import { DataTable } from '../../components/DataTable'
import { ListControls } from '../../components/ListControls'
import { useListState } from '../../components/listState'
import { VhostLink } from '../../components/Links'
import { confirmAction, useApiMutation } from '../../components/mutation'
import { PageHeader } from '../../components/PageHeader'
import { Section } from '../../components/Section'
import { VhostState } from '../../components/State'
import { ErrorMessage } from '../../components/Status'
import { Num, Rate } from '../../components/Values'

export function RestartVhost({ vhost, node }: { vhost: string; node: string }) {
  const restart = useApiMutation({
    mutationFn: () => restartVhost(vhost, node),
    success: `Virtual host ${vhost} restarted on ${node}`,
    invalidate: [['vhosts']],
  })
  return (
    <button
      type="button"
      className="btn btn-small"
      disabled={restart.isPending}
      onClick={() => {
        if (confirmAction(`Restart virtual host ${vhost} on ${node}?`)) restart.mutate()
      }}
    >
      Restart
    </button>
  )
}

function vhostColumns(stats: StatsMode, usersOf: (vhost: string) => string[] | undefined): ColumnSpec<Vhost>[] {
  const optional = (id: string, label: string, defaultVisible: boolean) => ({ id, optional: { label, defaultVisible } })
  const columns: ColumnSpec<Vhost>[] = [
    { id: 'name', group: 'Overview', header: 'Name', sortKey: 'name', cell: (v) => <VhostLink name={v.name} /> },
    {
      id: 'users',
      group: 'Overview',
      header: 'Users',
      cell: (v) => {
        const users = usersOf(v.name)
        if (!users) return null
        return users.length > 0 ? users.join(', ') : <span style={{ color: 'var(--warn)' }}>No users</span>
      },
    },
    { id: 'state', group: 'Overview', header: 'State', cell: (v) => <VhostState clusterState={v.cluster_state} /> },
    {
      ...optional('default-queue-type', 'Default queue type', false),
      group: 'Overview',
      header: 'Default queue type',
      cell: (v) => (v.default_queue_type && v.default_queue_type !== 'undefined' ? v.default_queue_type : <span className="muted">&lt;not set&gt;</span>),
    },
    {
      ...optional('cluster-state', 'Cluster state', false),
      group: 'Overview',
      header: 'Cluster state',
      cell: (v) => (
        <table className="facts">
          <tbody>
            {Object.entries(v.cluster_state ?? {}).map(([node, state]) => (
              <tr key={node}>
                <th>{node}</th>
                <td>
                  {state} {state === 'stopped' ? <RestartVhost vhost={v.name} node={node} /> : null}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      ),
    },
    { ...optional('description', 'Description', false), group: 'Overview', header: 'Description', cell: (v) => v.description },
    { ...optional('tags', 'Tags', false), group: 'Overview', header: 'Tags', cell: (v) => (v.tags ?? []).join(', ') },
  ]
  if (stats.disabled) return columns
  const msgs = (id: string, label: string, key: 'messages_ready' | 'messages_unacknowledged' | 'messages'): ColumnSpec<Vhost> => ({
    ...optional(id, label, true),
    group: 'Messages',
    header: label === 'Unacknowledged' ? 'Unacked' : label,
    sortKey: key,
    numeric: true,
    cell: (v) => <Num value={v[key]} />,
  })
  columns.push(
    msgs('msgs-ready', 'Ready', 'messages_ready'),
    msgs('msgs-unacked', 'Unacknowledged', 'messages_unacknowledged'),
    msgs('msgs-total', 'Total', 'messages'),
    { ...optional('from_client', 'From client', true), group: 'Network', header: 'From client', sortKey: 'recv_oct_details.rate', numeric: true, cell: (v) => <Rate details={v.recv_oct_details} bytes /> },
    { ...optional('to_client', 'To client', true), group: 'Network', header: 'To client', sortKey: 'send_oct_details.rate', numeric: true, cell: (v) => <Rate details={v.send_oct_details} bytes /> },
  )
  if (!stats.hasRates) return columns
  columns.push(
    {
      ...optional('rate-publish', 'publish', true),
      group: 'Message rates',
      header: 'publish',
      sortKey: 'message_stats.publish_details.rate',
      numeric: true,
      cell: (v) => <Rate details={v.message_stats?.publish_details} />,
    },
    {
      ...optional('rate-deliver', 'deliver / get', true),
      group: 'Message rates',
      header: 'deliver / get',
      sortKey: 'message_stats.deliver_get_details.rate',
      numeric: true,
      cell: (v) => <Rate details={v.message_stats?.deliver_get_details} />,
    },
  )
  return columns
}

export function VhostsPage() {
  const { stats } = useAppData()
  const list = useListState('vhosts')
  const interval = useRefetchInterval('stats')
  const vhosts = useQuery({ ...vhostListQuery(list.params), refetchInterval: interval, placeholderData: keepPreviousData })
  const permissions = useQuery({ ...allPermissionsQuery(), refetchInterval: useRefetchInterval('topology') })
  const usersOf = (vhost: string) => permissions.data?.filter((p) => p.vhost === vhost).map((p) => p.user)
  const columns = vhostColumns(stats, usersOf)
  return (
    <>
      <PageHeader title="Virtual Hosts" documentTitle="Virtual Hosts" />
      <Section id="vhosts-list" title="All virtual hosts" actions={<ColumnChooser mode="vhosts" columns={columns} />}>
        <ListControls state={list} page={vhosts.data} noun="virtual hosts" error={vhosts.error} />
        {vhosts.error ? <ErrorMessage error={vhosts.error} what="virtual hosts" /> : null}
        <DataTable
          mode="vhosts"
          columns={columns}
          rows={vhosts.data?.items ?? []}
          rowKey={(v) => v.name}
          sort={list.params}
          onSortChange={(sort) => list.update({ ...sort, page: 1 })}
          empty={vhosts.isPending ? 'Loading…' : 'No virtual hosts'}
        />
      </Section>
      <Section id="vhosts-add" title="Add a new virtual host" defaultOpen={false}>
        <AddVhostForm />
      </Section>
    </>
  )
}

function AddVhostForm() {
  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [tags, setTags] = useState('')
  const [defaultQueueType, setDefaultQueueType] = useState('classic')
  const add = useApiMutation({
    mutationFn: () => declareVhost(name, { description, tags, default_queue_type: defaultQueueType }),
    success: `Virtual host ${name} added`,
    invalidate: [['vhosts'], ['permissions']],
    onSuccess: () => {
      setName('')
      setDescription('')
      setTags('')
    },
  })
  const submit = (event: FormEvent) => {
    event.preventDefault()
    add.mutate()
  }
  return (
    <form className="form" onSubmit={submit} data-testid="add-vhost-form">
      <label htmlFor="vhost-name">Name</label>
      <input id="vhost-name" type="text" required value={name} onChange={(e) => setName(e.target.value)} data-testid="vhost-name" />
      <label htmlFor="vhost-description">Description</label>
      <input id="vhost-description" type="text" value={description} onChange={(e) => setDescription(e.target.value)} />
      <label htmlFor="vhost-tags">Tags</label>
      <input id="vhost-tags" type="text" value={tags} onChange={(e) => setTags(e.target.value)} placeholder="comma-separated" />
      <label htmlFor="vhost-dqt">Default queue type</label>
      <select id="vhost-dqt" value={defaultQueueType} onChange={(e) => setDefaultQueueType(e.target.value)}>
        <option value="classic">Classic</option>
        <option value="quorum">Quorum</option>
        <option value="stream">Stream</option>
      </select>
      <div className="actions">
        <button type="submit" className="btn btn-primary" disabled={add.isPending} data-testid="add-vhost-submit">
          Add virtual host
        </button>
      </div>
    </form>
  )
}
