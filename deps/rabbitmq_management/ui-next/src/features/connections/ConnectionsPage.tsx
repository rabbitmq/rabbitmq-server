import { keepPreviousData, useQuery } from '@tanstack/react-query'
import { connectionListQuery } from '../../api/resources/connections'
import { nodesQuery } from '../../api/resources/overview'
import type { Connection } from '../../api/types/connections'
import { useAppData, type StatsMode } from '../../app/context'
import { useRefetchInterval } from '../../app/refresh'
import { useSelectedVhost } from '../../app/vhost'
import { ColumnChooser } from '../../components/ColumnChooser'
import type { ColumnSpec } from '../../components/columns'
import { DataTable } from '../../components/DataTable'
import { ListControls } from '../../components/ListControls'
import { useListState } from '../../components/listState'
import { ConnectionLink } from '../../components/Links'
import { PageHeader } from '../../components/PageHeader'
import { Section } from '../../components/Section'
import { ObjectState } from '../../components/State'
import { ErrorMessage } from '../../components/Status'
import { Bool, Rate, Timestamp } from '../../components/Values'
import { clientName, shortConn } from '../../format/names'

function connectionColumns(stats: StatsMode, opts: { showVhost: boolean; showNode: boolean }): ColumnSpec<Connection>[] {
  const optional = (id: string, label: string, defaultVisible: boolean) => ({ id, optional: { label, defaultVisible } })
  const columns: ColumnSpec<Connection>[] = []
  if (opts.showVhost) columns.push({ id: 'vhost', group: 'Overview', header: 'Virtual host', sortKey: 'vhost', cell: (c) => c.vhost })
  columns.push({
    id: 'name',
    group: 'Overview',
    header: 'Name',
    sortKey: 'name',
    cell: (c) => {
      const provided = c.client_properties?.connection_name
      return (
        <>
          <ConnectionLink name={c.name} />
          {typeof provided === 'string' ? <div className="hint">{shortConn(provided)}</div> : null}
        </>
      )
    },
  })
  if (opts.showNode) columns.push({ id: 'node', group: 'Overview', header: 'Node', sortKey: 'node', cell: (c) => c.node })
  columns.push(
    { ...optional('container_id', 'Container ID', true), group: 'Overview', header: 'Container ID', sortKey: 'container_id', cell: (c) => c.container_id },
    { ...optional('user', 'User name', true), group: 'Overview', header: 'User name', sortKey: 'user', cell: (c) => c.user },
  )
  if (!stats.disabled) columns.push({ ...optional('state', 'State', true), group: 'Overview', header: 'State', sortKey: 'state', cell: (c) => <ObjectState obj={c} /> })
  columns.push(
    { ...optional('ssl', 'TLS', true), group: 'Details', header: 'TLS', sortKey: 'ssl', cell: (c) => <Bool value={c.ssl} /> },
    {
      ...optional('ssl_info', 'TLS details', false),
      group: 'Details',
      header: 'TLS details',
      cell: (c) =>
        c.ssl ? (
          <>
            {c.ssl_protocol}
            <div className="hint">
              {c.ssl_key_exchange} {c.ssl_cipher} {c.ssl_hash}
            </div>
          </>
        ) : null,
    },
    { ...optional('protocol', 'Protocol', true), group: 'Details', header: 'Protocol', sortKey: 'protocol', cell: (c) => c.protocol },
  )
  if (!stats.disabled) {
    columns.push({ ...optional('channels', 'Channels', true), group: 'Details', header: 'Channels', sortKey: 'channels', numeric: true, cell: (c) => c.channels })
  }
  columns.push(
    { ...optional('channel_max', 'Channel max', false), group: 'Details', header: 'Channel max', sortKey: 'channel_max', numeric: true, cell: (c) => c.channel_max },
    { ...optional('frame_max', 'Frame max', false), group: 'Details', header: 'Frame max', sortKey: 'frame_max', numeric: true, cell: (c) => c.frame_max },
    { ...optional('auth_mechanism', 'SASL auth mechanism', false), group: 'Details', header: 'Auth mechanism', sortKey: 'auth_mechanism', cell: (c) => c.auth_mechanism },
    {
      ...optional('client', 'Client', false),
      group: 'Details',
      header: 'Client',
      cell: (c) => {
        const client = clientName(c.client_properties)
        return client ? (
          <>
            {client.name}
            {client.version ? <div className="hint">{client.version}</div> : null}
          </>
        ) : null
      },
    },
  )
  if (!stats.disabled) {
    columns.push(
      { ...optional('from_client', 'From client', true), group: 'Network', header: 'From client', sortKey: 'recv_oct_details.rate', numeric: true, cell: (c) => <Rate details={c.recv_oct_details} bytes /> },
      { ...optional('to_client', 'To client', true), group: 'Network', header: 'To client', sortKey: 'send_oct_details.rate', numeric: true, cell: (c) => <Rate details={c.send_oct_details} bytes /> },
    )
  }
  columns.push(
    { ...optional('heartbeat', 'Heartbeat', false), group: 'Network', header: 'Heartbeat', sortKey: 'timeout', numeric: true, cell: (c) => (c.timeout !== undefined ? `${c.timeout}s` : null) },
    { ...optional('connected_at', 'Connected at', false), group: 'Network', header: 'Connected at', sortKey: 'connected_at', cell: (c) => <Timestamp value={c.connected_at} /> },
  )
  return columns
}

export function ConnectionsPage() {
  const { stats, access, vhosts } = useAppData()
  const [vhost] = useSelectedVhost()
  const list = useListState('connections')
  const nodes = useQuery({ ...nodesQuery(), enabled: access.isMonitoring, staleTime: 60_000 })
  const columns = connectionColumns(stats, { showVhost: vhosts.length > 1 && vhost === '', showNode: (nodes.data?.length ?? 0) > 1 })
  const connections = useQuery({ ...connectionListQuery(vhost, list.params), refetchInterval: useRefetchInterval('stats'), placeholderData: keepPreviousData })
  return (
    <>
      <PageHeader title="Connections" documentTitle="Connections" />
      <Section id="connections-list" title="All connections" actions={<ColumnChooser mode="connections" columns={columns} />}>
        <ListControls state={list} page={connections.data} noun="connections" error={connections.error} />
        {connections.error ? <ErrorMessage error={connections.error} what="connections" /> : null}
        <DataTable
          mode="connections"
          columns={columns}
          rows={connections.data?.items ?? []}
          rowKey={(c) => c.name}
          sort={list.params}
          onSortChange={(sort) => list.update({ ...sort, page: 1 })}
          empty={connections.isPending ? 'Loading…' : 'No connections'}
        />
      </Section>
    </>
  )
}
