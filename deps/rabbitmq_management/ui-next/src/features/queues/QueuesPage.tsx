import { useState, type FormEvent } from 'react'
import { keepPreviousData, useQuery } from '@tanstack/react-query'
import { useNavigate } from '@tanstack/react-router'
import { declareQueue, queueListQuery } from '../../api/resources/queues'
import { nodesQuery } from '../../api/resources/overview'
import type { Queue } from '../../api/types/queues'
import { useAppData, type StatsMode } from '../../app/context'
import { useRefetchInterval } from '../../app/refresh'
import { useSelectedVhost } from '../../app/vhost'
import { ArgumentsEditor, rowsToTable, type ArgRow } from '../../components/ArgumentsEditor'
import { ColumnChooser } from '../../components/ColumnChooser'
import { columnsParam, isColumnVisible, type ColumnSpec } from '../../components/columns'
import { DataTable } from '../../components/DataTable'
import { ListControls } from '../../components/ListControls'
import { useListState } from '../../components/listState'
import { PolicyLink, QueueLink } from '../../components/Links'
import { useApiMutation } from '../../components/mutation'
import { PageHeader } from '../../components/PageHeader'
import { Section } from '../../components/Section'
import { ObjectState } from '../../components/State'
import { ErrorMessage } from '../../components/Status'
import { Bytes, FeatureTags, Num, PolicyTags, Rate, Trend } from '../../components/Values'
import { fmtPercent } from '../../format/numbers'
import { usePrefsVersion } from '../../prefs/storage'
import { useNotify } from '../../app/notifications'
import { errorMessage } from '../../api/errors'
import { QUEUE_TYPES } from './queueTypes'

function Members({ queue }: { queue: Queue }) {
  const node = queue.node ?? queue.leader
  if (!queue.members) return <>{node}</>
  const followers = (queue.online ?? []).filter((member) => member !== node)
  const majority = (queue.online?.length ?? 0) >= Math.floor(queue.members.length / 2) + 1
  return (
    <>
      {node}{' '}
      <abbr
        className="tag"
        style={majority ? undefined : { color: 'var(--bad)' }}
        title={majority ? `Followers: ${followers.join(', ')}` : 'Cluster is in minority'}
      >
        +{followers.length}
      </abbr>
    </>
  )
}

/** The columns of queues.ejs, with the classic UI's column keys. */
function queueColumns(stats: StatsMode, showVhost: boolean, showNode: boolean): ColumnSpec<Queue>[] {
  const columns: ColumnSpec<Queue>[] = []
  if (showVhost) columns.push({ id: 'vhost', group: 'Overview', header: 'Virtual host', sortKey: 'vhost', cell: (q) => q.vhost })
  columns.push({ id: 'name', group: 'Overview', header: 'Name', sortKey: 'name', cell: (q) => <QueueLink vhost={q.vhost} name={q.name} /> })
  if (showNode) {
    columns.push({ id: 'node', group: 'Overview', header: 'Node', sortKey: 'node', fields: ['node', 'leader', 'members', 'online'], cell: (q) => <Members queue={q} /> })
  }
  const optional = (id: string, label: string, defaultVisible: boolean) => ({ id, optional: { label, defaultVisible } })
  columns.push(
    { ...optional('type', 'Type', true), group: 'Overview', header: 'Type', sortKey: 'type', cell: (q) => q.type },
    {
      ...optional('features', 'Features (with policy)', true),
      group: 'Overview',
      header: 'Features',
      fields: ['durable', 'auto_delete', 'exclusive', 'arguments', 'policy', 'operator_policy'],
      cell: (q) => (
        <>
          <FeatureTags obj={q} />
          <PolicyTags policy={q.policy} operatorPolicy={q.operator_policy} />
        </>
      ),
    },
    {
      ...optional('features_no_policy', 'Features (no policy)', false),
      group: 'Overview',
      header: 'Features',
      fields: ['durable', 'auto_delete', 'exclusive', 'arguments'],
      cell: (q) => <FeatureTags obj={q} />,
    },
    {
      ...optional('policy', 'Policy', false),
      group: 'Overview',
      header: 'Policy',
      sortKey: 'policy',
      fields: ['policy', 'operator_policy'],
      cell: (q) => (
        <>
          {q.policy ? <PolicyLink vhost={q.vhost} name={q.policy} /> : null} {q.operator_policy}
        </>
      ),
    },
  )
  if (!stats.disabled) {
    columns.push(
      { ...optional('consumers', 'Consumer count', false), group: 'Overview', header: 'Consumers', sortKey: 'consumers', numeric: true, fields: ['consumers'], cell: (q) => <Num value={q.consumers} /> },
      {
        ...optional('consumer_capacity', 'Consumer capacity', false),
        group: 'Overview',
        header: 'Consumer capacity',
        sortKey: 'consumer_capacity',
        numeric: true,
        fields: ['consumer_capacity'],
        cell: (q) => fmtPercent(q.consumer_capacity),
      },
    )
  }
  columns.push({ ...optional('state', 'State', true), group: 'Overview', header: 'State', sortKey: 'state', fields: ['state', 'idle_since'], cell: (q) => <ObjectState obj={q} /> })

  if (stats.disabled && !stats.queueTotals) return columns
  const msgs = (id: string, label: string, header: string, key: keyof Queue, defaultVisible: boolean): ColumnSpec<Queue> => ({
    ...optional(id, label, defaultVisible),
    group: 'Messages',
    header,
    sortKey: key,
    numeric: true,
    fields: [key],
    cell: (q) => <Num value={q[key] as number | undefined} />,
  })
  columns.push(msgs('msgs-ready', 'Ready', 'Ready', 'messages_ready', true), msgs('msgs-unacked', 'Unacknowledged', 'Unacked', 'messages_unacknowledged', true))
  if (!stats.disabled) {
    columns.push(
      msgs('msgs-delayed', 'Delayed', 'Delayed', 'messages_delayed', false),
      msgs('msgs-ram', 'In memory', 'In memory', 'messages_ram', false),
      msgs('msgs-persistent', 'Persistent', 'Persistent', 'messages_persistent', false),
    )
  }
  columns.push({
    ...optional('msgs-total', 'Total', true),
    group: 'Messages',
    header: 'Total',
    sortKey: 'messages',
    numeric: true,
    fields: ['messages', 'messages_details.rate'],
    cell: (q) => (
      <>
        <Num value={q.messages} />
        <Trend details={q.messages_details} />
      </>
    ),
  })
  if (stats.disabled) return columns

  const bytes = (id: string, label: string, key: keyof Queue): ColumnSpec<Queue> => ({
    ...optional(id, label, false),
    group: 'Message bytes',
    header: label === 'Unacknowledged' ? 'Unacked' : label,
    sortKey: key,
    numeric: true,
    fields: [key],
    cell: (q) => <Bytes value={q[key] as number | undefined} />,
  })
  columns.push(
    bytes('msg-bytes-ready', 'Ready', 'message_bytes_ready'),
    bytes('msg-bytes-unacked', 'Unacknowledged', 'message_bytes_unacknowledged'),
    bytes('msg-bytes-ram', 'In memory', 'message_bytes_ram'),
    bytes('msg-bytes-persistent', 'Persistent', 'message_bytes_persistent'),
    bytes('msg-bytes-total', 'Total', 'message_bytes'),
  )
  if (!stats.hasRates) return columns
  const rate = (id: string, label: string, stat: string, defaultVisible: boolean): ColumnSpec<Queue> => ({
    ...optional(id, label, defaultVisible),
    group: 'Message rates',
    header: label,
    sortKey: `message_stats.${stat}_details.rate`,
    numeric: true,
    fields: [`message_stats.${stat}_details.rate`],
    cell: (q) => <Rate details={(q.message_stats as Record<string, never> | undefined)?.[`${stat}_details`]} />,
  })
  columns.push(
    rate('rate-incoming', 'incoming', 'publish', true),
    rate('rate-deliver', 'deliver / get', 'deliver_get', true),
    rate('rate-redeliver', 'redelivered', 'redeliver', false),
    rate('rate-ack', 'ack', 'ack', true),
  )
  return columns
}

export function QueuesPage() {
  const { stats, access, vhosts } = useAppData()
  const [vhost] = useSelectedVhost()
  const list = useListState('queues')
  usePrefsVersion()
  const nodes = useQuery({ ...nodesQuery(), enabled: access.isMonitoring, staleTime: 60_000 })
  const showNode = (nodes.data?.length ?? 0) > 1
  const columns = queueColumns(stats, vhosts.length > 1 && vhost === '', showNode)
  const visible = columns.filter((c) => isColumnVisible('queues', c as ColumnSpec<unknown>))

  const params = { ...list.params, columns: columnsParam(visible, ['name', 'vhost', 'type']) }
  const queues = useQuery({ ...queueListQuery(vhost, params), refetchInterval: useRefetchInterval('stats'), placeholderData: keepPreviousData })

  return (
    <>
      <PageHeader title="Queues" documentTitle="Queues" />
      <Section id="queues-list" title="All queues" actions={<ColumnChooser mode="queues" columns={columns} />}>
        <ListControls state={list} page={queues.data} noun="queues" error={queues.error} />
        {queues.error ? <ErrorMessage error={queues.error} what="queues" /> : null}
        <DataTable
          mode="queues"
          columns={columns}
          rows={queues.data?.items ?? []}
          rowKey={(q) => `${q.vhost}\u0000${q.name}`}
          sort={list.params}
          onSortChange={(sort) => list.update({ ...sort, page: 1 })}
          empty={queues.isPending ? 'Loading…' : 'No queues'}
        />
      </Section>
      {access.canAccessVhosts ? (
        <Section id="queues-add" title="Add a new queue" defaultOpen={false}>
          <AddQueueForm nodes={access.isMonitoring && showNode ? (nodes.data ?? []).map((n) => n.name) : []} />
        </Section>
      ) : null}
    </>
  )
}

function AddQueueForm({ nodes }: { nodes: string[] }) {
  const { vhosts } = useAppData()
  const notify = useNotify()
  const navigate = useNavigate()
  const [selectedVhost] = useSelectedVhost()
  const [type, setType] = useState<'default' | keyof typeof QUEUE_TYPES>('default')
  const [vhost, setVhost] = useState(selectedVhost || vhosts[0]?.name || '/')
  const [name, setName] = useState('')
  const [durable, setDurable] = useState(true)
  const [autoDelete, setAutoDelete] = useState(false)
  const [node, setNode] = useState('')
  const [args, setArgs] = useState<ArgRow[]>([])
  const info = type === 'default' ? QUEUE_TYPES.classic : QUEUE_TYPES[type]

  const declare = useApiMutation<{ vhost: string; name: string; body: Parameters<typeof declareQueue>[2] }>({
    mutationFn: ({ vhost, name, body }) => declareQueue(vhost, name, body),
    success: (_, v) => `Queue ${v.name} declared`,
    invalidate: [['queues']],
    onSuccess: (_, v) => navigate({ to: '/queues/$vhost/$name', params: { vhost: v.vhost, name: v.name } }),
  })

  const submit = (event: FormEvent) => {
    event.preventDefault()
    let table
    try {
      table = rowsToTable(args)
    } catch (err) {
      notify('error', errorMessage(err))
      return
    }
    if (type !== 'default') table['x-queue-type'] = type
    const body = {
      durable: type === 'classic' || type === 'default' ? durable : true,
      auto_delete: type === 'classic' ? autoDelete : false,
      arguments: table,
      ...info.params,
      ...(node ? { node } : {}),
    }
    declare.mutate({ vhost, name, body })
  }

  return (
    <form className="form" onSubmit={submit} data-testid="add-queue-form">
      <label htmlFor="queue-type">Type</label>
      <select id="queue-type" value={type} onChange={(e) => setType(e.target.value as typeof type)} data-testid="queue-type">
        <option value="default">Default for virtual host</option>
        {Object.entries(QUEUE_TYPES).map(([key, t]) => (
          <option key={key} value={key}>
            {t.label}
          </option>
        ))}
      </select>
      {vhosts.length > 1 ? (
        <>
          <label htmlFor="queue-vhost">Virtual host</label>
          <select id="queue-vhost" value={vhost} onChange={(e) => setVhost(e.target.value)}>
            {vhosts.map((v) => (
              <option key={v.name} value={v.name}>
                {v.name}
              </option>
            ))}
          </select>
        </>
      ) : null}
      <label htmlFor="queue-name">Name</label>
      <input id="queue-name" type="text" required value={name} onChange={(e) => setName(e.target.value)} data-testid="queue-name" />
      {type === 'classic' || type === 'default' ? (
        <>
          <label htmlFor="queue-durable">Durability</label>
          <select id="queue-durable" value={String(durable)} onChange={(e) => setDurable(e.target.value === 'true')}>
            <option value="true">Durable</option>
            <option value="false">Transient</option>
          </select>
        </>
      ) : null}
      {nodes.length > 0 ? (
        <>
          <label htmlFor="queue-node">Node</label>
          <select id="queue-node" value={node} onChange={(e) => setNode(e.target.value)}>
            <option value="">(any)</option>
            {nodes.map((n) => (
              <option key={n} value={n}>
                {n}
              </option>
            ))}
          </select>
        </>
      ) : null}
      {type === 'classic' ? (
        <>
          <label htmlFor="queue-auto-delete">Auto delete</label>
          <select id="queue-auto-delete" value={String(autoDelete)} onChange={(e) => setAutoDelete(e.target.value === 'true')}>
            <option value="false">No</option>
            <option value="true">Yes</option>
          </select>
        </>
      ) : null}
      <span className="label">Arguments</span>
      <ArgumentsEditor rows={args} onChange={setArgs} shortcuts={info.argumentShortcuts} testId="queue-arguments" />
      <div className="actions">
        <button type="submit" className="btn btn-primary" disabled={declare.isPending} data-testid="add-queue-submit">
          Add queue
        </button>
      </div>
    </form>
  )
}
