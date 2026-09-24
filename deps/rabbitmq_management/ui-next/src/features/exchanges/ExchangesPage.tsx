import { useState, type FormEvent } from 'react'
import { keepPreviousData, useQuery } from '@tanstack/react-query'
import { useNavigate } from '@tanstack/react-router'
import { declareExchange, exchangeListQuery } from '../../api/resources/exchanges'
import type { Exchange } from '../../api/types/exchanges'
import { useAppData, type StatsMode } from '../../app/context'
import { useNotify } from '../../app/notifications'
import { useRefetchInterval } from '../../app/refresh'
import { useSelectedVhost } from '../../app/vhost'
import { errorMessage } from '../../api/errors'
import { ArgumentsEditor, rowsToTable, type ArgRow } from '../../components/ArgumentsEditor'
import { ColumnChooser } from '../../components/ColumnChooser'
import { columnsParam, isColumnVisible, type ColumnSpec } from '../../components/columns'
import { DataTable } from '../../components/DataTable'
import { ListControls } from '../../components/ListControls'
import { useListState } from '../../components/listState'
import { ExchangeLink, PolicyLink } from '../../components/Links'
import { useApiMutation } from '../../components/mutation'
import { PageHeader } from '../../components/PageHeader'
import { Section } from '../../components/Section'
import { ErrorMessage } from '../../components/Status'
import { FeatureTags, PolicyTags, Rate } from '../../components/Values'
import { usePrefsVersion } from '../../prefs/storage'

function exchangeColumns(stats: StatsMode, showVhost: boolean): ColumnSpec<Exchange>[] {
  const optional = (id: string, label: string, defaultVisible: boolean) => ({ id, optional: { label, defaultVisible } })
  const columns: ColumnSpec<Exchange>[] = []
  if (showVhost) columns.push({ id: 'vhost', header: 'Virtual host', sortKey: 'vhost', cell: (x) => x.vhost })
  columns.push(
    { id: 'name', header: 'Name', sortKey: 'name', cell: (x) => <ExchangeLink vhost={x.vhost} name={x.name} /> },
    { ...optional('type', 'Type', true), header: 'Type', sortKey: 'type', cell: (x) => x.type },
    {
      ...optional('features', 'Features (with policy)', true),
      header: 'Features',
      fields: ['durable', 'auto_delete', 'internal', 'arguments', 'policy'],
      cell: (x) => (
        <>
          <FeatureTags obj={x} />
          <PolicyTags policy={x.policy} />
        </>
      ),
    },
    {
      ...optional('features_no_policy', 'Features (no policy)', false),
      header: 'Features',
      fields: ['durable', 'auto_delete', 'internal', 'arguments'],
      cell: (x) => <FeatureTags obj={x} />,
    },
    {
      ...optional('policy', 'Policy', false),
      header: 'Policy',
      sortKey: 'policy',
      fields: ['policy'],
      cell: (x) => (x.policy ? <PolicyLink vhost={x.vhost} name={x.policy} /> : null),
    },
  )
  if (stats.hasRates) {
    columns.push(
      {
        ...optional('rate-in', 'rate in', true),
        group: 'Message rates',
        header: 'Message rate in',
        sortKey: 'message_stats.publish_in_details.rate',
        numeric: true,
        fields: ['message_stats.publish_in_details.rate'],
        cell: (x) => <Rate details={x.message_stats?.publish_in_details} />,
      },
      {
        ...optional('rate-out', 'rate out', true),
        group: 'Message rates',
        header: 'Message rate out',
        sortKey: 'message_stats.publish_out_details.rate',
        numeric: true,
        fields: ['message_stats.publish_out_details.rate'],
        cell: (x) => <Rate details={x.message_stats?.publish_out_details} />,
      },
    )
  }
  return columns
}

export function ExchangesPage() {
  const { stats, access, vhosts } = useAppData()
  const [vhost] = useSelectedVhost()
  const list = useListState('exchanges')
  usePrefsVersion()
  const columns = exchangeColumns(stats, vhosts.length > 1 && vhost === '')
  const visible = columns.filter((c) => isColumnVisible('exchanges', c as ColumnSpec<unknown>))
  const params = { ...list.params, columns: columnsParam(visible, ['name', 'vhost', 'type']) }
  const exchanges = useQuery({ ...exchangeListQuery(vhost, params), refetchInterval: useRefetchInterval('stats'), placeholderData: keepPreviousData })

  return (
    <>
      <PageHeader title="Exchanges" documentTitle="Exchanges" />
      <Section id="exchanges-list" title="All exchanges" actions={<ColumnChooser mode="exchanges" columns={columns} />}>
        <ListControls state={list} page={exchanges.data} noun="exchanges" error={exchanges.error} />
        {exchanges.error ? <ErrorMessage error={exchanges.error} what="exchanges" /> : null}
        <DataTable
          mode="exchanges"
          columns={columns}
          rows={exchanges.data?.items ?? []}
          rowKey={(x) => `${x.vhost}\u0000${x.name}`}
          sort={list.params}
          onSortChange={(sort) => list.update({ ...sort, page: 1 })}
          empty={exchanges.isPending ? 'Loading…' : 'No exchanges'}
        />
      </Section>
      {access.canAccessVhosts ? (
        <Section id="exchanges-add" title="Add a new exchange" defaultOpen={false}>
          <AddExchangeForm />
        </Section>
      ) : null}
    </>
  )
}

function AddExchangeForm() {
  const { vhosts, overview } = useAppData()
  const notify = useNotify()
  const navigate = useNavigate()
  const [selectedVhost] = useSelectedVhost()
  const types = overview.exchange_types.filter((t) => !(t as { internal_purpose?: string }).internal_purpose)
  const [vhost, setVhost] = useState(selectedVhost || vhosts[0]?.name || '/')
  const [name, setName] = useState('')
  const [type, setType] = useState(types[0]?.name ?? 'direct')
  const [durable, setDurable] = useState(true)
  const [autoDelete, setAutoDelete] = useState(false)
  const [internal, setInternal] = useState(false)
  const [args, setArgs] = useState<ArgRow[]>([])

  const declare = useApiMutation<{ vhost: string; name: string; body: Parameters<typeof declareExchange>[2] }>({
    mutationFn: ({ vhost, name, body }) => declareExchange(vhost, name, body),
    success: (_, v) => `Exchange ${v.name} declared`,
    invalidate: [['exchanges']],
    onSuccess: (_, v) => navigate({ to: '/exchanges/$vhost/$name', params: { vhost: v.vhost, name: v.name } }),
  })

  const submit = (event: FormEvent) => {
    event.preventDefault()
    try {
      declare.mutate({ vhost, name, body: { type, durable, auto_delete: autoDelete, internal, arguments: rowsToTable(args) } })
    } catch (err) {
      notify('error', errorMessage(err))
    }
  }

  return (
    <form className="form" onSubmit={submit} data-testid="add-exchange-form">
      {vhosts.length > 1 ? (
        <>
          <label htmlFor="exchange-vhost">Virtual host</label>
          <select id="exchange-vhost" value={vhost} onChange={(e) => setVhost(e.target.value)}>
            {vhosts.map((v) => (
              <option key={v.name} value={v.name}>
                {v.name}
              </option>
            ))}
          </select>
        </>
      ) : null}
      <label htmlFor="exchange-name">Name</label>
      <input id="exchange-name" type="text" required value={name} onChange={(e) => setName(e.target.value)} data-testid="exchange-name" />
      <label htmlFor="exchange-type">Type</label>
      <select id="exchange-type" value={type} onChange={(e) => setType(e.target.value)} data-testid="exchange-type">
        {types.map((t) => (
          <option key={t.name} value={t.name}>
            {t.name}
          </option>
        ))}
      </select>
      <label htmlFor="exchange-durable">Durability</label>
      <select id="exchange-durable" value={String(durable)} onChange={(e) => setDurable(e.target.value === 'true')}>
        <option value="true">Durable</option>
        <option value="false">Transient</option>
      </select>
      <label htmlFor="exchange-auto-delete">Auto delete</label>
      <select id="exchange-auto-delete" value={String(autoDelete)} onChange={(e) => setAutoDelete(e.target.value === 'true')}>
        <option value="false">No</option>
        <option value="true">Yes</option>
      </select>
      <label htmlFor="exchange-internal">Internal</label>
      <select id="exchange-internal" value={String(internal)} onChange={(e) => setInternal(e.target.value === 'true')}>
        <option value="false">No</option>
        <option value="true">Yes</option>
      </select>
      <span className="label">Arguments</span>
      <ArgumentsEditor rows={args} onChange={setArgs} shortcuts={[{ key: 'alternate-exchange', label: 'Alternate exchange', type: 'string' }]} testId="exchange-arguments" />
      <div className="actions">
        <button type="submit" className="btn btn-primary" disabled={declare.isPending} data-testid="add-exchange-submit">
          Add exchange
        </button>
      </div>
    </form>
  )
}
