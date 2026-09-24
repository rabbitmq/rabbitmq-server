import { useState } from 'react'
import { Link } from '@tanstack/react-router'
import { useQuery } from '@tanstack/react-query'
import { apiFetch } from '../../api/client'
import { healthCheckQuery, nodesQuery, overviewQuery, resetStats } from '../../api/resources/overview'
import { exportDefinitions, importDefinitions } from '../../api/resources/admin'
import type { Vhost } from '../../api/types/admin'
import type { ClusterNode } from '../../api/types/nodes'
import { useAppData } from '../../app/context'
import { useRefetchInterval } from '../../app/refresh'
import { ChartRangeSelect, useChartRange } from '../../charts/ChartRangeSelect'
import { messageRateSeries, pickSeries, queueLengthSeries } from '../../charts/stats'
import { ColumnChooser } from '../../components/ColumnChooser'
import type { ColumnSpec } from '../../components/columns'
import { DataTable } from '../../components/DataTable'
import { confirmAction, useApiMutation } from '../../components/mutation'
import { PageHeader } from '../../components/PageHeader'
import { Section } from '../../components/Section'
import { ErrorMessage, Loading } from '../../components/Status'
import { Bool } from '../../components/Values'
import { fmtNum } from '../../format/numbers'
import { rabbitVersion } from '../../format/names'
import { fmtUptime } from '../../format/time'
import { ChartBlock } from '../shared/ChartBlock'
import { DiskBar, FdBar, MemoryBar, ProcessBar } from '../nodes/NodeBars'
import { attentionItems } from './attention'
import { useNotify } from '../../app/notifications'
import { errorMessage } from '../../api/errors'

export function OverviewPage() {
  const { access, stats, overview: settings } = useAppData()
  const range = useChartRange('global')
  const interval = useRefetchInterval('stats')
  const overview = useQuery({ ...overviewQuery(range), refetchInterval: interval })
  const nodes = useQuery({ ...nodesQuery(), refetchInterval: interval, enabled: access.isMonitoring })
  const vhostStates = useQuery({
    queryKey: ['vhosts', 'states'],
    queryFn: ({ signal }) => apiFetch<Pick<Vhost, 'name' | 'cluster_state'>[]>('vhosts', { signal, params: { columns: 'name,cluster_state' } }),
    refetchInterval: useRefetchInterval('topology'),
  })
  // Node data carries alarms only for monitoring users and only while statistics are collected.
  const nodeDetails = access.isMonitoring && !stats.disabled
  const alarms = useQuery({ ...healthCheckQuery('alarms'), refetchInterval: interval, enabled: !nodeDetails })

  const data = overview.data
  return (
    <>
      <PageHeader title={stats.disabled ? 'Overview: Management only mode' : 'Overview'} documentTitle="Overview" />
      <NeedsAttention
        items={attentionItems({ overview: data, nodes: nodes.data, vhosts: vhostStates.data, alarmsCheck: alarms.data, statsDisabled: stats.disabled })}
        ready={data !== undefined && (nodeDetails ? nodes.data !== undefined : alarms.data !== undefined)}
        allClear={nodeDetails ? 'No alarms, partitions or stopped nodes detected.' : 'No resource alarms in effect.'}
      />
      {overview.error ? <ErrorMessage error={overview.error} what="the overview" /> : null}

      <Section id="overview-totals" title="Totals" actions={stats.disabled ? null : <ChartRangeSelect type="global" />}>
        {!data ? (
          <Loading />
        ) : (
          <div className="stack">
            {!stats.disabled ? (
              <div className="grid-2">
                <ChartBlock title="Queued messages" series={queueLengthSeries(data.queue_totals)} kind="gauge" testId="chart-lengths" />
                {stats.hasRates ? (
                  <ChartBlock title="Message rates" series={messageRateSeries(data.message_stats)} kind="rate" testId="chart-msg-rates" />
                ) : null}
              </div>
            ) : null}
            {data.object_totals ? <GlobalCounts totals={data.object_totals} showChannels={!stats.disabled} /> : null}
          </div>
        )}
      </Section>

      {access.isMonitoring ? (
        <Section id="overview-nodes" title="Nodes" actions={stats.disabled ? null : <ColumnChooser mode="overview" columns={nodeColumns(access.isAdministrator, false, false)} />}>
          {nodes.error ? <ErrorMessage error={nodes.error} what="nodes" /> : null}
          {nodes.data ? <NodesTable nodes={nodes.data} /> : <Loading />}
        </Section>
      ) : null}

      {!stats.disabled && data ? (
        <Section id="overview-churn" title="Churn statistics" defaultOpen={false}>
          <div className="grid-2">
            <ChartBlock
              title="Connection operations"
              series={pickSeries(data.churn_rates, [
                ['Created', 'connection_created'],
                ['Closed', 'connection_closed'],
              ])}
              kind="rate"
            />
            <ChartBlock
              title="Channel operations"
              series={pickSeries(data.churn_rates, [
                ['Created', 'channel_created'],
                ['Closed', 'channel_closed'],
              ])}
              kind="rate"
            />
            <ChartBlock
              title="Queue operations"
              series={pickSeries(data.churn_rates, [
                ['Declared', 'queue_declared'],
                ['Created', 'queue_created'],
                ['Deleted', 'queue_deleted'],
              ])}
              kind="rate"
            />
          </div>
        </Section>
      ) : null}

      {!stats.disabled && data?.listeners ? (
        <Section id="overview-ports" title="Ports and contexts" defaultOpen={false}>
          <PortsAndContexts listeners={data.listeners} contexts={data.contexts ?? []} showNode={(nodes.data?.length ?? 0) > 1} />
        </Section>
      ) : null}

      {access.isAdministrator ? (
        <>
          <Section id="overview-export" title="Export definitions" defaultOpen={false}>
            <ExportDefinitions node={data?.node ?? settings.node ?? 'rabbit'} />
          </Section>
          <Section id="overview-import" title="Import definitions" defaultOpen={false}>
            <ImportDefinitions requireJson={settings.require_definition_json_extension ?? false} />
          </Section>
        </>
      ) : null}

      {settings.rates_mode === 'none' ? (
        <Section id="overview-rates-disabled" title="Message rates disabled" defaultOpen={false}>
          <p>Message rates are currently disabled.</p>
          <p>
            To re-enable message rates, set <code>management_agent.rates_mode</code> to <code>basic</code> or{' '}
            <code>detailed</code> in the configuration file.
          </p>
        </Section>
      ) : null}
    </>
  )
}

function NeedsAttention({ items, ready, allClear }: { items: ReturnType<typeof attentionItems>; ready: boolean; allClear: string }) {
  if (!ready) return null
  if (items.length === 0) {
    return (
      <div className="callout callout-ok" data-testid="attention-none">
        {allClear}
      </div>
    )
  }
  return (
    <div className={`callout ${items.some((i) => i.severity === 'bad') ? 'callout-bad' : 'callout-warn'}`} data-testid="attention">
      <strong>Needs attention</strong>
      <ul style={{ margin: '0.4rem 0 0', paddingLeft: '1.2rem' }}>
        {items.map((item, i) => (
          <li key={i} data-testid="attention-item">
            {item.link ? (
              <Link to={item.link.to} params={item.link.params as never}>
                {item.message}
              </Link>
            ) : (
              item.message
            )}
          </li>
        ))}
      </ul>
    </div>
  )
}

function GlobalCounts({ totals, showChannels }: { totals: NonNullable<import('../../api/types/overview').Overview['object_totals']>; showChannels: boolean }) {
  const counts: [string, number, string | undefined][] = [
    ['Connections', totals.connections, '/connections'],
    ...(showChannels ? ([['Channels', totals.channels, '/channels']] as [string, number, string][]) : []),
    ['Exchanges', totals.exchanges, '/exchanges'],
    ['Queues', totals.queues, '/queues'],
    ['Consumers', totals.consumers, undefined],
  ]
  return (
    <div>
      <h3>Global counts</h3>
      <div className="row" data-testid="global-counts">
        {counts
          .filter(([, value]) => value !== undefined)
          .map(([label, value, to]) =>
            to ? (
              <Link key={label} to={to} className="btn" data-testid={`count-${label.toLowerCase()}`}>
                {label}: <strong>{fmtNum(value)}</strong>
              </Link>
            ) : (
              <span key={label} className="btn" aria-disabled="true" data-testid={`count-${label.toLowerCase()}`}>
                {label}: <strong>{fmtNum(value)}</strong>
              </span>
            ),
          )}
      </div>
    </div>
  )
}

function nodeColumns(isAdministrator: boolean, versionsDiffer: boolean, statsDisabled: boolean): ColumnSpec<ClusterNode>[] {
  const unavailable = (node: ClusterNode) => !node.running || node.os_pid === undefined
  const stat = (render: (node: ClusterNode) => React.ReactNode) => (node: ClusterNode) => (unavailable(node) ? null : render(node))
  const columns: ColumnSpec<ClusterNode>[] = [
    {
      id: 'name',
      header: 'Name',
      cell: (node) => (
        <>
          <Link to="/nodes/$name" params={{ name: node.name }} data-testid="node-link">
            {node.name}
          </Link>
          {versionsDiffer ? <div className="hint">RabbitMQ {rabbitVersion(node.applications)}</div> : null}
          {!node.running ? <div style={{ color: 'var(--bad)' }}>Node not running</div> : null}
          {node.running && node.os_pid === undefined && !statsDisabled ? <div style={{ color: 'var(--warn)' }}>Node statistics not available</div> : null}
        </>
      ),
    },
  ]
  // Without statistics there is nothing but the name to show, as in the classic UI.
  if (statsDisabled) return columns
  columns.push(
    { id: 'file_descriptors', group: 'Statistics', header: 'File descriptors', optional: { label: 'File descriptors', defaultVisible: true }, cell: stat((n) => <FdBar node={n} />) },
    { id: 'erlang_processes', group: 'Statistics', header: 'Erlang processes', optional: { label: 'Erlang processes', defaultVisible: true }, cell: stat((n) => <ProcessBar node={n} />) },
    { id: 'memory', group: 'Statistics', header: 'Memory', optional: { label: 'Memory', defaultVisible: true }, cell: stat((n) => <MemoryBar node={n} />) },
    { id: 'disk_space', group: 'Statistics', header: 'Disk space', optional: { label: 'Disk space', defaultVisible: true }, cell: stat((n) => <DiskBar node={n} />) },
    { id: 'uptime', group: 'General', header: 'Uptime', optional: { label: 'Uptime', defaultVisible: true }, cell: stat((n) => fmtUptime(n.uptime)) },
    { id: 'cores', group: 'General', header: 'Cores', numeric: true, optional: { label: 'Cores', defaultVisible: true }, cell: stat((n) => n.processors) },
    {
      id: 'info',
      group: 'General',
      header: 'Info',
      optional: { label: 'Info', defaultVisible: true },
      cell: stat((n) => (
        <>
          {n.being_drained ? <span className="tag" style={{ color: 'var(--warn)' }}>maintenance mode</span> : null}
          <abbr className="tag" title="Message rates">{n.rates_mode}</abbr>
          <abbr
            className="tag"
            title={`Enabled plugins: ${(n.applications ?? []).filter((a) => n.enabled_plugins?.includes(a.name)).map((a) => a.name).join(', ')}`}
          >
            {(n.applications ?? []).filter((a) => n.enabled_plugins?.includes(a.name)).length}
          </abbr>
          <abbr className="tag" title="Memory calculation strategy">{n.mem_calculation_strategy}</abbr>
        </>
      )),
    },
  )
  if (isAdministrator) {
    columns.push({
      id: 'reset_stats',
      group: 'General',
      header: 'Reset stats',
      optional: { label: 'Reset stats', defaultVisible: true },
      cell: (n) => <ResetStats node={n.name} />,
    })
  }
  return columns
}

function NodesTable({ nodes }: { nodes: ClusterNode[] }) {
  const { access, stats } = useAppData()
  const versions = new Set(nodes.map((n) => rabbitVersion(n.applications)).filter((v) => v !== 'unknown'))
  return <DataTable mode="overview" columns={nodeColumns(access.isAdministrator, versions.size > 1, stats.disabled)} rows={nodes} rowKey={(n) => n.name} />
}

function ResetStats({ node }: { node: string }) {
  const reset = useApiMutation<string | undefined>({
    mutationFn: (target) => resetStats(target),
    success: (_, target) => (target ? `Statistics reset on ${target}` : 'Statistics reset on all nodes'),
    invalidate: [['overview'], ['nodes']],
  })
  const run = (target?: string) => {
    if (confirmAction('Are you sure? This resets the management statistics database.')) reset.mutate(target)
  }
  return (
    <span className="row" style={{ gap: '0.3rem' }}>
      <button type="button" className="btn btn-small" onClick={() => run(node)} disabled={reset.isPending}>
        This node
      </button>
      <button type="button" className="btn btn-small" onClick={() => run(undefined)} disabled={reset.isPending}>
        All nodes
      </button>
    </span>
  )
}

function PortsAndContexts({
  listeners,
  contexts,
  showNode,
}: {
  listeners: NonNullable<import('../../api/types/overview').Overview['listeners']>
  contexts: NonNullable<import('../../api/types/overview').Overview['contexts']>
  showNode: boolean
}) {
  return (
    <div className="stack">
      <div>
        <h3>Listening ports</h3>
        <div className="table-wrap">
          <table className="list" data-testid="listeners-table">
            <thead>
              <tr>
                <th>Protocol</th>
                {showNode ? <th>Node</th> : null}
                <th>Bound to</th>
                <th className="num">Port</th>
                <th>TLS</th>
              </tr>
            </thead>
            <tbody>
              {listeners.map((l) => (
                <tr key={`${l.node}-${l.protocol}-${l.ip_address}-${l.port}`}>
                  <td>{l.protocol}</td>
                  {showNode ? <td>{l.node}</td> : null}
                  <td>{l.ip_address}</td>
                  <td className="num">{l.port}</td>
                  <td>
                    <Bool value={l.tls ?? false} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
      <div>
        <h3>Web contexts</h3>
        <div className="table-wrap">
          <table className="list">
            <thead>
              <tr>
                <th>Context</th>
                {showNode ? <th>Node</th> : null}
                <th>Bound to</th>
                <th className="num">Port</th>
                <th>TLS</th>
                <th>Path</th>
              </tr>
            </thead>
            <tbody>
              {contexts.map((c) => (
                <tr key={`${c.node}-${c.port}-${c.path}`}>
                  <td>{c.description}</td>
                  {showNode ? <td>{c.node}</td> : null}
                  <td>{c.ip ?? '0.0.0.0'}</td>
                  <td className="num">{c.port}</td>
                  <td>
                    <Bool value={c.tls ?? false} />
                  </td>
                  <td>{c.path}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}

const TOKEN = /^[a-zA-Z0-9!#$%&'*+\-.^_`|~]+$/

function defaultFilename(node: string): string {
  const now = new Date()
  return `${node.replace('@', '_')}_${now.getFullYear()}-${now.getMonth() + 1}-${now.getDate()}.json`
}

function VhostOptions() {
  const { vhosts } = useAppData()
  return (
    <>
      <option value="">All</option>
      {vhosts.map((v) => (
        <option key={v.name} value={v.name}>
          {v.name}
        </option>
      ))}
    </>
  )
}

function ExportDefinitions({ node }: { node: string }) {
  const notify = useNotify()
  const [filename, setFilename] = useState(() => defaultFilename(node))
  const [vhost, setVhost] = useState('')
  const [busy, setBusy] = useState(false)
  const valid = TOKEN.test(filename)

  const download = async () => {
    setBusy(true)
    try {
      const blob = await exportDefinitions(vhost)
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = filename
      document.body.appendChild(a)
      a.click()
      a.remove()
      window.setTimeout(() => URL.revokeObjectURL(url), 1000)
    } catch (err) {
      notify('error', `Error downloading definitions: ${errorMessage(err)}`)
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="form">
      <label htmlFor="download-filename">Filename for download</label>
      <div>
        <input id="download-filename" type="text" value={filename} onChange={(e) => setFilename(e.target.value)} aria-invalid={!valid} />
        {!valid ? <div className="hint" style={{ color: 'var(--bad)' }}>Only a-z A-Z 0-9 and ! # $ % &amp; &apos; * + - . ^ _ ` | ~ are allowed.</div> : null}
      </div>
      <label htmlFor="download-vhost">Virtual host</label>
      <select id="download-vhost" value={vhost} onChange={(e) => setVhost(e.target.value)}>
        <VhostOptions />
      </select>
      <div className="actions">
        <button type="button" className="btn btn-primary" disabled={!valid || busy} onClick={download} data-testid="download-definitions">
          Download broker definitions
        </button>
      </div>
    </div>
  )
}

function ImportDefinitions({ requireJson }: { requireJson: boolean }) {
  const [file, setFile] = useState<File | null>(null)
  const [vhost, setVhost] = useState('')
  const upload = useApiMutation<{ file: File; vhost: string }>({
    mutationFn: ({ file, vhost }) => importDefinitions(vhost, file),
    success: 'Your definitions were imported successfully.',
    invalidate: [[]],
  })
  const submit = (event: React.FormEvent) => {
    event.preventDefault()
    if (!file) return
    if (confirmAction('Are you sure you want to import a definitions file? Some entities (vhosts, users, queues, etc) may be overwritten!')) {
      upload.mutate({ file, vhost })
    }
  }
  return (
    <form className="form" onSubmit={submit}>
      <label htmlFor="upload-file">Definitions file</label>
      <input id="upload-file" type="file" accept={requireJson ? '.json' : undefined} onChange={(e) => setFile(e.target.files?.[0] ?? null)} />
      <label htmlFor="upload-vhost">Virtual host</label>
      <select id="upload-vhost" value={vhost} onChange={(e) => setVhost(e.target.value)}>
        <VhostOptions />
      </select>
      <div className="actions">
        <button type="submit" className="btn btn-primary" disabled={!file || upload.isPending} data-testid="upload-definitions">
          Upload broker definitions
        </button>
      </div>
    </form>
  )
}
