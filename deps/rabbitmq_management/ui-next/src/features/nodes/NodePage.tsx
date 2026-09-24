import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useParams } from '@tanstack/react-router'
import { nodeQuery } from '../../api/resources/overview'
import type { ClusterNode, RegistryEntry } from '../../api/types/nodes'
import { useAppData } from '../../app/context'
import { useRefetchInterval } from '../../app/refresh'
import { ChartRangeSelect, useChartRange } from '../../charts/ChartRangeSelect'
import { pickSeries } from '../../charts/stats'
import { NodeLink } from '../../components/Links'
import { PageHeader } from '../../components/PageHeader'
import { Section } from '../../components/Section'
import { ErrorMessage, Loading } from '../../components/Status'
import { Bool, Facts, Rate } from '../../components/Values'
import { rabbitVersion } from '../../format/names'
import { fmtUptime } from '../../format/time'
import { ChartBlock } from '../shared/ChartBlock'
import { MemoryBreakdown } from './MemoryBreakdown'
import { DiskBar, FdBar, MemoryBar, ProcessBar, SocketsBar } from './NodeBars'

export function NodePage() {
  const { name } = useParams({ from: '/nodes/$name' })
  const { stats } = useAppData()
  const range = useChartRange()
  const node = useQuery({ ...nodeQuery(name, range), refetchInterval: useRefetchInterval('stats') })
  const header = <PageHeader kind="Node" title={name} documentTitle={`Node ${name}`} />

  if (!node.data) return <>{header}{node.error ? <ErrorMessage error={node.error} what={`Node ${name}`} /> : <Loading />}</>
  const n = node.data
  if (!n.running) return <>{header}<div className="callout callout-bad">Node not running</div></>
  const statsAvailable = n.os_pid !== undefined
  if (!statsAvailable && !stats.disabled) return <>{header}<div className="callout callout-warn">Node statistics not available</div></>

  const series = (items: [string, string][]) => pickSeries(n, items)
  return (
    <>
      {header}
      {node.error ? <ErrorMessage error={node.error} what="the latest node state" /> : null}
      {!stats.disabled ? (
        <>
          <Section id="node-overview" title="Overview">
            <div className="grid-2">
              <Facts
                rows={[
                  ...(n.being_drained ? ([['Status', <span key="s" style={{ color: 'var(--warn)' }}>The node was put under maintenance</span>]] as [string, React.ReactNode][]) : []),
                  ['Uptime', fmtUptime(n.uptime)],
                  ['Cores', n.processors],
                  ['RabbitMQ version', rabbitVersion(n.applications)],
                  ['Config files', (n.config_files ?? []).map((f) => <div key={f}><code>{f}</code></div>)],
                  ['Database directory', <code key="db">{n.db_dir}</code>],
                  [(n.log_files?.length ?? 0) === 1 ? 'Log file' : 'Log files', (n.log_files ?? []).map((f) => <div key={f}><code>{f}</code></div>)],
                ]}
              />
              <Facts
                rows={[
                  ['File descriptors', <FdBar node={n} key="fd" />],
                  ['Socket descriptors', <SocketsBar node={n} key="so" />],
                  ['Erlang processes', <ProcessBar node={n} key="p" />],
                  ['Memory', <MemoryBar node={n} key="m" />],
                  ['Disk space', <DiskBar node={n} key="d" />],
                ]}
              />
            </div>
          </Section>

          <Section id="node-process-stats" title="Process statistics" defaultOpen={false} actions={<ChartRangeSelect />}>
            <div className="grid-2">
              <ChartBlock title="File descriptors" series={series([['Used', 'fd_used']])} kind="gauge" />
              <ChartBlock title="Erlang processes" series={series([['Used', 'proc_used']])} kind="gauge" />
              <ChartBlock title="Memory" series={series([['Used', 'mem_used']])} kind="gauge" bytes />
              <ChartBlock title="Disk space" series={series([['Free', 'disk_free']])} kind="gauge" bytes />
            </div>
          </Section>

          <Section id="node-persistence" title="Persistence statistics" defaultOpen={false} actions={<ChartRangeSelect />}>
            <div className="grid-2">
              <ChartBlock title="Schema data store transactions" series={series([['RAM only', 'mnesia_ram_tx_count'], ['Disk', 'mnesia_disk_tx_count']])} kind="rate" />
              <ChartBlock
                title="Persistence operations (messages)"
                series={series([['QI Journal', 'queue_index_journal_write_count'], ['Store Read', 'msg_store_read_count'], ['Store Write', 'msg_store_write_count']])}
                kind="rate"
              />
              <ChartBlock title="Persistence operations (bulk)" series={series([['QI Read', 'queue_index_read_count'], ['QI Write', 'queue_index_write_count']])} kind="rate" />
            </div>
          </Section>

          <Section id="node-io" title="I/O statistics" defaultOpen={false} actions={<ChartRangeSelect />}>
            <div className="grid-2">
              <ChartBlock
                title="I/O operations"
                series={series([['Read', 'io_read_count'], ['Write', 'io_write_count'], ['Seek', 'io_seek_count'], ['Sync', 'io_sync_count'], ['File handle reopen', 'io_reopen_count']])}
                kind="rate"
              />
              <ChartBlock title="I/O data rates" series={series([['Read', 'io_read_bytes'], ['Write', 'io_write_bytes']])} kind="rate" bytes />
            </div>
          </Section>

          <Section id="node-churn" title="Churn statistics" defaultOpen={false} actions={<ChartRangeSelect />}>
            <div className="grid-2">
              <ChartBlock title="Connection operations" series={series([['Created', 'connection_created'], ['Closed', 'connection_closed']])} kind="rate" />
              <ChartBlock title="Channel operations" series={series([['Created', 'channel_created'], ['Closed', 'channel_closed']])} kind="rate" />
              <ChartBlock
                title="Queue operations"
                series={series([['Declared', 'queue_declared'], ['Created', 'queue_created'], ['Deleted', 'queue_deleted']])}
                kind="rate"
              />
            </div>
          </Section>

          <Section id="node-cluster-links" title="Cluster links" defaultOpen={false}>
            <ClusterLinks node={n} />
          </Section>
        </>
      ) : null}

      <Section id="node-memory" title="Memory details">
        <OnDemandBreakdown name={name} kind="memory" />
      </Section>
      <Section id="node-binary" title="Binary references" defaultOpen={false}>
        <p>
          <strong>Warning:</strong> calculating binary memory use can be expensive if there are many small binaries in the system.
        </p>
        <OnDemandBreakdown name={name} kind="binary" />
      </Section>

      {!stats.disabled && statsAvailable ? (
        <Section id="node-advanced" title="Advanced" defaultOpen={false} actions={<ChartRangeSelect />}>
          <Advanced node={n} />
        </Section>
      ) : null}
    </>
  )
}

/** Memory and binary breakdowns are expensive to compute, so they are only fetched on request. */
function OnDemandBreakdown({ name, kind }: { name: string; kind: 'memory' | 'binary' }) {
  const [requested, setRequested] = useState(kind === 'memory')
  const query = useQuery({ ...nodeQuery(name, undefined, { [kind]: true }), enabled: requested, staleTime: Infinity })
  const values = kind === 'memory' ? query.data?.memory : query.data?.binary
  const total = kind === 'memory' ? query.data?.memory?.total : undefined
  return (
    <div className="stack">
      {query.error ? <ErrorMessage error={query.error} what={`${kind} details`} /> : null}
      {values ? (
        <MemoryBreakdown
          values={values as Record<string, unknown>}
          caption={total ? `Total ${total.strategy ?? query.data?.memory?.strategy ?? ''}: RSS ${fmtMb(total.rss)}, allocated ${fmtMb(total.allocated)}, Erlang ${fmtMb(total.erlang)}` : undefined}
          testId={`${kind}-breakdown`}
        />
      ) : requested ? (
        <Loading />
      ) : null}
      <div>
        <button
          type="button"
          className="btn"
          onClick={() => (requested ? void query.refetch() : setRequested(true))}
          disabled={query.isFetching}
          data-testid={`${kind}-update`}
        >
          {requested ? 'Update' : 'Calculate'}
        </button>
      </div>
    </div>
  )
}

const fmtMb = (bytes: number) => `${(bytes / 1024 / 1024).toFixed(1)} MiB`

function ClusterLinks({ node }: { node: ClusterNode }) {
  const links = node.cluster_links ?? []
  if (links.length === 0) return <p className="muted">No cluster links</p>
  return (
    <div className="table-wrap">
      <table className="list">
        <thead>
          <tr>
            <th>Remote node</th>
            <th>Local address</th>
            <th className="num">Local port</th>
            <th>Remote address</th>
            <th className="num">Remote port</th>
            <th className="num">Recv</th>
            <th className="num">Send</th>
          </tr>
        </thead>
        <tbody>
          {links.map((link) => (
            <tr key={link.name}>
              <td>
                <NodeLink name={link.name} />
              </td>
              <td>{link.sock_addr}</td>
              <td className="num">{link.sock_port}</td>
              <td>{link.peer_addr}</td>
              <td className="num">{link.peer_port}</td>
              <td className="num">
                <Rate details={link.stats?.recv_bytes_details} bytes />
              </td>
              <td className="num">
                <Rate details={link.stats?.send_bytes_details} bytes />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function Registry({ entries, showEnabled }: { entries: RegistryEntry[] | undefined; showEnabled: boolean }) {
  return (
    <div className="table-wrap">
      <table className="list">
        <thead>
          <tr>
            <th>Name</th>
            <th>Description</th>
            {showEnabled ? <th>Enabled</th> : null}
          </tr>
        </thead>
        <tbody>
          {(entries ?? []).map((entry) => (
            <tr key={entry.name}>
              <td>{entry.name}</td>
              <td>{entry.description}</td>
              {showEnabled ? (
                <td>
                  <Bool value={entry.enabled} />
                </td>
              ) : null}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function Applications({ applications }: { applications: ClusterNode['applications'] }) {
  return (
    <div className="table-wrap">
      <table className="list">
        <thead>
          <tr>
            <th>Name</th>
            <th>Version</th>
            <th>Description</th>
          </tr>
        </thead>
        <tbody>
          {(applications ?? []).map((app) => (
            <tr key={app.name}>
              <td>{app.name}</td>
              <td>{app.version}</td>
              <td>{app.description}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function Advanced({ node: n }: { node: ClusterNode }) {
  const series = (items: [string, string][]) => pickSeries(n, items)
  const extra = n as ClusterNode & { metrics_gc_queue_length?: Record<string, number>; ra_open_file_metrics?: Record<string, number> }
  const plugins = (n.applications ?? []).filter((app) => n.enabled_plugins?.includes(app.name))
  return (
    <div className="stack">
      <div className="grid-2">
        <div>
          <h3>VM</h3>
          <Facts
            rows={[
              ['OS pid', n.os_pid],
              ['Rates mode', n.rates_mode],
              ['Net ticktime', `${n.net_ticktime}s`],
              ['Run queue', n.run_queue],
              ['Processors', n.processors],
            ]}
          />
        </div>
        <div>
          <h3>Management GC queue length</h3>
          <Facts rows={Object.entries(extra.metrics_gc_queue_length ?? {}).map(([k, v]) => [k, v])} />
          <h3>Quorum queue open file metrics</h3>
          <Facts rows={Object.entries(extra.ra_open_file_metrics ?? {}).map(([k, v]) => [k, v])} />
        </div>
      </div>
      <div className="grid-2">
        <ChartBlock title="GC operations" series={series([['GC', 'gc_num']])} kind="rate" />
        <ChartBlock title="GC bytes reclaimed" series={series([['GC bytes reclaimed', 'gc_bytes_reclaimed']])} kind="rate" bytes />
        <ChartBlock title="Context switch operations" series={series([['Context switches', 'context_switches']])} kind="rate" />
      </div>
      <h3>Plugins</h3>
      <Applications applications={plugins} />
      <h3>All applications</h3>
      <Applications applications={n.applications} />
      <h3>Exchange types</h3>
      <Registry entries={n.exchange_types} showEnabled={false} />
      <h3>Authentication mechanisms</h3>
      <Registry entries={n.auth_mechanisms} showEnabled />
    </div>
  )
}
