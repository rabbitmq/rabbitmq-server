import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useNavigate, useParams } from '@tanstack/react-router'
import { closeConnection, connectionChannelsQuery, connectionQuery, connectionSessionsQuery } from '../../api/resources/connections'
import type { AmqpLink, AmqpSession, Connection } from '../../api/types/connections'
import { useAppData } from '../../app/context'
import { useRefetchInterval } from '../../app/refresh'
import { ChartRangeSelect, useChartRange } from '../../charts/ChartRangeSelect'
import { dataRateSeries } from '../../charts/stats'
import { DataTable } from '../../components/DataTable'
import { confirmAction, useApiMutation } from '../../components/mutation'
import { PageHeader } from '../../components/PageHeader'
import { Section } from '../../components/Section'
import { ObjectState } from '../../components/State'
import { ErrorMessage, Loading } from '../../components/Status'
import { AmqpTableView, Bool, Facts, Timestamp } from '../../components/Values'
import { ChartBlock } from '../shared/ChartBlock'
import { RuntimeMetrics } from '../shared/RuntimeMetrics'
import { channelColumns } from '../channels/channelColumns'

const isAmqp10 = (c: Connection) => c.protocol === 'AMQP 1-0' || c.protocol === 'Web AMQP 1-0'

export function ConnectionPage() {
  const { name } = useParams({ from: '/connections/$name' })
  const { stats } = useAppData()
  const range = useChartRange()
  const connection = useQuery({ ...connectionQuery(name, range), refetchInterval: useRefetchInterval('stats') })
  const header = (
    <PageHeader
      kind="Connection"
      title={name}
      documentTitle={`Connection ${name}`}
      meta={connection.data ? <span>Virtual host <strong>{connection.data.vhost}</strong></span> : null}
    />
  )
  if (!connection.data) return <>{header}{connection.error ? <ErrorMessage error={connection.error} what={`Connection ${name}`} /> : <Loading />}</>
  const c = connection.data
  const props = c.client_properties && !Array.isArray(c.client_properties) ? c.client_properties : {}
  return (
    <>
      {header}
      {connection.error ? <ErrorMessage error={connection.error} what="the latest connection state" /> : null}
      <Section id="connection-overview" title="Overview" actions={stats.disabled ? null : <ChartRangeSelect />}>
        <div className="stack">
          {!stats.disabled ? <ChartBlock title="Data rates" series={dataRateSeries(c)} kind="rate" bytes testId="chart-connection-rates" /> : null}
          <div className="grid-2">
            <Facts
              rows={[
                ['Node', c.node],
                ...(typeof props.connection_name === 'string' ? ([['Client-provided connection name', props.connection_name]] as [string, React.ReactNode][]) : []),
                ...(c.container_id ? ([['Container ID', c.container_id]] as [string, React.ReactNode][]) : []),
                ['Username', c.user],
                ['Protocol', c.protocol],
                ['Connected at', <Timestamp value={c.connected_at} key="t" />],
                ...(c.ssl ? ([['TLS', <Bool value key="s" />]] as [string, React.ReactNode][]) : []),
                ...(c.auth_mechanism ? ([['SASL auth mechanism', c.auth_mechanism]] as [string, React.ReactNode][]) : []),
              ]}
            />
            <Facts
              rows={[
                ...(c.state ? ([['State', <ObjectState obj={c} key="s" />]] as [string, React.ReactNode][]) : []),
                ['Heartbeat', c.timeout !== undefined ? `${c.timeout}s` : ''],
                ['Frame max', `${c.frame_max} bytes`],
                ['Channel limit', `${c.channel_max} channels`],
              ]}
            />
          </div>
        </div>
      </Section>
      {!stats.disabled ? (
        isAmqp10(c) ? (
          <Section id="connection-sessions" title="Sessions">
            <Sessions name={name} />
          </Section>
        ) : (
          <Section id="connection-channels" title="Channels">
            <ConnectionChannels name={name} />
          </Section>
        )
      ) : null}
      {c.ssl ? (
        <Section id="connection-tls" title="TLS">
          <Facts
            rows={[
              ['Protocol version', c.ssl_protocol],
              ['Key exchange algorithm', c.ssl_key_exchange],
              ['Cipher algorithm', c.ssl_cipher],
              ['Hash algorithm', c.ssl_hash],
              ...(c.peer_cert_issuer
                ? ([
                    ['Peer certificate issuer', c.peer_cert_issuer],
                    ['Peer certificate subject', c.peer_cert_subject],
                    ['Peer certificate validity', c.peer_cert_validity],
                  ] as [string, React.ReactNode][])
                : []),
            ]}
          />
        </Section>
      ) : null}
      {Object.keys(props).length > 0 ? (
        <Section id="connection-client-properties" title="Client properties" defaultOpen={false}>
          <AmqpTableView table={props} />
        </Section>
      ) : null}
      {!stats.disabled && (c.reductions !== undefined || c.garbage_collection) ? (
        <Section id="connection-runtime" title="Runtime Metrics (Advanced)" defaultOpen={false}>
          <RuntimeMetrics obj={c} />
        </Section>
      ) : null}
      <Section id="connection-close" title="Close this connection" defaultOpen={false}>
        <CloseConnection name={name} />
      </Section>
    </>
  )
}

function ConnectionChannels({ name }: { name: string }) {
  const { stats } = useAppData()
  const channels = useQuery({ ...connectionChannelsQuery(name), refetchInterval: useRefetchInterval('stats') })
  if (!channels.data) return channels.error ? <ErrorMessage error={channels.error} what="channels" /> : <Loading />
  return <DataTable mode="channels" columns={channelColumns(stats, { showVhost: false, showNode: false })} rows={channels.data} rowKey={(ch) => ch.name} />
}

function Sessions({ name }: { name: string }) {
  const sessions = useQuery({ ...connectionSessionsQuery(name), refetchInterval: useRefetchInterval('stats') })
  if (!sessions.data) return sessions.error ? <ErrorMessage error={sessions.error} what="sessions" /> : <Loading />
  if (sessions.data.length === 0) return <p className="muted">No sessions</p>
  return (
    <div className="stack">
      {sessions.data.map((session) => (
        <SessionView key={session.channel_number} session={session} />
      ))}
    </div>
  )
}

function SessionView({ session: s }: { session: AmqpSession }) {
  const credit = (value: number | undefined) => (value === 0 ? { background: 'var(--warn-bg)' } : undefined)
  return (
    <div className="callout" data-testid="amqp-session">
      <Facts
        rows={[
          ['Channel number', s.channel_number],
          ['handle-max', s.handle_max],
          ['next-incoming-id', s.next_incoming_id],
          ['incoming-window', <span key="iw" style={credit(s.incoming_window)}>{s.incoming_window}</span>],
          ['next-outgoing-id', s.next_outgoing_id],
          ['remote-incoming-window', <span key="riw" style={credit(s.remote_incoming_window)}>{s.remote_incoming_window}</span>],
          ['remote-outgoing-window', s.remote_outgoing_window],
          ['Outgoing unsettled deliveries', s.outgoing_unsettled_deliveries],
        ]}
      />
      {s.incoming_links.length > 0 ? <Links title={`Incoming links (${s.incoming_links.length})`} links={s.incoming_links} incoming /> : null}
      {s.outgoing_links.length > 0 ? <Links title={`Outgoing links (${s.outgoing_links.length})`} links={s.outgoing_links} incoming={false} /> : null}
    </div>
  )
}

function Links({ title, links, incoming }: { title: string; links: AmqpLink[]; incoming: boolean }) {
  const ext = (link: AmqpLink) => link as AmqpLink & { consumer_timeout?: boolean; filter?: { name: string; descriptor: string; value: unknown }[] }
  return (
    <>
      <h3>{title}</h3>
      <div className="table-wrap">
        <table className="list">
          <thead>
            <tr>
              <th>Link handle</th>
              <th>Link name</th>
              <th>{incoming ? 'Target address' : 'Source address'}</th>
              {incoming ? <th>snd-settle-mode</th> : (
                <>
                  <th>Source queue</th>
                  <th>Sender settles</th>
                </>
              )}
              <th>max-message-size (bytes)</th>
              <th>delivery-count</th>
              <th>link-credit</th>
              {incoming ? <th>Unconfirmed messages</th> : (
                <>
                  <th>Consumer timeout</th>
                  <th>Filters</th>
                </>
              )}
            </tr>
          </thead>
          <tbody>
            {links.map((link) => (
              <tr key={link.handle}>
                <td>{link.handle}</td>
                <td className="mono">{link.link_name}</td>
                <td className="mono">{incoming ? link.target_address : link.source_address}</td>
                {incoming ? <td>{link.snd_settle_mode}</td> : (
                  <>
                    <td>{link.queue_name}</td>
                    <td><Bool value={link.send_settled} /></td>
                  </>
                )}
                <td className="num">{link.max_message_size}</td>
                <td className="num">{link.delivery_count}</td>
                <td className="num" style={link.credit === 0 ? { background: 'var(--warn-bg)' } : undefined}>{link.credit}</td>
                {incoming ? <td className="num">{link.unconfirmed_messages}</td> : (
                  <>
                    <td><Bool value={ext(link).consumer_timeout ?? false} /></td>
                    <td>
                      {(ext(link).filter ?? []).map((f) => (
                        <abbr key={f.name} className="tag" title={`(descriptor: ${f.descriptor}) ${JSON.stringify(f.value)}`}>
                          {f.name}
                        </abbr>
                      ))}
                    </td>
                  </>
                )}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  )
}

function CloseConnection({ name }: { name: string }) {
  const navigate = useNavigate()
  const [reason, setReason] = useState('Closed via management plugin')
  const close = useApiMutation({
    mutationFn: () => closeConnection(name, reason),
    success: 'Connection closed',
    invalidate: [['connections']],
    onSuccess: () => navigate({ to: '/connections' }),
  })
  return (
    <form
      className="form"
      onSubmit={(event) => {
        event.preventDefault()
        if (confirmAction('Are you sure? This object cannot be recovered after deletion.')) close.mutate()
      }}
    >
      <label htmlFor="close-reason">Reason</label>
      <input id="close-reason" type="text" value={reason} onChange={(e) => setReason(e.target.value)} />
      <div className="actions">
        <button type="submit" className="btn btn-danger" disabled={close.isPending} data-testid="close-connection">
          Force close
        </button>
      </div>
    </form>
  )
}
