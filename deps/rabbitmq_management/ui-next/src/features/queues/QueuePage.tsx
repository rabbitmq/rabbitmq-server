import { useState, type FormEvent } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useNavigate, useParams } from '@tanstack/react-router'
import { deleteQueue, getMessages, purgeQueue, queueBindingsQuery, queueQuery } from '../../api/resources/queues'
import type { GetMessagesRequest, Queue, RetrievedMessage } from '../../api/types/queues'
import { useAppData } from '../../app/context'
import { useRefetchInterval } from '../../app/refresh'
import { ChartRangeSelect, useChartRange } from '../../charts/ChartRangeSelect'
import { messageRateSeries, queueLengthSeries } from '../../charts/stats'
import { ConnectionLink, PolicyLink } from '../../components/Links'
import { confirmAction, useApiMutation } from '../../components/mutation'
import { PageHeader } from '../../components/PageHeader'
import { Section } from '../../components/Section'
import { ObjectState } from '../../components/State'
import { ErrorMessage, Loading } from '../../components/Status'
import { AmqpTableView, Bool, Bytes, Facts, FeatureTags, Num } from '../../components/Values'
import { exchangeName } from '../../format/names'
import { fmtPercent } from '../../format/numbers'
import { fmtTimestamp } from '../../format/time'
import { AddBindingForm, BindingsTable } from '../shared/Bindings'
import { ChartBlock } from '../shared/ChartBlock'
import { ConsumersTable } from '../shared/Consumers'
import { DeliveriesTable, PublishesTable } from '../shared/MessageStatsTables'
import { PublishForm } from '../shared/Publish'
import { RuntimeMetrics } from '../shared/RuntimeMetrics'
import { queueTypeInfo } from './queueTypes'
import { queueVerdict } from './verdict'

export function QueuePage() {
  const { vhost, name } = useParams({ from: '/queues/$vhost/$name' })
  const { stats } = useAppData()
  const range = useChartRange()
  const queue = useQuery({ ...queueQuery(vhost, name, range), refetchInterval: useRefetchInterval('stats') })

  const header = (
    <PageHeader
      kind="Queue"
      title={name}
      documentTitle={`Queue ${name}`}
      meta={
        <>
          <span>
            Virtual host <strong>{vhost}</strong>
          </span>
          {queue.data ? (
            <>
              <span className="tag">{queue.data.type}</span>
              <ObjectState obj={queue.data} />
            </>
          ) : null}
        </>
      }
    />
  )
  if (!queue.data) {
    return (
      <>
        {header}
        {queue.error ? <ErrorMessage error={queue.error} what={`Queue ${name}`} /> : <Loading />}
      </>
    )
  }
  const q = queue.data
  const type = queueTypeInfo(q.type)
  const verdict = stats.disabled ? undefined : queueVerdict(q, stats.hasRates)

  return (
    <>
      {header}
      {queue.error ? <ErrorMessage error={queue.error} what="the latest queue state" /> : null}
      {verdict ? (
        <div className={verdict.severity === 'info' ? 'callout' : `callout callout-${verdict.severity}`} data-testid="queue-verdict">
          {verdict.text}
        </div>
      ) : null}

      <Section id="queue-overview" title="Overview" actions={stats.disabled ? null : <ChartRangeSelect />}>
        <div className="stack">
          {!stats.disabled ? (
            <div className="grid-2">
              <ChartBlock title="Queued messages" series={queueLengthSeries(q)} kind="gauge" testId="chart-queue-lengths" />
              {stats.hasRates ? <ChartBlock title="Message rates" series={messageRateSeries(q.message_stats)} kind="rate" testId="chart-queue-rates" /> : null}
            </div>
          ) : null}
          <div className="grid-2">
            <div>
              <h3>Details</h3>
              <QueueDetails queue={q} />
            </div>
            {!stats.disabled ? (
              <div>
                <h3>Statistics</h3>
                <QueueStats queue={q} />
              </div>
            ) : null}
          </div>
        </div>
      </Section>

      {stats.ratesMode === 'detailed' && !stats.disabled ? (
        <Section id="queue-rates-breakdown" title="Message rates breakdown" defaultOpen={false}>
          <div className="grid-2">
            <PublishesTable rows={q.incoming} by="exchange" label="Incoming" />
            <DeliveriesTable rows={q.deliveries} by="channel" />
          </div>
        </Section>
      ) : null}

      {!stats.disabled ? (
        <Section id="queue-consumers" title={`Consumers (${q.consumer_details?.length ?? 0})`} defaultOpen={false}>
          <ConsumersTable consumers={q.consumer_details ?? []} mode="queue" />
          {(q.consumer_details?.length ?? 0) === 0 ? (
            <p className="hint">Messages will accumulate in this queue until a consumer attaches.</p>
          ) : null}
        </Section>
      ) : null}

      <Section id="queue-bindings" title="Bindings" defaultOpen={false}>
        <QueueBindings vhost={vhost} name={name} />
      </Section>

      <Section id="queue-publish" title="Publish message" defaultOpen={false}>
        <PublishForm mode="queue" vhost={vhost} queue={name} classic={q.type === 'classic'} />
      </Section>

      {type.canGet ? (
        <Section id="queue-get" title="Get messages" defaultOpen={false}>
          <GetMessages vhost={vhost} name={name} />
        </Section>
      ) : null}

      {!q.internal ? (
        <Section id="queue-delete" title="Delete" defaultOpen={false}>
          <DeleteQueue vhost={vhost} name={name} />
        </Section>
      ) : null}

      {type.canPurge ? (
        <Section id="queue-purge" title="Purge" defaultOpen={false}>
          <PurgeQueue vhost={vhost} name={name} />
        </Section>
      ) : null}

      {q.reductions !== undefined || q.garbage_collection ? (
        <Section id="queue-runtime" title="Runtime Metrics (Advanced)" defaultOpen={false}>
          <RuntimeMetrics obj={q} />
        </Section>
      ) : null}
    </>
  )
}

function QueueDetails({ queue: q }: { queue: Queue }) {
  const { stats } = useAppData()
  const rows: [React.ReactNode, React.ReactNode][] = [['Features', <FeatureTags obj={q} key="f" />]]
  if (!stats.disabled) {
    rows.push(
      ['Policy', q.policy ? <PolicyLink vhost={q.vhost} name={q.policy} /> : null],
      ['Operator policy', q.operator_policy ?? ''],
      ...(q.owner_pid_details ? ([['Exclusive owner', <ConnectionLink name={q.owner_pid_details.name} key="o" />]] as [string, React.ReactNode][]) : []),
      ['Effective policy definition', <AmqpTableView table={q.effective_policy_definition} key="e" />],
    )
  }
  if (Object.keys(q.arguments ?? {}).length > 0) rows.push(['Arguments', <AmqpTableView table={q.arguments} key="a" />])
  if (q.leader || q.members) {
    rows.push(
      ['Leader', q.leader],
      ['Online', (q.online ?? []).join(', ')],
      ['Members', (q.members ?? []).join(', ')],
    )
  } else if (q.node) {
    rows.push(['Node', q.node])
  }
  return <Facts rows={rows} />
}

function QueueStats({ queue: q }: { queue: Queue }) {
  const consumers = q.consumers || q.consumer_details?.length
  const facts: [React.ReactNode, React.ReactNode][] = [
    ['State', <ObjectState obj={q} key="s" />],
    ['Consumers', consumers ?? 0],
    ...(q.publishers !== undefined ? ([['Publishers', q.publishers]] as [string, React.ReactNode][]) : []),
  ]
  if (q.type === 'stream') {
    facts.push(
      ['Readers', <AmqpTableView table={q.readers} key="r" />],
      ['Segments', q.segments],
      ['Oldest message', fmtTimestamp(q.first_timestamp)],
      ['Messages', <Num value={q.messages} key="m" />],
      ['Process memory', <Bytes value={q.memory} key="pm" />],
    )
    return <Facts rows={facts} />
  }
  if (q.type === 'quorum') {
    facts.push(['Open files', <AmqpTableView table={q.open_files} key="o" />])
    if (Object.keys(q.messages_by_priority ?? {}).length > 0) facts.push(['Messages by priority', <AmqpTableView table={q.messages_by_priority} key="p" />])
    if (q.next_delayed_at) facts.push(['Next delayed retry', fmtTimestamp(q.next_delayed_at)])
    if (q.last_delayed_at) facts.push(['Last delayed retry', fmtTimestamp(q.last_delayed_at)])
    if (q.delivery_limit !== undefined) facts.push(['Delivery limit', q.delivery_limit])
  } else {
    facts.push(['Consumer capacity', fmtPercent(q.consumer_capacity)])
  }
  const quorum = q.type === 'quorum'
  return (
    <div className="stack">
      <Facts rows={facts} />
      <div className="table-wrap">
        <table className="list" data-testid="queue-message-counts">
          <thead>
            <tr>
              <th />
              <th className="num">Total</th>
              <th className="num">Ready</th>
              <th className="num">Unacked</th>
              {quorum ? (
                <>
                  <th className="num">Returned</th>
                  <th className="num">Delayed</th>
                  <th className="num">Dead-lettered</th>
                </>
              ) : (
                <>
                  <th className="num">In memory</th>
                  <th className="num">Persistent</th>
                </>
              )}
            </tr>
          </thead>
          <tbody>
            <tr>
              <th>Messages</th>
              <td className="num"><Num value={q.messages} /></td>
              <td className="num"><Num value={q.messages_ready} /></td>
              <td className="num"><Num value={q.messages_unacknowledged} /></td>
              {quorum ? (
                <>
                  <td className="num"><Num value={q.messages_ready_returned} /></td>
                  <td className="num"><Num value={q.messages_delayed} /></td>
                  <td className="num"><Num value={q.messages_dlx} /></td>
                </>
              ) : (
                <>
                  <td className="num"><Num value={q.messages_ram} /></td>
                  <td className="num"><Num value={q.messages_persistent} /></td>
                </>
              )}
            </tr>
            <tr>
              <th>Message body bytes</th>
              <td className="num"><Bytes value={q.message_bytes} /></td>
              <td className="num"><Bytes value={q.message_bytes_ready} /></td>
              <td className="num"><Bytes value={q.message_bytes_unacknowledged} /></td>
              {quorum ? (
                <>
                  <td />
                  <td />
                  <td className="num"><Bytes value={q.message_bytes_dlx} /></td>
                </>
              ) : (
                <>
                  <td className="num"><Bytes value={q.message_bytes_ram} /></td>
                  <td className="num"><Bytes value={q.message_bytes_persistent} /></td>
                </>
              )}
            </tr>
            <tr>
              <th>Process memory</th>
              <td className="num"><Bytes value={q.memory} /></td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  )
}

function QueueBindings({ vhost, name }: { vhost: string; name: string }) {
  const bindings = useQuery({ ...queueBindingsQuery(vhost, name), refetchInterval: useRefetchInterval('topology') })
  return (
    <div className="stack">
      {bindings.error ? <ErrorMessage error={bindings.error} what="bindings" /> : null}
      {bindings.data ? <BindingsTable bindings={bindings.data} mode="queue" /> : <Loading />}
      <p className="muted">⇓ This queue</p>
      <AddBindingForm vhost={vhost} name={name} mode="queue" />
    </div>
  )
}

function GetMessages({ vhost, name }: { vhost: string; name: string }) {
  const [request, setRequest] = useState<GetMessagesRequest>({ count: 1, ackmode: 'ack_requeue_true', encoding: 'auto', truncate: 50000 })
  const [messages, setMessages] = useState<RetrievedMessage[] | undefined>()
  const get = useApiMutation<GetMessagesRequest, RetrievedMessage[]>({
    mutationFn: (r) => getMessages(vhost, name, r),
    invalidate: [['queues']],
    onSuccess: (result) => setMessages(result),
  })
  const submit = (event: FormEvent) => {
    event.preventDefault()
    get.mutate(request)
  }
  return (
    <div className="stack">
      <p>Warning: getting messages from a queue is a destructive action. Requeued messages are marked as redelivered.</p>
      <form className="form" onSubmit={submit} data-testid="get-messages-form">
        <label htmlFor="get-ackmode">Ack mode</label>
        <select id="get-ackmode" value={request.ackmode} onChange={(e) => setRequest({ ...request, ackmode: e.target.value as GetMessagesRequest['ackmode'] })}>
          <option value="ack_requeue_true">Nack message requeue true</option>
          <option value="ack_requeue_false">Automatic ack</option>
          <option value="reject_requeue_true">Reject requeue true</option>
          <option value="reject_requeue_false">Reject requeue false</option>
        </select>
        <label htmlFor="get-encoding">Encoding</label>
        <select id="get-encoding" value={request.encoding} onChange={(e) => setRequest({ ...request, encoding: e.target.value as GetMessagesRequest['encoding'] })}>
          <option value="auto">Auto string / base64</option>
          <option value="base64">base64</option>
        </select>
        <label htmlFor="get-count">Messages</label>
        <input
          id="get-count"
          type="number"
          min={1}
          value={request.count}
          onChange={(e) => setRequest({ ...request, count: Math.max(1, parseInt(e.target.value, 10) || 1) })}
          data-testid="get-count"
        />
        <div className="actions">
          <button type="submit" className="btn btn-primary" disabled={get.isPending} data-testid="get-submit">
            Get message(s)
          </button>
        </div>
      </form>
      {messages !== undefined ? (
        messages.length === 0 ? (
          <p className="muted" data-testid="queue-empty">Queue is empty</p>
        ) : (
          messages.map((msg, i) => <MessageView key={i} index={i} msg={msg} />)
        )
      ) : null}
    </div>
  )
}

function MessageView({ msg, index }: { msg: RetrievedMessage; index: number }) {
  return (
    <div className="callout" data-testid="retrieved-message">
      <h3 style={{ marginTop: 0 }}>Message {index + 1}</h3>
      <p>
        The server reported <strong>{msg.message_count}</strong> messages remaining.
      </p>
      <Facts
        rows={[
          ['Exchange', exchangeName(msg.exchange)],
          ['Routing key', <span className="mono" key="rk">{msg.routing_key}</span>],
          ['Redelivered', <Bool value={msg.redelivered} key="r" />],
          ['Properties', <AmqpTableView table={msg.properties} key="p" />],
          [
            <>
              Payload
              <div className="hint">
                {msg.payload_bytes} bytes, encoding: {msg.payload_encoding}
              </div>
            </>,
            <pre className="payload" key="payload" data-testid="message-payload">
              {msg.payload}
            </pre>,
          ],
        ]}
      />
    </div>
  )
}

function DeleteQueue({ vhost, name }: { vhost: string; name: string }) {
  const navigate = useNavigate()
  const [ifEmpty, setIfEmpty] = useState(false)
  const [ifUnused, setIfUnused] = useState(false)
  const remove = useApiMutation({
    mutationFn: () => deleteQueue(vhost, name, { ifEmpty, ifUnused }),
    success: `Queue ${name} deleted`,
    invalidate: [['queues']],
    onSuccess: () => navigate({ to: '/queues' }),
  })
  return (
    <div className="row">
      <label className="row" style={{ gap: '0.3rem' }}>
        <input type="checkbox" checked={ifEmpty} onChange={(e) => setIfEmpty(e.target.checked)} /> Only if empty
      </label>
      <label className="row" style={{ gap: '0.3rem' }}>
        <input type="checkbox" checked={ifUnused} onChange={(e) => setIfUnused(e.target.checked)} /> Only if unused
      </label>
      <button
        type="button"
        className="btn btn-danger"
        disabled={remove.isPending}
        onClick={() => {
          if (confirmAction('Are you sure? The queue is going to be deleted. Messages cannot be recovered after deletion.')) remove.mutate()
        }}
        data-testid="delete-queue"
      >
        Delete queue
      </button>
    </div>
  )
}

function PurgeQueue({ vhost, name }: { vhost: string; name: string }) {
  const purge = useApiMutation({ mutationFn: () => purgeQueue(vhost, name), success: `Queue ${name} purged`, invalidate: [['queues']] })
  return (
    <button
      type="button"
      className="btn btn-danger"
      disabled={purge.isPending}
      onClick={() => {
        if (confirmAction('Are you sure? Messages cannot be recovered after purging.')) purge.mutate()
      }}
      data-testid="purge-queue"
    >
      Purge messages
    </button>
  )
}
