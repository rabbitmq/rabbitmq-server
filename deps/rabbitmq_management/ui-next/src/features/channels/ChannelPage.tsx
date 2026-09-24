import { useQuery } from '@tanstack/react-query'
import { useParams } from '@tanstack/react-router'
import { channelQuery } from '../../api/resources/connections'
import { useAppData } from '../../app/context'
import { useRefetchInterval } from '../../app/refresh'
import { ChartRangeSelect, useChartRange } from '../../charts/ChartRangeSelect'
import { messageRateSeries } from '../../charts/stats'
import { ConnectionLink } from '../../components/Links'
import { PageHeader } from '../../components/PageHeader'
import { Section } from '../../components/Section'
import { ObjectState } from '../../components/State'
import { ErrorMessage, Loading } from '../../components/Status'
import { Facts } from '../../components/Values'
import { ChartBlock } from '../shared/ChartBlock'
import { ConsumersTable } from '../shared/Consumers'
import { DeliveriesTable, PublishesTable } from '../shared/MessageStatsTables'
import { RuntimeMetrics } from '../shared/RuntimeMetrics'
import { ChannelMode } from './channelColumns'

export function ChannelPage() {
  const { name } = useParams({ from: '/channels/$name' })
  const { stats } = useAppData()
  const range = useChartRange()
  const channel = useQuery({ ...channelQuery(name, range), refetchInterval: useRefetchInterval('stats') })
  const header = (
    <PageHeader
      kind="Channel"
      title={name}
      documentTitle={`Channel ${name}`}
      meta={channel.data ? <span>Virtual host <strong>{channel.data.vhost}</strong></span> : null}
    />
  )
  if (!channel.data) return <>{header}{channel.error ? <ErrorMessage error={channel.error} what={`Channel ${name}`} /> : <Loading />}</>
  const c = channel.data
  return (
    <>
      {header}
      {channel.error ? <ErrorMessage error={channel.error} what="the latest channel state" /> : null}
      <Section id="channel-overview" title="Overview" actions={stats.hasRates ? <ChartRangeSelect /> : null}>
        <div className="grid-2">
          {stats.hasRates ? <ChartBlock title="Message rates" series={messageRateSeries(c.message_stats)} kind="rate" testId="chart-channel-rates" /> : null}
          <div>
            <h3>Details</h3>
            <Facts
              rows={[
                ['Connection', c.connection_details ? <ConnectionLink name={c.connection_details.name} key="c" /> : null],
                ['Node', c.node],
                ['Username', c.user],
                ['Mode', <ChannelMode channel={c} key="m" />],
                ['State', <ObjectState obj={c} key="s" />],
                ['Prefetch count', c.prefetch_count],
                ...(c.global_prefetch_count ? ([['Global prefetch count', c.global_prefetch_count]] as [string, React.ReactNode][]) : []),
                ['Messages unacknowledged', c.messages_unacknowledged],
                ['Messages unconfirmed', c.messages_unconfirmed],
                ['Messages uncommitted', c.messages_uncommitted],
                ['Acks uncommitted', c.acks_uncommitted],
                ['Pending Raft commands', c.pending_raft_commands],
                ['Cached segments', c.cached_segments],
              ]}
            />
          </div>
        </div>
      </Section>
      {!stats.disabled ? (
        <Section id="channel-consumers" title={`Consumers (${c.consumer_details?.length ?? 0})`}>
          <ConsumersTable consumers={c.consumer_details ?? []} mode="channel" />
        </Section>
      ) : null}
      {stats.ratesMode === 'detailed' && !stats.disabled ? (
        <Section id="channel-rates-breakdown" title="Message rates breakdown" defaultOpen={false}>
          <div className="grid-2">
            <PublishesTable rows={c.publishes} by="exchange" label="Publishes" />
            <DeliveriesTable rows={c.deliveries} by="queue" />
          </div>
        </Section>
      ) : null}
      {!stats.disabled && (c.reductions !== undefined || c.garbage_collection) ? (
        <Section id="channel-runtime" title="Runtime Metrics (Advanced)" defaultOpen={false}>
          <RuntimeMetrics obj={c} />
        </Section>
      ) : null}
    </>
  )
}
