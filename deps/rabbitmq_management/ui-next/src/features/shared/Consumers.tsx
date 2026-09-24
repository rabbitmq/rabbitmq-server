import type { ConsumerDetails } from '../../api/types/queues'
import { ChannelLink, ConnectionLink, QueueLink } from '../../components/Links'
import { Bool } from '../../components/Values'
import { fmtTableFlat } from '../../format/args'

/** The owner of a consumer: its channel, or its connection for channel-less protocols such as MQTT. */
function ConsumerOwner({ consumer }: { consumer: ConsumerDetails }) {
  const cd = consumer.channel_details
  if (consumer.consumer_tag.startsWith('stream.subid-')) return <ConnectionLink name={cd.connection_name} />
  if (cd.name) return <ChannelLink name={cd.name} />
  if (cd.connection_name) return <ConnectionLink name={cd.connection_name} />
  return <span className="unknown">?</span>
}

export function ConsumersTable({ consumers, mode }: { consumers: ConsumerDetails[]; mode: 'queue' | 'channel' }) {
  if (consumers.length === 0) return <p className="muted">No consumers</p>
  return (
    <div className="table-wrap">
      <table className="list" data-testid="consumers-table">
        <thead>
          <tr>
            {mode === 'queue' ? (
              <>
                <th>Owner</th>
                <th>Consumer tag</th>
              </>
            ) : (
              <>
                <th>Consumer tag</th>
                <th>Queue</th>
              </>
            )}
            <th>Ack required</th>
            <th>Exclusive</th>
            <th className="num">Prefetch count</th>
            <th>Active</th>
            <th>Activity status</th>
            <th className="num">Consumer timeout</th>
            <th>Arguments</th>
          </tr>
        </thead>
        <tbody>
          {consumers.map((consumer) => (
            <tr key={`${consumer.channel_details.name}|${consumer.consumer_tag}`}>
              {mode === 'queue' ? (
                <>
                  <td>
                    <ConsumerOwner consumer={consumer} />
                  </td>
                  <td className="mono">{consumer.consumer_tag}</td>
                </>
              ) : (
                <>
                  <td className="mono">{consumer.consumer_tag}</td>
                  <td>
                    <QueueLink vhost={consumer.queue.vhost} name={consumer.queue.name} />
                  </td>
                </>
              )}
              <td>
                <Bool value={consumer.ack_required} />
              </td>
              <td>
                <Bool value={consumer.exclusive} />
              </td>
              <td className="num">{consumer.prefetch_count}</td>
              <td>
                <Bool value={consumer.active} />
              </td>
              <td>{consumer.activity_status?.replace('_', ' ')}</td>
              <td className="num">{consumer.consumer_timeout}</td>
              <td>{fmtTableFlat(consumer.arguments ?? {})}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
