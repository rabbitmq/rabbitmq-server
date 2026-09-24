import type { MessageStats } from '../../api/types/common'
import { ChannelLink, ExchangeLink, QueueLink } from '../../components/Links'
import { Rate } from '../../components/Values'

interface Named {
  name: string
  vhost: string
}

type PublishRow = { exchange?: Named; queue?: Named; channel_details?: { name: string }; stats: MessageStats }

export function PublishesTable({ rows, by, label }: { rows: PublishRow[] | undefined; by: 'exchange' | 'channel' | 'queue'; label: string }) {
  return (
    <div>
      <h3>{label}</h3>
      {rows && rows.length > 0 ? (
        <div className="table-wrap">
          <table className="list">
            <thead>
              <tr>
                <th>{by === 'exchange' ? 'Exchange' : by === 'channel' ? 'Channel' : 'Queue'}</th>
                <th className="num">publish</th>
                {by !== 'queue' ? <th className="num">confirm</th> : null}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, i) => (
                <tr key={i}>
                  <td>
                    {by === 'exchange' && row.exchange ? <ExchangeLink vhost={row.exchange.vhost} name={row.exchange.name} /> : null}
                    {by === 'channel' && row.channel_details ? <ChannelLink name={row.channel_details.name} /> : null}
                    {by === 'queue' && row.queue ? <QueueLink vhost={row.queue.vhost} name={row.queue.name} /> : null}
                  </td>
                  <td className="num">
                    <Rate details={row.stats.publish_details} />
                  </td>
                  {by !== 'queue' ? (
                    <td className="num">
                      <Rate details={row.stats.confirm_details} />
                    </td>
                  ) : null}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <p className="muted">No publishes</p>
      )}
    </div>
  )
}

type DeliveryRow = { queue?: Named; channel_details?: { name: string }; stats: MessageStats }

export function DeliveriesTable({ rows, by }: { rows: DeliveryRow[] | undefined; by: 'channel' | 'queue' }) {
  return (
    <div>
      <h3>Deliveries</h3>
      {rows && rows.length > 0 ? (
        <div className="table-wrap">
          <table className="list">
            <thead>
              <tr>
                <th>{by === 'channel' ? 'Channel' : 'Queue'}</th>
                <th className="num">deliver / get</th>
                <th className="num">ack</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row, i) => (
                <tr key={i}>
                  <td>
                    {by === 'channel' && row.channel_details ? <ChannelLink name={row.channel_details.name} /> : null}
                    {by === 'queue' && row.queue ? <QueueLink vhost={row.queue.vhost} name={row.queue.name} /> : null}
                  </td>
                  <td className="num">
                    <Rate details={row.stats.deliver_get_details} />
                  </td>
                  <td className="num">
                    <Rate details={row.stats.ack_details} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <p className="muted">No deliveries</p>
      )}
    </div>
  )
}
