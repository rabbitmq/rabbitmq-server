import type { Channel } from '../../api/types/connections'
import type { StatsMode } from '../../app/context'
import type { ColumnSpec } from '../../components/columns'
import { ChannelLink } from '../../components/Links'
import { ObjectState } from '../../components/State'
import { Num, Rate } from '../../components/Values'

export function ChannelMode({ channel }: { channel: Channel }) {
  if (channel.transactional) return <abbr className="tag" title="Transactional">T</abbr>
  if (channel.confirm) return <abbr className="tag" title="Confirm">C</abbr>
  return null
}

/** The columns of channels-list.ejs, with the classic UI's column keys. */
export function channelColumns(stats: StatsMode, opts: { showVhost: boolean; showNode: boolean }): ColumnSpec<Channel>[] {
  const optional = (id: string, label: string, defaultVisible: boolean) => ({ id, optional: { label, defaultVisible } })
  const columns: ColumnSpec<Channel>[] = [
    { id: 'name', group: 'Overview', header: 'Channel', sortKey: 'name', cell: (c) => <ChannelLink name={c.name} /> },
  ]
  if (opts.showNode) columns.push({ id: 'node', group: 'Overview', header: 'Node', sortKey: 'node', cell: (c) => c.node })
  if (opts.showVhost) columns.push({ id: 'vhost', group: 'Overview', header: 'Virtual host', sortKey: 'vhost', cell: (c) => c.vhost })
  columns.push({ ...optional('user', 'User name', true), group: 'Overview', header: 'User name', sortKey: 'user', cell: (c) => c.user })
  if (stats.disabled) return columns
  columns.push(
    { ...optional('mode', 'Mode', true), group: 'Overview', header: 'Mode', cell: (c) => <ChannelMode channel={c} /> },
    { ...optional('state', 'State', true), group: 'Overview', header: 'State', sortKey: 'state', cell: (c) => <ObjectState obj={c} /> },
    { ...optional('msgs-unconfirmed', 'Unconfirmed', true), group: 'Details', header: 'Unconfirmed', sortKey: 'messages_unconfirmed', numeric: true, cell: (c) => <Num value={c.messages_unconfirmed} /> },
    { ...optional('consumer-count', 'Consumer count', false), group: 'Details', header: 'Consumer count', sortKey: 'consumer_count', numeric: true, cell: (c) => <Num value={c.consumer_count} /> },
    {
      ...optional('prefetch', 'Prefetch', true),
      group: 'Details',
      header: 'Prefetch',
      sortKey: 'prefetch_count',
      numeric: true,
      cell: (c) => (
        <>
          {c.prefetch_count !== 0 ? c.prefetch_count : null}
          {c.global_prefetch_count ? <span className="hint"> {c.global_prefetch_count} (global)</span> : null}
        </>
      ),
    },
    { ...optional('msgs-unacked', 'Unacked', true), group: 'Details', header: 'Unacked', sortKey: 'messages_unacknowledged', numeric: true, cell: (c) => <Num value={c.messages_unacknowledged} /> },
    { ...optional('msgs-uncommitted', 'Msgs uncommitted', false), group: 'Transactions', header: 'Msgs uncommitted', sortKey: 'messages_uncommitted', numeric: true, cell: (c) => <Num value={c.messages_uncommitted} /> },
    { ...optional('acks-uncommitted', 'Acks uncommitted', false), group: 'Transactions', header: 'Acks uncommitted', sortKey: 'acks_uncommitted', numeric: true, cell: (c) => <Num value={c.acks_uncommitted} /> },
  )
  if (!stats.hasRates) return columns
  const rate = (id: string, label: string, stat: string, defaultVisible: boolean): ColumnSpec<Channel> => ({
    ...optional(id, label, defaultVisible),
    group: 'Message rates',
    header: label,
    sortKey: `message_stats.${stat}_details.rate`,
    numeric: true,
    cell: (c) => <Rate details={(c.message_stats as Record<string, never> | undefined)?.[`${stat}_details`]} />,
  })
  columns.push(
    rate('rate-publish', 'publish', 'publish', true),
    rate('rate-confirm', 'confirm', 'confirm', true),
    rate('rate-unroutable-drop', 'unroutable (drop)', 'drop_unroutable', true),
    rate('rate-unroutable-return', 'unroutable (return)', 'return_unroutable', false),
    rate('rate-deliver', 'deliver / get', 'deliver_get', true),
    rate('rate-redeliver', 'redelivered', 'redeliver', false),
    rate('rate-ack', 'ack', 'ack', true),
  )
  return columns
}
