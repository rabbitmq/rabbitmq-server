import type { MessageStats, RateDetails } from '../api/types/common'
import type { ChartSeries } from './RateChart'

type WithDetails = object

function pick(stats: WithDetails | undefined, items: [string, string][]): ChartSeries[] {
  if (!stats) return []
  const obj = stats as Record<string, unknown>
  return items
    .filter(([, key]) => obj[key] !== undefined && obj[`${key}_details`] !== undefined)
    .map(([label, key]) => ({ key, label, details: obj[`${key}_details`] as RateDetails, value: obj[key] as number }))
}

export function messageRateSeries(stats: MessageStats | undefined): ChartSeries[] {
  return pick(stats, [
    ['Publish', 'publish'],
    ['Publisher confirm', 'confirm'],
    ['Publish (In)', 'publish_in'],
    ['Publish (Out)', 'publish_out'],
    ['Deliver (manual ack)', 'deliver'],
    ['Deliver (auto ack)', 'deliver_no_ack'],
    ['Consumer ack', 'ack'],
    ['Redelivered', 'redeliver'],
    ['Get (manual ack)', 'get'],
    ['Get (auto ack)', 'get_no_ack'],
    ['Get (empty)', 'get_empty'],
    ['Unroutable (return)', 'return_unroutable'],
    ['Unroutable (drop)', 'drop_unroutable'],
  ])
}

export function queueLengthSeries(stats: WithDetails | undefined): ChartSeries[] {
  return pick(stats, [
    ['Ready', 'messages_ready'],
    ['Unacked', 'messages_unacknowledged'],
    ['Total', 'messages'],
  ])
}

export function dataRateSeries(stats: WithDetails | undefined): ChartSeries[] {
  return pick(stats, [
    ['From client', 'recv_oct'],
    ['To client', 'send_oct'],
  ])
}

export function reductionSeries(stats: WithDetails | undefined): ChartSeries[] {
  return pick(stats, [['Reductions', 'reductions']])
}

export function pickSeries(stats: WithDetails | undefined, items: [string, string][]): ChartSeries[] {
  return pick(stats, items)
}
