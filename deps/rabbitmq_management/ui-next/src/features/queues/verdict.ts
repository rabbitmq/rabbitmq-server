import type { Queue } from '../../api/types/queues'

export interface Verdict {
  /** `info` states a fact that is often intended, such as a parked dead-letter queue. */
  severity: 'info' | 'warn' | 'bad'
  text: string
}

/** Describes the queue's state in one sentence, for the states that need attention. */
export function queueVerdict(queue: Queue, hasRates: boolean): Verdict | undefined {
  if (queue.state && ['down', 'crashed', 'stopped'].includes(queue.state)) {
    return { severity: 'bad', text: `The queue is ${queue.state}: it cannot accept or deliver messages.` }
  }
  if (queue.state === 'minority') {
    return { severity: 'bad', text: 'The queue has too few online members to make progress.' }
  }
  if (queue.type === 'stream') return undefined
  const depth = queue.messages ?? 0
  const consumers = queue.consumers ?? queue.consumer_details?.length ?? 0
  // The instantaneous rate compares only the last two samples and often reads
  // zero between publishes; the average over the chart range is steadier.
  const growth = queue.messages_details?.avg_rate ?? queue.messages_details?.rate ?? 0
  if (depth > 0 && consumers === 0) {
    return growth > 0
      ? { severity: 'warn', text: 'Filling, with no consumer: messages will accumulate until one attaches.' }
      : { severity: 'info', text: 'Messages are waiting, but no consumer is attached.' }
  }
  if (hasRates && consumers > 0 && growth > 0 && depth > 0) {
    const incoming = queue.message_stats?.publish_details?.rate ?? 0
    const outgoing = queue.message_stats?.deliver_get_details?.rate ?? 0
    if (incoming > outgoing) return { severity: 'warn', text: 'Filling: messages arrive faster than consumers take them.' }
  }
  return undefined
}
