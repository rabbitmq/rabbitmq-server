import { describe, expect, it } from 'vitest'
import type { Queue } from '../../api/types/queues'
import fixture from '../../../test/fixtures/queue-classic.json'
import { queueVerdict } from './verdict'

describe('queueVerdict', () => {
  it('reports a filling queue without consumers', () => {
    expect(queueVerdict(fixture as unknown as Queue, true)?.text).toMatch(/^Filling/)
  })
})

describe('queueVerdict without data worth acting on', () => {
  const base = { name: 'q', vhost: '/', type: 'classic', durable: true, auto_delete: false, arguments: {} } as Queue

  it('says nothing about an empty queue', () => {
    expect(queueVerdict({ ...base, messages: 0, consumers: 0 }, true)).toBeUndefined()
  })

  it('says nothing about streams, which are not drained by consumers', () => {
    expect(queueVerdict({ ...base, type: 'stream', messages: 100, consumers: 0 }, true)).toBeUndefined()
  })

  it('reports failed queues', () => {
    expect(queueVerdict({ ...base, state: 'down' }, true)?.severity).toBe('bad')
  })
})
