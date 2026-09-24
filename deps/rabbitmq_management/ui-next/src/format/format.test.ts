import { describe, expect, it } from 'vitest'
import { featureTags, fmtTableFlat } from './args'
import { clientName, exchangeName, maskUriCredentials, shortChan, shortConn } from './names'
import { objectState, vhostState } from './state'
import { fmtUptime } from './time'

describe('fmtUptime', () => {
  it('uses the two largest units', () => {
    expect(fmtUptime(42_000)).toBe('0m 42s')
    expect(fmtUptime(3 * 3600_000 + 5 * 60_000)).toBe('3h 5m')
    expect(fmtUptime(2 * 86400_000 + 4 * 3600_000)).toBe('2d 4h')
  })
})

describe('objectState', () => {
  it('reports idle objects as idle', () => {
    expect(objectState({ state: 'running', idle_since: '2026-09-24T10:00:00Z' })).toMatchObject({ colour: 'grey', text: 'idle' })
  })

  it('explains alarm and failure states', () => {
    expect(objectState({ state: 'blocked' })).toMatchObject({ colour: 'red', explanation: 'Resource alarm: connection blocked.' })
    expect(objectState({ state: 'minority' })?.colour).toBe('yellow')
    expect(objectState({ state: 'terminated', terminated_by: 'admin' })?.explanation).toBe('The queue is being deleted by "admin".')
    expect(objectState({ state: 'running' })).toEqual({ colour: 'green', text: 'running' })
    expect(objectState({})).toBeUndefined()
  })
})

describe('vhostState', () => {
  it('distinguishes running, partial and stopped vhosts', () => {
    expect(vhostState({ a: 'running', b: 'running' }).text).toBe('running')
    expect(vhostState({ a: 'running', b: 'nodedown' })).toMatchObject({ text: 'partial', explanation: expect.stringContaining('b') })
    expect(vhostState({ a: 'stopped' }).text).toBe('stopped')
  })
})

describe('featureTags', () => {
  it('lists implicit and known arguments in the classic order, then other arguments', () => {
    const tags = featureTags({ durable: true, arguments: { 'x-message-ttl': 60000, 'x-queue-type': 'classic', 'x-single-active-consumer': false } })
    expect(tags.map((t) => t.short)).toEqual(['D', 'TTL', 'Args'])
    expect(tags[1].title).toBe('x-message-ttl: 60000')
    expect(tags[2].title).toBe('x-queue-type: classic')
  })

  it('flattens nested tables', () => {
    expect(fmtTableFlat({ a: 1, b: { c: [1, 2] } })).toBe('a: 1, b: (c: [1,2])')
  })
})

describe('names', () => {
  it('shortens connection and channel names to the client side', () => {
    expect(shortConn('127.0.0.1:5000 -> 127.0.0.1:5672')).toBe('127.0.0.1:5000')
    expect(shortChan('127.0.0.1:5000 -> 127.0.0.1:5672 (3)')).toBe('127.0.0.1:5000 (3)')
  })

  it('names the default exchange', () => {
    expect(exchangeName('')).toBe('(AMQP default)')
    expect(exchangeName('amq.topic')).toBe('amq.topic')
  })

  it('masks URI passwords', () => {
    expect(maskUriCredentials('amqp://user:secret@host/vh')).toBe('amqp://user:[redacted]@host/vh')
  })

  it('reads the client product from client properties', () => {
    expect(clientName({ product: 'amqplib', platform: 'Node.JS', version: '0.10' })).toEqual({ name: 'amqplib / Node.JS', version: '0.10', clientId: undefined })
    expect(clientName({})).toBeUndefined()
  })
})

describe('safeHttpUrl', () => {
  it('only allows http and https links', async () => {
    const { safeHttpUrl } = await import('./names')
    expect(safeHttpUrl('https://www.rabbitmq.com/docs/feature-flags')).toBe('https://www.rabbitmq.com/docs/feature-flags')
    expect(safeHttpUrl('javascript:alert(1)')).toBeUndefined()
    expect(safeHttpUrl('data:text/html,x')).toBeUndefined()
    expect(safeHttpUrl('')).toBeUndefined()
  })
})
