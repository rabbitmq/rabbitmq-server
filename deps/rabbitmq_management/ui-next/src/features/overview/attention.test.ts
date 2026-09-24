import { describe, expect, it } from 'vitest'
import type { ClusterNode } from '../../api/types/nodes'
import { attentionItems } from './attention'

const node = (overrides: Partial<ClusterNode>): ClusterNode => ({ name: 'rabbit@a', type: 'disc', running: true, os_pid: '1', ...overrides })

describe('attentionItems', () => {
  it('is empty for a healthy cluster', () => {
    expect(attentionItems({ overview: undefined, nodes: [node({})], vhosts: [{ name: '/', cluster_state: { 'rabbit@a': 'running' } }], alarmsCheck: undefined })).toEqual([])
  })

  it('reports alarms, stopped nodes, partitions and stopped vhosts', () => {
    const items = attentionItems({
      overview: undefined,
      nodes: [node({ mem_alarm: true }), node({ name: 'rabbit@b', running: false }), node({ name: 'rabbit@c', partitions: ['rabbit@a'] })],
      vhosts: [{ name: 'v', cluster_state: { 'rabbit@a': 'stopped' } }],
      alarmsCheck: undefined,
    })
    expect(items.map((i) => i.message)).toEqual([
      'Memory alarm on rabbit@a: publishers are blocked',
      'Node rabbit@b is not running',
      'Network partition: rabbit@c was partitioned from rabbit@a',
      'Virtual host v experienced an error on node rabbit@a and may be inaccessible',
    ])
  })

  it('falls back to the alarms health check for users who cannot list nodes', () => {
    const items = attentionItems({ overview: undefined, nodes: undefined, vhosts: [], alarmsCheck: { status: 'failed', reason: 'memory alarm' } })
    expect(items).toHaveLength(1)
    expect(items[0].severity).toBe('bad')
  })
})

describe('attentionItems with statistics disabled', () => {
  it('does not report missing node statistics, which are expected', () => {
    const items = attentionItems({ overview: undefined, nodes: [node({ os_pid: undefined })], vhosts: [], alarmsCheck: undefined, statsDisabled: true })
    expect(items).toEqual([])
  })
})

describe('attentionItems from the alarms health check', () => {
  it('uses the check when statistics are disabled, since nodes then carry no alarm fields', () => {
    const items = attentionItems({ overview: undefined, nodes: [node({})], vhosts: [], alarmsCheck: { status: 'failed' }, statsDisabled: true })
    expect(items.map((i) => i.severity)).toEqual(['bad'])
  })
})
