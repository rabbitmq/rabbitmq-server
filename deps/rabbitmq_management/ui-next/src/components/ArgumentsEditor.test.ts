import { describe, expect, it } from 'vitest'
import { rowsToTable, tableToRows } from './ArgumentsEditor'

describe('arguments round trip', () => {
  it('keeps the types of a policy definition', () => {
    const definition = {
      'max-length': 10,
      'ha-sync': true,
      'dead-letter-exchange': 'dlx',
      'federation-upstream-set': 'all',
      nodes: ['rabbit@a', 'rabbit@b'],
      ports: [1, 2],
      nested: { a: 1 },
    }
    expect(rowsToTable(tableToRows(definition))).toEqual(definition)
  })

  it('reports invalid numbers and JSON', () => {
    expect(() => rowsToTable([{ id: 1, key: 'x', value: 'abc', type: 'number' }])).toThrow('not a number')
    expect(() => rowsToTable([{ id: 1, key: 'x', value: '{', type: 'json' }])).toThrow('not valid JSON')
  })

  it('skips rows without a name', () => {
    expect(rowsToTable([{ id: 1, key: '', value: 'orphan', type: 'string' }])).toEqual({})
  })
})
