import { describe, expect, it } from 'vitest'
import { chartRanges, rangeParams } from './range'
import { gaugePoints, hasSamples, ratePoints } from './series'

const details = {
  rate: 2,
  samples: [
    { sample: 30, timestamp: 10_000 },
    { sample: 20, timestamp: 5_000 },
    { sample: 0, timestamp: 0 },
  ],
}

describe('series', () => {
  it('orders samples oldest first', () => {
    expect(gaugePoints(details).map((p) => p.x)).toEqual([0, 5_000, 10_000])
  })

  it('turns counters into per-second rates between samples', () => {
    expect(ratePoints(details)).toEqual([
      { x: 5_000, y: 4 },
      { x: 10_000, y: 2 },
    ])
  })

  it('needs at least two samples to draw anything', () => {
    expect(hasSamples([{ rate: 0 }, { rate: 0, samples: [{ sample: 1, timestamp: 1 }] }])).toBe(false)
    expect(hasSamples([details])).toBe(true)
  })
})

describe('chart ranges', () => {
  it('offers the ranges the retention policy can serve, always including the last minute', () => {
    const policies = { global: [600, 3600, 28800, 86400], basic: [600, 3600], detailed: [600] }
    expect(chartRanges(policies, 'basic').map((r) => r.value)).toEqual(['60|5', '600|5', '3600|60'])
    expect(chartRanges(policies, 'global')).toHaveLength(5)
  })

  it('maps a range to the age and increment parameters', () => {
    expect(rangeParams(['lengths', 'msg_rates'], '600|5')).toEqual({ lengths_age: 600, lengths_incr: 5, msg_rates_age: 600, msg_rates_incr: 5 })
  })
})
