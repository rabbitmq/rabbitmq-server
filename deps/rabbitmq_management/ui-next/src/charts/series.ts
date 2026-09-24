import type { RateDetails, Sample } from '../api/types/common'

export interface Point {
  x: number
  y: number
}

/** Samples arrive newest first; charts want them oldest first. */
function ascending(samples: Sample[]): Sample[] {
  return [...samples].sort((a, b) => a.timestamp - b.timestamp)
}

export function gaugePoints(details: RateDetails | undefined): Point[] {
  return ascending(details?.samples ?? []).map((s) => ({ x: s.timestamp, y: s.sample }))
}

/**
 * Plots a counter, such as messages published, as a per-second rate between
 * consecutive samples, as the classic UI's charts.js does.
 */
export function ratePoints(details: RateDetails | undefined): Point[] {
  const samples = ascending(details?.samples ?? [])
  const points: Point[] = []
  for (let i = 1; i < samples.length; i++) {
    const seconds = (samples[i].timestamp - samples[i - 1].timestamp) / 1000
    if (seconds <= 0) continue
    points.push({ x: samples[i].timestamp, y: Math.max(0, (samples[i].sample - samples[i - 1].sample) / seconds) })
  }
  return points
}

export function hasSamples(details: (RateDetails | undefined)[]): boolean {
  return details.some((d) => (d?.samples?.length ?? 0) > 1)
}
