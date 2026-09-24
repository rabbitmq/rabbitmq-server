import type { GarbageCollection, RateDetails } from '../../api/types/common'
import { reductionSeries } from '../../charts/stats'
import { Facts } from '../../components/Values'
import { ChartBlock } from './ChartBlock'

export function RuntimeMetrics({ obj }: { obj: { reductions?: number; reductions_details?: RateDetails; garbage_collection?: GarbageCollection } }) {
  const gc = obj.garbage_collection ?? {}
  const rows: [string, number | undefined][] = [
    ['Minimum binary virtual heap size in words (min_bin_vheap_size)', gc.min_bin_vheap_size],
    ['Minimum heap size in words (min_heap_size)', gc.min_heap_size],
    ['Maximum generational collections before fullsweep (fullsweep_after)', gc.fullsweep_after],
    ['Number of minor GCs (minor_gcs)', gc.minor_gcs],
  ]
  return (
    <>
      <ChartBlock title="Reductions (per second)" series={reductionSeries(obj)} kind="rate" />
      <Facts rows={rows.filter(([, v]) => v).map(([label, value]) => [label, value])} />
    </>
  )
}
