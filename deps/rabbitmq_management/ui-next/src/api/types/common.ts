export interface Sample {
  sample: number
  timestamp: number
}

/** The `<metric>_details` object that accompanies a counter or gauge. */
export interface RateDetails {
  rate: number
  avg?: number
  avg_rate?: number
  /** Present only when `*_age` and `*_incr` are requested; newest first. */
  samples?: Sample[]
}

export interface Paginated<T> {
  total_count: number
  item_count: number
  filtered_count: number
  page: number
  page_size: number
  page_count: number
  items: T[]
}

export type AmqpValue = string | number | boolean | null | AmqpValue[] | { [key: string]: AmqpValue }
export type AmqpTable = Record<string, AmqpValue>

export type RatesMode = 'none' | 'basic' | 'detailed'

export interface MessageStats {
  publish?: number
  publish_details?: RateDetails
  publish_in?: number
  publish_in_details?: RateDetails
  publish_out?: number
  publish_out_details?: RateDetails
  confirm?: number
  confirm_details?: RateDetails
  deliver?: number
  deliver_details?: RateDetails
  deliver_no_ack?: number
  deliver_no_ack_details?: RateDetails
  get?: number
  get_details?: RateDetails
  get_no_ack?: number
  get_no_ack_details?: RateDetails
  get_empty?: number
  get_empty_details?: RateDetails
  deliver_get?: number
  deliver_get_details?: RateDetails
  redeliver?: number
  redeliver_details?: RateDetails
  ack?: number
  ack_details?: RateDetails
  return_unroutable?: number
  return_unroutable_details?: RateDetails
  drop_unroutable?: number
  drop_unroutable_details?: RateDetails
}

export interface GarbageCollection {
  fullsweep_after?: number
  max_heap_size?: number
  min_bin_vheap_size?: number
  min_heap_size?: number
  minor_gcs?: number
}
