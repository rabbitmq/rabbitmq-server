<script setup>
import { computed } from 'vue'
import { usage, formatBytes, isNumeric } from '../format.js'

const props = defineProps({
  label: { type: String, required: true },
  used: { type: [Number, String], default: null },
  total: { type: [Number, String], default: null },
  thresholds: { type: Array, default: () => [] },
  invert: { type: Boolean, default: false },
  forceAlarm: { type: Boolean, default: false },
  unavailableText: { type: String, default: 'not available' },
  formatter: { type: Function, default: formatBytes }
})

const usedAvailable = computed(() => isNumeric(props.used))
const totalAvailable = computed(() => isNumeric(props.total))
const available = computed(() => usedAvailable.value && totalAvailable.value)

// For invert (disk free vs. low watermark), `used` is far larger than `total`
// most of the time, so the bar fills as free space APPROACHES the watermark
// rather than as a used/total fraction — ratio = watermark / free, clamped.
const barUsage = computed(() => {
  if (!available.value) return { ratio: 0, level: 'green' }
  const base = props.invert
    ? usage(props.total, Math.max(props.used, props.total), props.thresholds)
    : usage(props.used, props.total, props.thresholds)
  if (props.forceAlarm) return { ...base, level: 'red' }
  return base
})

const caption = computed(() => {
  if (available.value) {
    return props.invert
      ? `${props.formatter(props.used)} free (limit ${props.formatter(props.total)})`
      : `${props.formatter(props.used)} / ${props.formatter(props.total)}`
  }
  // The limit can be reported as a magic string (e.g. "memory_monitoring_disabled")
  // while the current value is still a real number — show that value alone.
  if (usedAvailable.value) return props.formatter(props.used)
  return props.unavailableText
})
</script>

<template>
  <div class="usage-row">
    <div class="usage-header">
      <span class="usage-label">{{ label }}</span>
      <span class="usage-caption">{{ caption }}</span>
    </div>
    <div class="usage-bar">
      <div
        class="usage-fill"
        :class="barUsage.level"
        :style="{ width: `${barUsage.ratio * 100}%` }"
      ></div>
    </div>
  </div>
</template>

<style scoped>
.usage-row {
  font-size: 0.8rem;
  padding: 0.3rem 0;
}

.usage-header {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  margin-bottom: 0.25rem;
}

.usage-label {
  color: #333;
  font-weight: 600;
}

.usage-bar {
  background: #eee;
  border-radius: 4px;
  height: 0.5rem;
  overflow: hidden;
}

.usage-fill {
  height: 100%;
  border-radius: 4px;
}

.usage-fill.green {
  background: #27ae60;
}

.usage-fill.yellow {
  background: #e67e22;
}

.usage-fill.red {
  background: #c0392b;
}

.usage-caption {
  color: #888;
  white-space: nowrap;
}
</style>
