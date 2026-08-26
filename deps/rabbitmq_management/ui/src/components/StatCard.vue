<script setup>
import { computed } from 'vue'
import { formatNumber, isNumeric } from '../format.js'

const props = defineProps({
  label: { type: String, required: true },
  value: { type: [Number, String], default: '—' }
})

const displayValue = computed(() => (isNumeric(props.value) ? formatNumber(props.value) : props.value))
</script>

<template>
  <div class="stat-card">
    <div class="value">{{ displayValue }}</div>
    <div class="label">{{ label }}</div>
  </div>
</template>

<style scoped>
.stat-card {
  background: white;
  border-radius: 8px;
  padding: 1rem 1.25rem;
  box-shadow: 0 1px 4px rgba(0, 0, 0, 0.06);
  min-width: 120px;
}

.value {
  font-size: 1.75rem;
  font-weight: 700;
  color: #222;
}

.label {
  font-size: 0.8rem;
  color: #777;
  margin-top: 0.25rem;
  text-transform: uppercase;
  letter-spacing: 0.03em;
}
</style>
