<script setup>
import { computed } from 'vue'
import { formatNumber } from '../format.js'

const props = defineProps({
  data: { type: Object, default: () => ({}) },
  rows: { type: Array, required: true },
  showTotal: { type: Boolean, default: true },
  showRate: { type: Boolean, default: true }
})

const rates = computed(() =>
  props.rows.map((row) => ({
    ...row,
    total: props.data[row.key] ?? 0,
    rate: props.data[`${row.key}_details`]?.rate ?? 0
  }))
)
</script>

<template>
  <table class="metric-table">
    <thead>
      <tr>
        <th>Event</th>
        <th v-if="showTotal">Total</th>
        <th v-if="showRate">Rate/s</th>
      </tr>
    </thead>
    <tbody>
      <tr v-for="row in rates" :key="row.key">
        <td>{{ row.label }}</td>
        <td v-if="showTotal">{{ formatNumber(row.total) }}</td>
        <td v-if="showRate">{{ formatNumber(row.rate, 2) }}</td>
      </tr>
    </tbody>
  </table>
</template>

<style scoped>
.metric-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.85rem;
}

th,
td {
  text-align: left;
  padding: 0.35rem 0.5rem;
  border-bottom: 1px solid #eee;
}

th {
  color: #888;
  font-weight: 600;
  text-transform: uppercase;
  font-size: 0.7rem;
}
</style>
