<script setup>
import { computed } from 'vue'
import { Line } from 'vue-chartjs'
import { Chart, registerables } from 'chart.js'
import { formatNumber } from '../format.js'

Chart.register(...registerables)

const props = defineProps({
  series: {
    type: Array,
    required: true
  },
  mode: {
    type: String,
    default: 'gauge'
  }
})

function toAscending(samples) {
  return [...samples].reverse()
}

function toRatePoints(samples) {
  const ascending = toAscending(samples)
  const points = []
  for (let i = 1; i < ascending.length; i++) {
    const prev = ascending[i - 1]
    const curr = ascending[i]
    const seconds = (curr.timestamp - prev.timestamp) / 1000
    const value = seconds > 0 ? (curr.sample - prev.sample) / seconds : 0
    points.push({ timestamp: curr.timestamp, value })
  }
  return points
}

function formatTime(timestampMs) {
  return new Date(timestampMs).toLocaleTimeString([], { hour12: false })
}

const seriesPoints = computed(() =>
  props.series.map((s) => {
    const samples = s.details?.samples
    if (!samples?.length) return { ...s, points: [] }
    if (props.mode === 'rate') {
      return { ...s, points: toRatePoints(samples) }
    }
    return { ...s, points: toAscending(samples).map((point) => ({ timestamp: point.timestamp, value: point.sample })) }
  })
)

const labels = computed(() => {
  const withPoints = seriesPoints.value.find((s) => s.points.length)
  if (!withPoints) return []
  return withPoints.points.map((point) => formatTime(point.timestamp))
})

const chartData = computed(() => ({
  labels: labels.value,
  datasets: seriesPoints.value.map((s) => ({
    label: s.label,
    borderColor: s.color,
    backgroundColor: s.color,
    tension: 0.2,
    pointRadius: 0,
    borderWidth: 2,
    data: s.points.map((point) => point.value)
  }))
}))

const chartOptions = computed(() => ({
  responsive: true,
  maintainAspectRatio: false,
  animation: false,
  interaction: { mode: 'index', intersect: false },
  scales: {
    y: {
      beginAtZero: true,
      title: { display: props.mode === 'rate', text: 'per second' },
      ticks: { callback: (value) => formatNumber(value) }
    }
  },
  plugins: {
    legend: { position: 'bottom' },
    tooltip: {
      callbacks: {
        label: (ctx) => `${ctx.dataset.label}: ${formatNumber(ctx.parsed.y, props.mode === 'rate' ? 2 : 0)}`
      }
    }
  }
}))
</script>

<template>
  <div class="chart-wrap">
    <Line :data="chartData" :options="chartOptions" update-mode="none" />
  </div>
</template>

<style scoped>
.chart-wrap {
  height: 220px;
  position: relative;
}
</style>
