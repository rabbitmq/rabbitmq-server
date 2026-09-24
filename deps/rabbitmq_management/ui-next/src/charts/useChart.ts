import { useEffect, useRef } from 'react'
import {
  Chart,
  Filler,
  Legend,
  LineController,
  LineElement,
  LinearScale,
  PointElement,
  Tooltip,
  type ChartConfiguration,
} from 'chart.js'

// Only what line charts need, to keep the bundle small.
Chart.register(LineController, LineElement, PointElement, LinearScale, Tooltip, Legend, Filler)

/**
 * Owns one Chart.js instance for the lifetime of the canvas and updates its data
 * in place, so that a refresh does not recreate the chart or reset its hover state.
 */
export function useChart(config: ChartConfiguration<'line', { x: number; y: number }[]>) {
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const chartRef = useRef<Chart<'line', { x: number; y: number }[]> | null>(null)

  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas) return
    chartRef.current = new Chart(canvas, config)
    return () => {
      chartRef.current?.destroy()
      chartRef.current = null
    }
    // The chart is created once; later changes go through the effect below.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  useEffect(() => {
    const chart = chartRef.current
    if (!chart) return
    chart.data.datasets.forEach((dataset, i) => {
      const next = config.data.datasets[i]
      if (next) {
        dataset.data = next.data
        dataset.hidden = next.hidden
      }
    })
    if (config.data.datasets.length !== chart.data.datasets.length) chart.data.datasets = config.data.datasets
    chart.options = config.options ?? {}
    chart.update('none')
  })

  return canvasRef
}
