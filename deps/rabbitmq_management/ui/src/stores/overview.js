import { reactive } from 'vue'
import { fetchOverview, fetchNodes, UnauthorizedError, ForbiddenError } from '../api.js'

const POLL_INTERVALS = [2000, 5000, 10000]

const state = reactive({
  overview: null,
  nodes: null,
  nodesForbidden: false,
  loading: false,
  error: null,
  stale: false,
  lastUpdated: null,
  pollIntervalMs: 5000,
  unauthorized: false
})

let timerId = null
let subscriberCount = 0
let visibilityHandlerAttached = false

async function poll() {
  state.loading = true
  try {
    const [overview, nodesOutcome] = await Promise.all([
      fetchOverview(),
      fetchNodes().then((nodes) => ({ nodes }), (err) => ({ err }))
    ])
    state.overview = overview
    state.error = null
    state.stale = false
    state.lastUpdated = new Date()

    if (nodesOutcome.nodes) {
      state.nodes = nodesOutcome.nodes
      state.nodesForbidden = false
    } else if (nodesOutcome.err instanceof UnauthorizedError) {
      state.unauthorized = true
      stopPolling()
      return
    } else if (nodesOutcome.err instanceof ForbiddenError) {
      state.nodesForbidden = true
    } else {
      state.nodesForbidden = false
      state.error = nodesOutcome.err.message
    }
  } catch (err) {
    if (err instanceof UnauthorizedError) {
      state.unauthorized = true
      stopPolling()
      return
    }
    state.error = err.message
    state.stale = state.overview !== null
  } finally {
    state.loading = false
  }
}

function scheduleTimer() {
  clearTimer()
  timerId = setInterval(poll, state.pollIntervalMs)
}

function clearTimer() {
  if (timerId !== null) {
    clearInterval(timerId)
    timerId = null
  }
}

function onVisibilityChange() {
  if (document.visibilityState === 'hidden') {
    clearTimer()
  } else if (subscriberCount > 0) {
    poll()
    scheduleTimer()
  }
}

function startPolling() {
  subscriberCount += 1
  if (subscriberCount > 1) return

  state.unauthorized = false
  if (!visibilityHandlerAttached) {
    document.addEventListener('visibilitychange', onVisibilityChange)
    visibilityHandlerAttached = true
  }

  poll()
  if (document.visibilityState !== 'hidden') {
    scheduleTimer()
  }
}

function stopPolling() {
  subscriberCount = Math.max(0, subscriberCount - 1)
  if (subscriberCount === 0) {
    clearTimer()
  }
}

function setPollInterval(ms) {
  state.pollIntervalMs = ms
  if (timerId !== null) {
    scheduleTimer()
  }
}

export { state, startPolling, stopPolling, setPollInterval, POLL_INTERVALS }

