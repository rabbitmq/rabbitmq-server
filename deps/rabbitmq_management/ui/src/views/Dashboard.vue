<script setup>
import { computed, onMounted, onUnmounted, watch } from 'vue'
import { useRouter } from 'vue-router'
import { state, startPolling, stopPolling, setPollInterval, POLL_INTERVALS } from '../stores/overview.js'
import { logout } from '../api.js'
import StatCard from '../components/StatCard.vue'
import MetricChart from '../components/MetricChart.vue'
import MetricTable from '../components/MetricTable.vue'
import Panel from '../components/Panel.vue'
import NodesPanel from '../components/NodesPanel.vue'
import ClusterInfoPanel from '../components/ClusterInfoPanel.vue'
import AppHeader from '../components/AppHeader.vue'

const router = useRouter()

onMounted(startPolling)
onUnmounted(stopPolling)

watch(
  () => state.unauthorized,
  (unauthorized) => {
    if (unauthorized) router.push({ name: 'login' })
  }
)

const overview = computed(() => state.overview)
const objectTotals = computed(() => overview.value?.object_totals ?? {})
const queueTotals = computed(() => overview.value?.queue_totals ?? {})
const messageStats = computed(() => overview.value?.message_stats ?? {})
const churnRates = computed(() => overview.value?.churn_rates ?? {})

const queueSeries = computed(() => [
  { label: 'Ready', color: '#2980b9', details: queueTotals.value.messages_ready_details },
  { label: 'Unacknowledged', color: '#e67e22', details: queueTotals.value.messages_unacknowledged_details },
  { label: 'Total', color: '#8e44ad', details: queueTotals.value.messages_details }
])

const messageRateSeries = computed(() => [
  { label: 'Publish', color: '#27ae60', details: messageStats.value.publish_details },
  { label: 'Deliver / get', color: '#2980b9', details: messageStats.value.deliver_get_details },
  { label: 'Ack', color: '#c0392b', details: messageStats.value.ack_details }
])

const QUEUE_TOTALS_ROWS = [
  { key: 'messages_ready', label: 'Ready' },
  { key: 'messages_unacknowledged', label: 'Unacknowledged' },
  { key: 'messages', label: 'Total' }
]

const MESSAGE_RATE_ROWS = [
  { key: 'publish', label: 'Publish' },
  { key: 'deliver_get', label: 'Deliver / get' },
  { key: 'ack', label: 'Ack' }
]

const CONNECTION_CHANNEL_ROWS = [
  { key: 'connection_created', label: 'Connections created' },
  { key: 'connection_closed', label: 'Connections closed' },
  { key: 'channel_created', label: 'Channels created' },
  { key: 'channel_closed', label: 'Channels closed' }
]

const QUEUE_CHURN_ROWS = [
  { key: 'queue_declared', label: 'Queues declared' },
  { key: 'queue_created', label: 'Queues created' },
  { key: 'queue_deleted', label: 'Queues deleted' }
]

const churnSeries = computed(() => [
  { label: 'Connections created', color: '#2980b9', details: churnRates.value.connection_created_details },
  { label: 'Connections closed', color: '#c0392b', details: churnRates.value.connection_closed_details },
  { label: 'Channels created', color: '#27ae60', details: churnRates.value.channel_created_details },
  { label: 'Channels closed', color: '#e67e22', details: churnRates.value.channel_closed_details }
])

const queueChurnSeries = computed(() => [
  { label: 'Queues declared', color: '#2980b9', details: churnRates.value.queue_declared_details },
  { label: 'Queues created', color: '#27ae60', details: churnRates.value.queue_created_details },
  { label: 'Queues deleted', color: '#c0392b', details: churnRates.value.queue_deleted_details }
])

function onLogout() {
  logout()
  router.push({ name: 'login' })
}
</script>

<template>
  <div class="dashboard">
    <AppHeader>
      <template #actions>
        <button class="logout" @click="onLogout">Log out</button>
      </template>
    </AppHeader>

    <div class="content">
      <header class="sub-header">
        <div class="header-main">
          <h1>{{ overview?.cluster_name ?? 'RabbitMQ' }}</h1>
          <span class="node">{{ overview?.node }}</span>
        </div>
        <div class="header-meta">
          <span v-if="overview">{{ overview.product_name }} {{ overview.product_version }}</span>
          <span v-if="state.stale" class="badge stale">Stale data</span>
          <span v-if="state.error" class="badge error">{{ state.error }}</span>
          <span v-if="state.lastUpdated" class="updated">
            Updated {{ state.lastUpdated.toLocaleTimeString([], { hour12: false }) }}
          </span>
          <select :value="state.pollIntervalMs" @change="setPollInterval(Number($event.target.value))">
            <option v-for="ms in POLL_INTERVALS" :key="ms" :value="ms">every {{ ms / 1000 }}s</option>
          </select>
        </div>
      </header>

      <section class="stat-grid">
      <StatCard label="Queues" :value="objectTotals.queues" />
      <StatCard label="Exchanges" :value="objectTotals.exchanges" />
      <StatCard label="Connections" :value="objectTotals.connections" />
      <StatCard label="Channels" :value="objectTotals.channels" />
      <StatCard label="Consumers" :value="objectTotals.consumers" />
    </section>

    <section class="row-queued-cluster">
      <Panel title="Queued messages">
        <MetricChart :series="queueSeries" />
        <MetricTable :data="queueTotals" :rows="QUEUE_TOTALS_ROWS" :show-rate="false" />
      </Panel>
      <Panel title="Cluster">
        <ClusterInfoPanel :overview="overview ?? {}" />
      </Panel>
    </section>

    <section class="row-rates-nodes">
      <Panel title="Message rates">
        <MetricChart :series="messageRateSeries" mode="rate" />
        <MetricTable :data="messageStats" :rows="MESSAGE_RATE_ROWS" :show-total="false" />
      </Panel>
      <Panel title="Nodes">
        <NodesPanel
          :nodes="state.nodes"
          :forbidden="state.nodesForbidden"
          :disable-stats="overview?.disable_stats"
        />
      </Panel>
    </section>

    <section class="detail-grid">
      <Panel title="Connection and channel churn" collapsible>
        <MetricChart :series="churnSeries" mode="rate" />
        <MetricTable :data="churnRates" :rows="CONNECTION_CHANNEL_ROWS" />
      </Panel>
      <Panel title="Queue churn" collapsible>
        <MetricChart :series="queueChurnSeries" mode="rate" />
        <MetricTable :data="churnRates" :rows="QUEUE_CHURN_ROWS" />
      </Panel>
    </section>
    </div>
  </div>
</template>

<style scoped>
.content {
  max-width: 1200px;
  margin: 0 auto;
  padding: 1.5rem;
}

.sub-header {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  flex-wrap: wrap;
  gap: 0.75rem;
  margin-bottom: 1.5rem;
}

.header-main {
  display: flex;
  align-items: baseline;
  gap: 0.75rem;
}

h1 {
  margin: 0;
  font-size: 1.4rem;
}

.node {
  color: #888;
  font-size: 0.85rem;
}

.header-meta {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  font-size: 0.8rem;
  color: #666;
}

.badge {
  padding: 0.15rem 0.5rem;
  border-radius: 10px;
  font-weight: 600;
}

.badge.stale {
  background: #fdebd0;
  color: #b9770e;
}

.badge.error {
  background: #fadbd8;
  color: #c0392b;
}

select,
button {
  padding: 0.3rem 0.5rem;
  border: 1px solid #ccc;
  border-radius: 4px;
  background: white;
  font-size: 0.8rem;
}

.logout {
  cursor: pointer;
}

.stat-grid {
  display: flex;
  gap: 1rem;
  flex-wrap: wrap;
  margin-bottom: 1.5rem;
}

.row-queued-cluster {
  display: grid;
  grid-template-columns: 2fr 1fr;
  gap: 1rem;
  margin-bottom: 1.5rem;
}

.row-rates-nodes {
  display: grid;
  grid-template-columns: 2fr 1fr;
  gap: 1rem;
  margin-bottom: 1.5rem;
}

.detail-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 1rem;
}
</style>
