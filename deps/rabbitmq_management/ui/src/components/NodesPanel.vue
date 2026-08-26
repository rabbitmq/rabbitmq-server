<script setup>
import { computed } from 'vue'
import UsageBar from './UsageBar.vue'
import { formatUptime, formatCount, isNumeric, FD_THRESHOLDS, PROCESS_THRESHOLDS } from '../format.js'

const props = defineProps({
  nodes: { type: Array, default: null },
  forbidden: { type: Boolean, default: false },
  disableStats: { type: Boolean, default: false }
})

const rows = computed(() => props.nodes ?? [])

function statsUnavailableMessage() {
  return props.disableStats
    ? 'Statistics are disabled on this cluster.'
    : 'Node statistics not available — enable the rabbitmq_management_agent plugin on this node.'
}
</script>

<template>
  <div v-if="forbidden" class="notice">Requires the monitoring tag.</div>
  <div v-else-if="!rows.length" class="notice">No node data yet.</div>
  <div v-else class="nodes">
    <div v-for="node in rows" :key="node.name" class="node">
      <div class="node-header">
        <span class="name">{{ node.name }}</span>
        <span class="type">{{ node.type }}</span>
        <span :class="['running', node.running ? 'up' : 'down']">
          {{ node.running ? 'running' : 'not running' }}
        </span>
        <span v-if="node.being_drained" class="badge maintenance">maintenance mode</span>
        <span v-if="isNumeric(node.processors)" class="cores">{{ node.processors }} cores</span>
      </div>

      <div v-if="!node.running" class="notice error">Node not running.</div>
      <div v-else-if="node.os_pid === undefined" class="notice warning">{{ statsUnavailableMessage() }}</div>
      <template v-else>
        <UsageBar
          label="Memory"
          :used="node.mem_used"
          :total="node.mem_limit"
          :force-alarm="node.mem_alarm"
          unavailable-text="monitoring disabled"
        />
        <UsageBar
          label="Disk"
          :used="node.disk_free"
          :total="node.disk_free_limit"
          invert
          :force-alarm="node.disk_free_alarm"
          unavailable-text="monitoring disabled"
        />
        <UsageBar
          label="File descriptors"
          :used="node.fd_used"
          :total="node.fd_total"
          :thresholds="FD_THRESHOLDS"
          :formatter="formatCount"
          unavailable-text="not available"
        />
        <UsageBar
          label="Erlang processes"
          :used="node.proc_used"
          :total="node.proc_total"
          :thresholds="PROCESS_THRESHOLDS"
          :formatter="formatCount"
          unavailable-text="not available"
        />
        <div class="node-footer">
          <span>Uptime {{ formatUptime(node.uptime) }}</span>
          <span>Run queue {{ formatCount(node.run_queue) }}</span>
          <span>{{ node.rates_mode }} rates</span>
          <span>{{ node.mem_calculation_strategy }}</span>
          <span>{{ (node.enabled_plugins ?? []).length }} plugins enabled</span>
        </div>
      </template>
    </div>
  </div>
</template>

<style scoped>
.nodes {
  display: flex;
  flex-direction: column;
  gap: 1rem;
}

.node {
  border: 1px solid #eee;
  border-radius: 6px;
  padding: 0.75rem;
}

.node-header {
  display: flex;
  align-items: baseline;
  gap: 0.6rem;
  margin-bottom: 0.5rem;
  flex-wrap: wrap;
}

.name {
  font-weight: 600;
  font-size: 0.9rem;
}

.type,
.cores {
  color: #888;
  font-size: 0.75rem;
}

.running {
  font-size: 0.75rem;
  font-weight: 600;
  text-transform: uppercase;
}

.running.up {
  color: #27ae60;
}

.running.down {
  color: #c0392b;
}

.badge.maintenance {
  padding: 0.1rem 0.4rem;
  border-radius: 10px;
  background: #fdebd0;
  color: #b9770e;
  font-size: 0.7rem;
  font-weight: 600;
}

.node-footer {
  display: flex;
  gap: 1rem;
  flex-wrap: wrap;
  margin-top: 0.5rem;
  font-size: 0.75rem;
  color: #888;
}

.notice {
  font-size: 0.85rem;
  color: #666;
}

.notice.error {
  color: #c0392b;
}

.notice.warning {
  color: #b9770e;
}
</style>
