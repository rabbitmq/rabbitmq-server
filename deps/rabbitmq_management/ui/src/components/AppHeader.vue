<script setup>
import logoUrl from '../assets/rabbitmqlogo.svg'

defineProps({
  showNav: { type: Boolean, default: true }
})

const TABS = [
  { label: 'Overview', current: true },
  { label: 'Connections' },
  { label: 'Channels' },
  { label: 'Exchanges' },
  { label: 'Queues and Streams' },
  { label: 'Admin' }
]
</script>

<template>
  <div class="app-header">
    <div class="logo-row">
      <img :src="logoUrl" alt="RabbitMQ logo" width="204" height="37" />
      <slot name="actions" />
    </div>
    <nav v-if="showNav" class="tabs">
      <span
        v-for="tab in TABS"
        :key="tab.label"
        class="tab"
        :class="{ current: tab.current }"
        :title="tab.current ? null : 'Not implemented in this prototype'"
      >
        {{ tab.label }}
      </span>
    </nav>
  </div>
</template>

<style scoped>
.app-header {
  background: white;
  box-shadow: 0 1px 4px rgba(0, 0, 0, 0.08);
}

.logo-row {
  max-width: 1200px;
  margin: 0 auto;
  padding: 0.75rem 1.5rem;
  display: flex;
  align-items: center;
  justify-content: space-between;
}

.tabs {
  max-width: 1200px;
  margin: 0 auto;
  padding: 0 1.5rem;
  display: flex;
  gap: 1.5rem;
  border-top: 1px solid #eee;
}

.tab {
  padding: 0.6rem 0;
  font-size: 0.85rem;
  color: #aaa;
  cursor: default;
  border-bottom: 2px solid transparent;
}

.tab.current {
  color: #ff6600;
  font-weight: 600;
  border-bottom-color: #ff6600;
}
</style>
