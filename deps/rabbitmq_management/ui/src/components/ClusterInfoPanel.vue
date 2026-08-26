<script setup>
import { computed } from 'vue'
import { formatNumber } from '../format.js'

const props = defineProps({
  overview: { type: Object, default: () => ({}) }
})

const facts = computed(() => {
  const o = props.overview
  return [
    { label: 'Cluster name', value: o.cluster_name },
    { label: 'Node', value: o.node },
    { label: 'Cluster tags', value: formatTags(o.cluster_tags) },
    { label: 'Node tags', value: formatTags(o.node_tags) },
    { label: 'RabbitMQ version', value: o.rabbitmq_version },
    { label: 'Erlang version', value: o.erlang_version },
    { label: 'Crypto library version', value: o.crypto_lib_version },
    { label: 'Management version', value: o.management_version },
    { label: 'Rates mode', value: o.rates_mode },
    { label: 'Default queue type', value: o.default_queue_type },
    { label: 'Statistics', value: o.disable_stats ? 'disabled' : 'enabled' }
  ].filter((fact) => fact.value !== undefined && fact.value !== null)
})

const eventQueueLength = computed(() => props.overview.statistics_db_event_queue)
const eventQueueBacklogged = computed(() => typeof eventQueueLength.value === 'number' && eventQueueLength.value > 1000)

function formatTags(tags) {
  if (!Array.isArray(tags) || !tags.length) return null
  return tags.join(', ')
}
</script>

<template>
  <div class="cluster-info">
    <dl>
      <template v-for="fact in facts" :key="fact.label">
        <dt>{{ fact.label }}</dt>
        <dd>{{ fact.value }}</dd>
      </template>
    </dl>
    <p v-if="eventQueueBacklogged" class="warning">
      Statistics database event queue backlog: {{ formatNumber(eventQueueLength) }} events.
    </p>
  </div>
</template>

<style scoped>
dl {
  display: grid;
  grid-template-columns: auto 1fr;
  gap: 0.3rem 0.75rem;
  margin: 0;
  font-size: 0.85rem;
}

dt {
  color: #888;
}

dd {
  margin: 0;
  color: #222;
}

.warning {
  margin: 0.75rem 0 0;
  color: #b9770e;
  font-size: 0.8rem;
}
</style>
