<script setup>
import { ref } from 'vue'

defineProps({
  title: { type: String, default: null },
  collapsible: { type: Boolean, default: false }
})

const open = ref(false)

function toggle() {
  open.value = !open.value
}
</script>

<template>
  <div class="panel">
    <h3 v-if="collapsible">
      <button type="button" class="toggle" :aria-expanded="open" @click="toggle">
        <span class="marker">{{ open ? '▾' : '▸' }}</span>
        {{ title }}
      </button>
    </h3>
    <h3 v-else-if="title">{{ title }}</h3>
    <div v-if="!collapsible || open" class="body">
      <slot />
    </div>
  </div>
</template>

<style scoped>
.panel {
  background: white;
  border-radius: 8px;
  padding: 1rem;
  box-shadow: 0 1px 4px rgba(0, 0, 0, 0.06);
}

h3 {
  margin: 0 0 0.5rem;
  font-size: 0.95rem;
  color: #333;
}

.toggle {
  display: block;
  width: 100%;
  background: none;
  border: 0;
  padding: 0;
  margin: 0;
  font: inherit;
  color: inherit;
  text-align: left;
  cursor: pointer;
}

.marker {
  display: inline-block;
  width: 1em;
}

.body {
  display: flex;
  flex-direction: column;
  gap: 1rem;
}
</style>
