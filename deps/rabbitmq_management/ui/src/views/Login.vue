<script setup>
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import { login } from '../api.js'
import logoUrl from '../assets/rabbitmqlogo.svg'

const username = ref('guest')
const password = ref('guest')
const error = ref(null)
const submitting = ref(false)
const router = useRouter()

async function onSubmit() {
  error.value = null
  submitting.value = true
  try {
    await login(username.value, password.value)
    router.push({ name: 'dashboard' })
  } catch (err) {
    error.value = 'Login failed: check username and password.'
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <div class="login-screen">
    <form class="login-form" @submit.prevent="onSubmit">
      <img class="logo" :src="logoUrl" alt="RabbitMQ logo" width="204" height="37" />
      <p class="subtitle">Vue dashboard prototype</p>

      <label>
        Username
        <input v-model="username" type="text" autocomplete="username" required />
      </label>

      <label>
        Password
        <input v-model="password" type="password" autocomplete="current-password" required />
      </label>

      <p v-if="error" class="error">{{ error }}</p>

      <button type="submit" :disabled="submitting">
        {{ submitting ? 'Signing in…' : 'Sign in' }}
      </button>
    </form>
  </div>
</template>

<style scoped>
.login-screen {
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 100vh;
}

.login-form {
  background: white;
  padding: 2.5rem;
  border-radius: 8px;
  box-shadow: 0 2px 12px rgba(0, 0, 0, 0.08);
  width: 320px;
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
}

.logo {
  align-self: center;
}

.subtitle {
  margin: 0 0 1rem;
  color: #666;
  font-size: 0.85rem;
}

label {
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
  font-size: 0.85rem;
  color: #444;
}

input {
  padding: 0.5rem;
  border: 1px solid #ccc;
  border-radius: 4px;
  font-size: 1rem;
}

button {
  margin-top: 0.5rem;
  padding: 0.6rem;
  border: none;
  border-radius: 4px;
  background: #ff6600;
  color: white;
  font-weight: 600;
  cursor: pointer;
}

button:disabled {
  opacity: 0.6;
  cursor: default;
}

.error {
  color: #c0392b;
  font-size: 0.85rem;
  margin: 0;
}
</style>
