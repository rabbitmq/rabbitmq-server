/// <reference types="vitest/config" />
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

const broker = process.env.RABBITMQ_MGMT_URL ?? 'http://localhost:15672'

export default defineConfig({
  plugins: [react()],
  // Relative asset URLs, so that the bundle works under any management.path_prefix.
  base: './',
  build: {
    outDir: '../priv/www/next',
    emptyOutDir: true,
    // The polyfill is an inline script, which the default CSP (script-src 'self') blocks.
    modulePreload: { polyfill: false },
    chunkSizeWarningLimit: 1024,
  },
  server: {
    proxy: {
      '/api': broker,
      '/favicon.ico': broker,
    },
  },
  test: {
    environment: 'jsdom',
    setupFiles: ['./test/setup.ts'],
    css: { modules: { classNameStrategy: 'non-scoped' } },
  },
})
