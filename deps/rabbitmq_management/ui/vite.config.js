import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

export default defineConfig({
  plugins: [vue()],
  base: './',
  build: {
    outDir: '../priv/www/vue',
    emptyOutDir: true,
    modulePreload: {
      polyfill: false
    }
  },
  server: {
    proxy: {
      '/api': 'http://localhost:15672'
    }
  }
})
