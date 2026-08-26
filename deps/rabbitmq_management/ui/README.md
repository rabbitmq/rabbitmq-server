# Management UI Vue prototype

A Vue 3 + Chart.js prototype dashboard for the RabbitMQ management plugin. It replaces the
existing UI's whole-page 5 second refresh with a single poller feeding a reactive store, so
only the components bound to changed values re-render.

Scope for this iteration: a login screen and one dashboard page, polled from `GET /api/overview`
and `GET /api/nodes`. The latter requires the `monitoring` (or `administrator`) tag; a user with
only the `management` tag sees the rest of the dashboard update normally, with the node panel
reporting that it needs additional permissions. The existing UI under `../priv/www` is untouched.

## Toolchain

This project is managed with [Volta](https://volta.sh); `package.json` pins node 22.22.2
(npm 10.9.7 ships bundled with that node build). `node_modules` and the built bundle are not
committed.

## Development

Start a broker with the management plugin from the repository root:

```shell
gmake ENABLED_PLUGINS="rabbitmq_management" run-background-broker
```

Then, from this directory:

```shell
npm install
npm run dev
```

Open the printed `http://localhost:5173` URL. The dev server proxies `/api` to
`http://localhost:15672`, so no CORS configuration on the broker is needed and RabbitMQ's
CSP header (which is only sent for responses from the broker itself) does not apply.

## Production build

```shell
npm run build
```

This writes the built assets straight into `../priv/www/vue`, which is served automatically
by the plugin's existing static file route — no Erlang changes are needed.

If you started the broker with `run-background-broker`, RabbitMQ serves plugin assets from a
packaged copy under `plugins/rabbitmq_management-<version>/priv/www/`, made when the plugin
was last built — not live from this source tree. After `npm run build`, re-run

```shell
gmake ENABLED_PLUGINS="rabbitmq_management" run-background-broker
```

from the repository root to refresh that copy (this does not restart the already-running
node, it only re-syncs `priv`). Then `http://localhost:15672/vue/index.html` serves the new
build. This build step is not wired into the plugin's `gmake` build; run it manually.

The production build is where the broker's default Content-Security-Policy
(`script-src 'self'; object-src 'self'`) applies. The Vite config disables the module
preload polyfill and the app is built from precompiled Vue single-file components only, to
stay within that policy.
