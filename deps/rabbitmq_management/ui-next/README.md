# Management UI: React prototype

A translation of the management UI in `../priv/www` to React 19, TypeScript,
TanStack Query, TanStack Router, TanStack Table and Chart.js. It keeps the
classic UI's object model and its hash routes (`#/queues/%2F/orders`), so links
and bookmarks work in both UIs.

The broker serves the build at `/next/`, beside the classic UI at `/`. Both UIs
keep their session and preferences under the same `rabbitmq.*` localStorage
keys, so a user logged into one is logged into the other.

This is a prototype. Only basic authentication is implemented; OAuth 2 sign-in
and plugin pages link to the classic UI.


## Development

Node is pinned with [Volta](https://volta.sh) in `package.json`. Start a broker
from the repository root:

```shell
gmake ENABLED_PLUGINS="rabbitmq_management" run-background-broker
```

Then, from this directory:

```shell
npm ci
npm run dev
```

The dev server on `http://localhost:5173` proxies `/api` to
`http://localhost:15672`. Set `RABBITMQ_MGMT_URL` to use another broker.


## Checks

```shell
npm run lint
npm run typecheck
npm test
```

The tests use Vitest, Testing Library and MSW, with responses captured from a
real broker in `test/fixtures`.


## Building into the plugin

```shell
npm run build
```

writes the bundle to `../priv/www/next`, which is gitignored, and then fails if
the HTML contains anything that the broker's default Content Security Policy
(`script-src 'self'; object-src 'self'`) would block.

The plugin's `build-ui-next` target runs `npm ci && npm run build`, and
`gmake BUILD_UI_NEXT=1` makes it part of the regular build. It is opt-in
because it needs Node and network access.

The bundle uses relative URLs and resolves the API as `../api/`, so it works
with `management.path_prefix` without any configuration.


## Layout

 * `src/api`: the HTTP client, hand-written types for API responses, and one
   module of query and mutation functions per resource
 * `src/app`: the shell, router, navigation, refresh control and session start-up
 * `src/components`: tables with server-side paging, sections, forms and values
 * `src/charts`: Chart.js line charts that update in place
 * `src/features`: one directory per screen of the classic UI
 * `src/format`: formatting functions ported from the classic UI's `formatters.js`
