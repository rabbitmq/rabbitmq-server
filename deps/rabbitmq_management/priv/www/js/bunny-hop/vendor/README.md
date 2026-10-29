# PixiJS

- Version: `8.22.0`
- Production ES module: https://cdn.jsdelivr.net/npm/pixi.js@8.22.0/dist/pixi.min.mjs
- Upstream release: https://github.com/pixijs/pixijs/releases/tag/v8.22.0
- License source: https://cdn.jsdelivr.net/npm/pixi.js@8.22.0/LICENSE
- SHA-256: `66257bc46776898bf8c54daaed402ed8f9ac109379c47b2dfa8aa048544d2953`

The distribution is copied without modifications. Verify a replacement with `shasum -a 256 pixi-8.22.0.min.mjs`. Assets are served locally; no package manager is needed to build or run RabbitMQ.

## Content Security Policy support

The official classic compatibility extension is loaded against a mutable copy of the module namespace before renderer initialization. It replaces dynamic code generation with static functions under the existing management CSP. Its functions continue to use the page-local `PIXI` namespace.

- `pixi-8.22.0.min.mjs` SHA-256 verified against package metadata: `66257bc46776898bf8c54daaed402ed8f9ac109379c47b2dfa8aa048544d2953`
- `unsafe-eval-8.22.0.min.js` SHA-256 verified against package metadata: `80d5eabed0e1286e2f646b272687170d9211ecd1c901b3382c2d5274ba2c42a9`

Extension source: https://cdn.jsdelivr.net/npm/pixi.js@8.22.0/dist/packages/unsafe-eval.min.js
