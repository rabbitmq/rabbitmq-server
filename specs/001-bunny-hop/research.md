# Bunny Hop research

## Standalone management document and timer isolation

Decision: Serve `bunny-hop.html` with `rabbit_mgmt_wm_static` using a `priv_file` route before the catchall in `rabbit_mgmt_dispatcher.erl`. Apply `maybe_add_path_prefix/2` through the existing route list. Do not add it through `build_module_routes/1`, which prepends `/api`.

Rationale: `priv/www/js/main.js` calls `update_interval()` during startup and uses `setInterval(onRefresh, timer_interval)` in `reset_timer()`. A document that does not load the management shell has no management timer to suspend or accidentally restart. Existing static resources do not require a separate authenticated gameplay service; the game has no broker data access. Preserve the existing static access policy.

Alternatives considered: A hash route inside the management shell would require suppressing full and partial refreshes, pending callbacks, and restoring the preference on every navigation path. A separate web server would complicate packaging and prefixes.

## Engine and distribution

Decision: Pin the self-contained PixiJS `8.22.0` production module locally, retaining the MIT license and provenance. Verify the artifact checksum during acquisition and record it alongside the vendored file.

Rationale: The [official release](https://github.com/pixijs/pixijs/releases/tag/v8.22.0) supplies a production module. This fits static asset packaging and avoids runtime CDN access or a new management build pipeline.

Alternatives considered: A CDN adds network dependency; a new bundler adds an unnecessary build prerequisite; TypeScript conflicts with the requested language.

## Application lifecycle

Decision: Use asynchronous initialization, `app.canvas`, a private ticker, and explicit start, stop, and destroy. Keep native HTML controls and counters separate from the canvas.

Rationale: The [Application guide](https://pixijs.com/8.x/guides/components/application) documents two-stage initialization and private ticker options. The [Ticker guide](https://pixijs.com/8.x/guides/components/ticker) documents callbacks receiving the ticker and explicit loop control. Native buttons provide keyboard focus without recreating controls inside the scene graph.

Alternatives considered: A shared ticker makes teardown ownership unclear; canvas-only controls require extra accessibility integration.

## Simulation, collision, and layout

Decision: Separate a deterministic JavaScript model from rendering, step at 120 updates per simulated second, and use occupied logo pixels with swept collision checks. Keep all gameplay coordinates fixed while resizing the presentation.

Rationale: This permits repeatable scoring tests without a graphics context, prevents delayed frames skipping contact, and avoids viewport changes changing jump reach. A mask preserves the logo's visible outline and empty areas.

Alternatives considered: A single bounding rectangle counts invisible padding as contact. Frame-count scoring changes with refresh rate. Scaling the physics with window size changes difficulty.

## Controls and return navigation

Decision: Register an early native `q` handler and visible Exit link before importing the engine. Resolve assets and the overview from the document URL and configured prefix; do not hard-code origin-root assets.

Rationale: Exit must work during loading failures. A full document navigation back to the management root recreates normal refresh using its unchanged saved preference.

Alternatives considered: Closing the browser tab is unreliable; rewriting refresh preferences would alter the user's management session.

## Research completion

All planning unknowns are resolved. Implementation must inspect the supplied image's alpha representation, confirm the vendored artifact checksum, and calibrate obstacle dimensions against the documented jump and reaction limits as normal implementation work.

## Targeted test integration

Decision: Use the existing management `package.json` module mode and `node --test` runner with `test/bunny-hop.test.js`. Add `bunny_hop_test` to `some_tests()` in `rabbit_mgmt_http_SUITE.erl` for both prefix groups, and a dedicated Selenium suite.

Rationale: This follows the existing `test/main.test.js`, `test/router.test.js`, and `test/html.test.js` patterns without introducing a second JavaScript test framework for the game model.

Alternatives considered: Running a full management suite is unnecessary and conflicts with the targeted testing principle.

## Implementation setup

Read the PixiJS router and application, ticker, assets, sprite, graphics, events, and performance skills before implementation. JavaScript sources use the existing management module mode; no TypeScript compiler is introduced. The supplied PNG is 1000 by 1000 with alpha. Derive occupied row spans from its alpha channel at loading time, trim the rendered texture to the same occupied bounds, and preserve transparent holes.

## Content Security Policy integration

Use the official pinned `dist/packages/unsafe-eval.min.js` compatibility extension before creating the renderer, alongside the production module. The extension replaces dynamic shader and uniform generation with static functions; do not relax management CSP. It expects a mutable global `PIXI` namespace, supplied from the imported module exports. Both distributions are unmodified, licensed, and verified against the package metadata SHA-256 values. Read the PixiJS environments skill for this integration.

Disable `loadTextures.config.preferWorkers` before logo loading: the default loader creates a blob worker, which the management CSP also blocks. Decode the single local texture on the main thread instead.
