# Implementation Plan: Bunny Hop

**Branch**: `spec-rabbit` | **Date**: 2026-10-29 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/001-bunny-hop/spec.md`

## Summary

Serve a dedicated game document at `/bunny-hop` through the management plugin. Use JavaScript and a locally packaged PixiJS `8.22.0` browser distribution. Keep the game outside the management shell so its refresh timer never starts. Provide a logo runner, collision detection, distance and obstacle counters, replay, and a visible Exit action bound to `q`.

## Technical Context

**Language/Version**: JavaScript using browser modules; Erlang compatible with the repository's `27.x` target for the route change; no TypeScript

**Primary Dependencies**: PixiJS `8.22.0`, existing management static handler and Cowboy routing; use native HTML controls around the canvas

**Storage**: In-memory run state only

**Testing**: Node built-in test runner for deterministic game logic; targeted management Common Test cases for routes; existing Mocha and Selenium infrastructure for browser integration

**Target Platform**: Modern desktop and mobile browsers with WebGL; readable failure screen when renderer initialization fails

**Project Type**: Browser game shipped in an existing web application

**Performance Goals**: Target 60 frames per second on a typical desktop, cap rendering at 60 frames per second, show collision results within 100 milliseconds under normal foreground scheduling

**Constraints**: Local assets only; configured path prefix respected; no management refresh timers or API polling; `q` exits from all states; 360 by 640 minimum viewport; no changes to broker data or access policy

**Scale/Scope**: One player and one active run per page; no score persistence or backend gameplay service

## Constitution Check

Pre-research gates pass: the route is additive, public interfaces remain compatible, verification is restricted to affected cases, and repository prose and comment rules apply.

Post-design gates pass: existing static resource access rules are reused, no API schema or broker state changes are introduced, and targeted test commands are documented. The implementation must use the `pixijs` router and appropriate application, ticker, assets, sprite, graphics, events, and performance skills. No exceptions are required.

## Project Structure

### Documentation (this feature)

```text
specs/001-bunny-hop/
├── spec.md
├── delivery-constraints.md
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/ui.md
└── checklists/requirements.md
```

### Source Code (repository root)

Proposed additions and affected existing files:

```text
deps/rabbitmq_management/
├── src/rabbit_mgmt_dispatcher.erl
├── priv/www/
│   ├── bunny-hop.html
│   ├── css/bunny-hop.css
│   ├── img/bunny-hop/rabbitmq-logo.png
│   └── js/bunny-hop/
│       ├── bootstrap.js
│       ├── game.js
│       ├── model.js
│       └── vendor/
│           ├── pixi-8.22.0.min.mjs
│           ├── LICENSE
│           └── README.md
└── test/
    ├── rabbit_mgmt_http_SUITE.erl
    └── bunny-hop.test.js
selenium/
├── test/bunny-hop/game.js
└── suites/bunny-hop.sh
```

**Structure Decision**: Add an explicit static document route before the static catchall in `build_routes/1`. Use existing prefix handling rather than the API module dispatcher, which adds `/api`. Serve modules, styles, and a copy of the logo from the management plugin's own `priv/www`. Do not depend on the Prometheus plugin at runtime. The proposed Selenium suite should reuse existing basic-auth setup and support a configured prefix.

## Design and implementation sequence

1. Add the standalone page and prefixed route; establish visible Exit and `q` handling before asynchronous module and renderer loading
2. Vendor the pinned PixiJS distribution and license with source URL and checksum; no Node installation is required to build or serve RabbitMQ
3. Implement the pure run model and shape collision tests independently of the renderer
4. Add the PixiJS application and logo sprite, Graphics obstacles, native HTML counters and state controls, resize handling, and complete teardown
5. Add focused route and browser validation, including both timer isolation and restoration of the prior management refresh preference

Use `new Application()` followed by awaited `init()`, a private ticker, WebGL preference, `autoStart: false`, and capped device resolution. Start rendering only during active play; render ready, paused, and results scenes once. Use `Assets.load()` for the local logo and handle initialization failures. Track disposal during asynchronous loading so an exit cannot attach a canvas later.

Keep physics in logical coordinates with a fixed visible course width, scaling uniformly to fit each viewport. Use a fixed simulation step of 1/120 second with capped elapsed time per frame and continuous collision checks across each step. Discard excess backlog after a long stall rather than fast-forwarding through obstacles. Reset timing on resume. Collision must be checked before clearance scoring.

Use an alpha-derived occupied-pixel mask for the supplied logo and simple filled obstacle shapes, with broad-phase bounds followed by shape checks including boundary contact. Ignore transparent padding and holes. Verify the PNG's alpha representation during implementation before constructing its mask. Do not approximate the entire logo by one rectangle. Use deterministic obstacle generation with a minimum gap derived from jump duration, speed, and a reaction margin of at least 500 milliseconds. Validate this at the narrowest viewport.

Exit stops the ticker, removes all registered listeners and observers, unloads owned textures, destroys the application and its owned scene resources, and navigates to the management root with `#/`. Page unload also disposes resources. The standalone document never loads `main.js`, `router.js`, or the management extension boot sequence, never installs a refresh interval, and never writes the refresh preference.

## Verification strategy

Map targeted tests to the scenarios in [quickstart.md](quickstart.md). Pure model tests cover scoring, shape boundaries, jump input edges, collision precedence, varied frame schedules, pause and reset, and obstacle spacing. HTTP cases cover document and assets with and without a prefix. Browser tests cover real keyboard and touch input, `q` from every state, loading failure, replay, visibility pause, resize, and no refresh across two configured intervals. Confirm normal management refresh resumes on return. Full suites remain for CI.

## Content Security Policy integration

Use the official pinned `dist/packages/unsafe-eval.min.js` compatibility extension before creating the renderer, alongside the production module. The extension replaces dynamic shader and uniform generation with static functions; do not relax management CSP. It expects a mutable global `PIXI` namespace, supplied from the imported module exports. Both distributions are unmodified, licensed, and verified against the package metadata SHA-256 values. Read the PixiJS environments skill for this integration.
