# Tasks: Bunny Hop

**Input**: Design documents from `specs/001-bunny-hop/`

**Prerequisites**: `plan.md`, `spec.md`, `research.md`, `data-model.md`, `contracts/ui.md`, and `quickstart.md`

**Validation**: Tasks include the targeted model, route, and browser checks specified by the feature acceptance scenarios and design validation guide. Do not run full component or repository suites locally.

**Organization**: Tasks are grouped by user story. All implementation is JavaScript, with an additive Erlang route change and locally packaged PixiJS `8.22.0`. Apply relevant PixiJS skills before engine work. No gameplay service, persistence, broker mutations, or refresh-preference changes are required.

## Format and paths

Each checkbox has a sequential ID, optional `[P]` marker, story label within story phases, and repository-relative file paths. `[P]` identifies work on separate files that may proceed concurrently after its stated prerequisite phase. Tasks sharing a file must remain sequential.

## Phase 1: Setup (Shared Infrastructure)

Prepare local assets and record the required implementation skills.

- [X] T001 Read the `pixijs` router and relevant application, ticker, assets, sprite, graphics, events, and performance skills before engine edits; confirm JavaScript-only implementation and record the skill and version decisions in `specs/001-bunny-hop/research.md`
- [X] T002 [P] Vendor the pinned PixiJS `8.22.0` production browser module into `deps/rabbitmq_management/priv/www/js/bunny-hop/vendor/pixi-8.22.0.min.mjs`, retaining `vendor/LICENSE` and recording official source URL, version, and verified checksum in `vendor/README.md`; require no runtime CDN or new RabbitMQ build prerequisite
- [X] T003 [P] Inspect the supplied PNG alpha and padding, then copy `deps/rabbitmq_prometheus/docker/grafana/publish/rabbitmq-logo.png` into `deps/rabbitmq_management/priv/www/img/bunny-hop/rabbitmq-logo.png` without changing its proportions or orange colour; record mask-relevant findings in `specs/001-bunny-hop/research.md`

## Phase 2: Foundational (Blocking Prerequisites)

Establish static delivery, early exit, timer isolation, and shared run ownership before gameplay.

- [X] T004 Add the canonical `/bunny-hop` static document route in `deps/rabbitmq_management/src/rabbit_mgmt_dispatcher.erl` before the catchall using `rabbit_mgmt_wm_static` and `priv_file`; reuse prefix application and existing static access policy, without adding `/api` or changing existing routes
- [X] T005 Create `deps/rabbitmq_management/priv/www/bunny-hop.html` with native loading and failure status, labelled counters, controls, and a real Exit anchor; register unmodified case-insensitive `q` before dynamically importing `js/bunny-hop/game.js`, preserve the prefix in assets and `./#/`, and catch module-load failures; load no management bootstrap scripts, refresh intervals, or API polling and never write refresh preferences
- [X] T006 Define shared session and fresh-run factories in `deps/rabbitmq_management/priv/www/js/bunny-hop/model.js` using `specs/001-bunny-hop/data-model.md`: states loading, ready, active, paused, finished, error, exited; distance is a "finite nonnegative number", hops an integer; "Ready, paused, finished, and error states do not advance simulation"
- [X] T007 Create `deps/rabbitmq_management/priv/www/js/bunny-hop/game.js` with explicit ownership of renderer, textures, observers, and listeners; "Exit is idempotent" and "Async initialization must check disposal after each await" so navigation during import, renderer initialization, or asset loading cannot attach a late canvas

## Phase 3: User Story 1 - Play a run (Priority: P1)

Deliver the playable logo runner with correct controls, obstacle clearance, and live counters.

**Independent Test**: Open the prefixed and default game routes, start, jump with keyboard and pointer, clear an obstacle, and verify Distance advances and Hops increments once.

- [X] T008 [P] [US1] Add deterministic gameplay acceptance cases in `deps/rabbitmq_management/test/bunny-hop.test.js` for grounded-only input, held-key rejection, exact landing, zero and multiple clears, 20 obstacle passes, frame schedule equivalence, delayed frames, and valid spacing; use the existing Node test runner and confirm failures before implementing the behaviour
- [X] T009 [P] [US1] Add `bunny_hop_test` in `deps/rabbitmq_management/test/rabbit_mgmt_http_SUITE.erl` and register it through `some_tests()` for both prefix groups; assert document and all game assets, JavaScript module MIME types, local URLs, and absence of management refresh bootstrap scripts
- [X] T010 [US1] Implement jump integration and deterministic obstacle generation in `deps/rabbitmq_management/priv/www/js/bunny-hop/model.js`; "A jump is accepted only for a fresh input while grounded", "Landing restores the ground position exactly", and minimum spacing permits a landing and next jump with "at least 500 milliseconds of initial reaction time"
- [X] T011 [US1] Implement course travel and clearance in `deps/rabbitmq_management/priv/www/js/bunny-hop/model.js`; "Display distance is the floor of cumulative course travel", "Only active simulation changes counters", "Each obstacle contributes at most one hop and is removed after leaving the course"; give obstacles unique run-local IDs, previous positions, and counted flags
- [X] T012 [US1] Initialize PixiJS in `deps/rabbitmq_management/priv/www/js/bunny-hop/game.js` with `new Application()` then awaited `init()`, private ticker, WebGL preference, `autoStart: false`, capped device resolution and 60 FPS; load the local logo with `Assets.load()`, render a logo Sprite and message and queue Graphics obstacles, and expose readable initialization errors
- [X] T013 [US1] Connect Space, Arrow Up, and play-area pointer input plus Start and the top Distance and Hops counters in `deps/rabbitmq_management/priv/www/js/bunny-hop/game.js`; reject repeats and airborne inputs, prevent page scrolling during play, keep action activation from generating a jump, and use 1/120-second model steps with capped elapsed time and discarded excess backlog
- [X] T014 [P] [US1] Implement responsive layout and visible keyboard focus in `deps/rabbitmq_management/priv/www/css/bunny-hop.css`; keep native controls and counters usable from 360 by 640 to 1920 by 1080 with no horizontal scrolling and reserve a uniformly scaled fixed logical course
- [X] T015 [US1] Implement canvas resize and logical-to-display transforms in `deps/rabbitmq_management/priv/www/js/bunny-hop/game.js` without changing physics or progress; verify identical sprite and collision-mask scale and origin and stop rendering when not active

## Phase 4: User Story 2 - Finish and replay (Priority: P1)

End every contact with frozen stats and support a clean replay.

**Independent Test**: Cause a visible-shape contact, confirm matching frozen stats, then replay ten times with zero counters and no carried-over obstacles.

- [X] T016 [US2] Extend `deps/rabbitmq_management/test/bunny-hop.test.js` with occupied-logo body and ear boundaries, empty padding and holes, swept contact on delayed frames, collision-before-scoring, immutable result, and ten clean replay cases; establish failing cases before implementation
- [X] T017 [US2] Implement alpha-mask broad-phase and swept visible-shape contact in `deps/rabbitmq_management/priv/www/js/bunny-hop/model.js`; "Empty padding and holes do not collide" and "The displayed logo and mask use identical scale and origin"; include boundary contact, stop at first contact, and resolve contact before clearance scoring
- [X] T018 [US2] Implement immutable final results and fresh replay transitions in `deps/rabbitmq_management/priv/www/js/bunny-hop/model.js`; "The colliding obstacle contributes no hop", "Values are immutable until replay or exit", and "Replay creates a new run and clears pending jump input, held-key state, timing remainder, and obstacles"
- [X] T019 [US2] Connect finished state and Play again in `deps/rabbitmq_management/priv/www/js/bunny-hop/game.js` and native results controls in `deps/rabbitmq_management/priv/www/bunny-hop.html`; stop the ticker on contact, show matching final counters within 100 milliseconds in normal foreground scheduling, ignore finished-state jump inputs, and clear old scene objects before replay

## Phase 5: User Story 3 - Leave and return safely (Priority: P2)

Complete exit, pause and resume, resource cleanup, and refresh-preference regression coverage.

**Independent Test**: Exit with `q` and the visible action from every state, reopen to zero counters, pause for 30 seconds without progress, and verify refresh stays absent in the game and resumes with the saved preference on return.

- [X] T020 [US3] Extend `deps/rabbitmq_management/test/bunny-hop.test.js` with pause and explicit resume, zero progress during 30 seconds paused, input clearing, idempotent exit and fresh reopen cases using `specs/001-bunny-hop/data-model.md` transition rules
- [X] T021 [US3] Implement visibility and blur pause with explicit visible-and-focused Resume in `deps/rabbitmq_management/priv/www/js/bunny-hop/game.js` and `deps/rabbitmq_management/priv/www/js/bunny-hop/model.js`; "Resume resets the frame baseline", clear held inputs, stop the ticker, preserve obstacle positions, and prevent action inputs from becoming jumps
- [X] T022 [US3] Complete disposal and wire it to early `q`, visible Exit, and page unload in `deps/rabbitmq_management/priv/www/js/bunny-hop/game.js` and `deps/rabbitmq_management/priv/www/bunny-hop.html`; remove listeners and observers, unload owned textures and destroy owned scene and renderer resources, ignore modified and repeated exit keys, and return to the prefixed overview within one second without surviving game handlers
- [X] T023 [P] [US3] Add focused browser suite setup in `selenium/suites/bunny-hop.sh`, reusing existing broker and browser helpers and running only `selenium/test/bunny-hop/game.js`; support default and configured-prefix runs without enabling a full suite
- [X] T024 [US3] Add end-to-end scenarios in `selenium/test/bunny-hop/game.js` for real jump input, exact counters, collision and ten replays, 30-second visibility pause, supported viewports and resize, focus, and visible Exit and `q` from loading, ready, active, paused, finished, and error; force delayed or failed module and logo loading to verify early exit and late-init disposal
- [X] T025 [US3] Add refresh regression cases in `selenium/test/bunny-hop/game.js`: wait at least 12 seconds in each game state with a five-second management refresh preference, assert no reload, management polling, canvas replacement, or run reset, then exit and confirm unchanged preference and normal polling; repeat with refresh disabled and verify no broker mutations

## Phase 6: Polish & Cross-Cutting Concerns

Run only affected checks, reconcile documentation, and review the completed change.

- [ ] T026 Run `node --test test/bunny-hop.test.js` and the two prefix-specific `bunny_hop_test` Common Test commands from `deps/rabbitmq_management`, then the focused `selenium/suites/bunny-hop.sh` default and prefix variants; follow `CONTRIBUTING.md` and record actual results and any environment blockers in `specs/001-bunny-hop/quickstart.md`
- [X] T027 Complete manual timing, logo outline, keyboard focus, narrow viewport, local-asset delivery, normal management navigation, and 60 FPS target checks from `specs/001-bunny-hop/quickstart.md`; reconcile actual paths and runnable commands with `specs/001-bunny-hop/plan.md` and `specs/001-bunny-hop/contracts/ui.md`
- [X] T028 Review the change against `specs/001-bunny-hop/spec.md` FR-001 through FR-018 and SC-001 through SC-007 and the repository rules; perform up to twenty iterative reviews, stop after three consecutive reviews find no meaningful improvements, and record completion and remaining limitations in `specs/001-bunny-hop/quickstart.md`

## Dependencies & Execution Order

```text
Setup T001–T003
  -> Foundation T004–T007
  -> US1 T008–T015
  -> US2 T016–T019
  -> US3 T020–T025
  -> Validation and review T026–T028
```

Setup asset tasks T002 and T003 can proceed together after T001. Foundation is sequential and blocks all story phases. Within each story, follow task order except the explicit parallel examples below. Tests are written against the model boundary before implementing the corresponding behaviour; run those focused cases again after implementation.

US1 supplies the running course needed to exercise US2 contact and replay. US3 builds on that complete game loop to exercise exit from finished state as well as active play. Each story has its own acceptance check, but integration follows this dependency order because the renderer, model, and browser scenario files are shared. Basic Exit and timer isolation already exist in the foundation; US3 completes and verifies the lifecycle.

## Parallel execution examples

- Setup: T002 vendors the engine while T003 inspects and copies the logo in separate files
- US1: T008 model tests, T009 HTTP delivery checks, and T014 CSS can proceed together once the foundation exists; T010–T013 and T015 remain sequential in their implementation files
- US2: No implementation task is marked parallel because model and controller changes depend on the preceding collision and replay work; after T019, the focused model tests can run while a reviewer manually checks the results screen
- US3: T023 prepares the Selenium suite while T020 adds model lifecycle tests; both follow US2 and use separate files; T024 needs T021–T023, and T025 follows T024 in the same browser test file

These are scheduling opportunities, not instructions to spawn agents automatically.

## Implementation Strategy

The first playable increment is Setup, Foundation, and US1: open the dedicated page, start, jump, and observe accurate counters. Validate this increment before continuing. The smallest complete game release also needs US2, because collision, final stats, and replay complete the requested loop. Finish US3 before shipping so all exit states, refresh isolation, and return navigation are verified.

Use the pure model for reproducible scoring and collision fixtures rather than adding production-only test endpoints. Keep rendered assets and model collision shapes aligned. Preserve the existing public static-resource policy and management routes. Run only the checks described in `quickstart.md`, record actual outcomes, and leave incomplete tasks unchecked when environment limitations prevent verification.

## Implementation status

T001–T025 and T027–T028 are complete. Targeted model and HTML tests and both HTTP prefix cases pass. Direct Chrome checks against an isolated current-source broker cover gameplay, replay, pause, exit and refresh preferences. T026 remains open because the Selenium runner dependencies cannot be installed while npm registries are unreachable; see `quickstart.md` for exact completed checks and limitations. No checklist quality markers were changed during implementation.
