# Bunny Hop validation guide

The implementation is present. The commands below run focused validation; actual results and remaining environment limitations are recorded at the end of this guide.

## Prerequisites

Read `CONTRIBUTING.md` before testing. Use the repository-supported Erlang and Elixir versions, GNU Make 4 (`gmake` where available), Node `21` or newer for the existing management test runner, and the Selenium prerequisites documented in `selenium/README.md` for browser automation. Use a modern WebGL-capable browser.

## Build and launch

From the repository root:

```sh
gmake ENABLED_PLUGINS="rabbitmq_management" run-broker
```

Open `http://localhost:15672/bunny-hop`. Static game resources follow the management static-resource access policy; the game does not call broker APIs. Returning to the overview uses the normal management login flow when needed.

For prefix validation, configure `management.path_prefix = /rabbitmq` using the normal local broker configuration, restart the development broker, and open `http://localhost:15672/rabbitmq/bunny-hop`. Every game asset and Exit target must retain that prefix.

## Targeted automated checks

From `deps/rabbitmq_management`:

```sh
node --test test/bunny-hop.test.js
gmake ct-rabbit_mgmt_http t="all_tests_without_prefix:bunny_hop_test"
gmake ct-rabbit_mgmt_http t="all_tests_with_prefix:bunny_hop_test"
```

Model checks must include exact scoring for zero, one, and multiple cleared obstacles, mask boundary and empty-padding cases, collision before scoring, held keys, replay reset, pause and resume, varied frame schedules, delayed frames, and minimum obstacle spacing.

HTTP cases must check the dedicated HTML, engine and logo delivery, correct content types, prefix handling, and absence of management bootstrap scripts. Register the new case in the shared `some_tests()` list used by both prefix groups.

From `selenium`, run the focused suite:

```sh
./suites/bunny-hop.sh
```

The suite reuses the existing broker and browser setup and runs only `test/bunny-hop/game.js`. Use `BUNNY_HOP_PROFILES=mgt-prefix ./suites/bunny-hop.sh` for the configured-prefix variant. The suite uses the existing `basic-auth` configuration, whose prefix is `/my-prefix/another-prefix`. For interactive development follow `selenium/README.md` and run against the current source build rather than an older packaged image. Do not run the full management suite for this feature locally.

## Browser acceptance checks

1. Open the game at each route variant; verify logo, zero counters, instructions, Start and Exit, and local asset loading with no external network requests
2. Start and jump using Space, Arrow Up, and pointer input; confirm holding a key does not cause repeated jumps and the page does not scroll
3. Clear controlled obstacles and compare the counter against exactly 20 successful passes, including a jump clearing multiple obstacles and empty jumps
4. Cause controlled contacts at the body, ears, and shape boundaries; confirm empty padding and holes do not collide and final counters freeze within 100 milliseconds during normal foreground scheduling
5. Replay ten times; each attempt must have zero initial counters and no old obstacles or pending inputs
6. Hide the tab for 30 seconds and restore it; counters and obstacle positions must remain paused until Resume
7. Test 360 by 640, 768 by 1024, and 1920 by 1080 viewports and resize mid-run; controls and counters remain visible and progress is preserved
8. Use `q` and the visible Exit action from loading, ready, active, paused, finished, and error screens; verify return to the prefixed overview, no surviving canvas or game input handlers, and working management controls
9. Block the logo or engine request to force loading failure; verify a readable error and working Exit and `q` bindings

## Auto-refresh regression

In the normal management UI, enable the five-second refresh preference. Open the game and observe it for at least 12 seconds in each state: ready, active, paused, finished, and error. Verify no document reload, management refresh request, counter reset, or canvas replacement. Rendering frames are expected only during active play.

Record the document navigation count and observe network requests so the test distinguishes game animation from management refresh. Exit to the overview and verify that normal polling resumes using the same five-second preference. Repeat with refresh disabled and confirm that it remains disabled after return.

See [contracts/ui.md](contracts/ui.md) for delivery and control behaviour, and [data-model.md](data-model.md) for state invariants.

## Implementation verification

- `node --test test/bunny-hop.test.js`: 13 cases pass, including 20 clearance events, 20 collision events, and 40 successive generated obstacles
- `node --test test/html.test.js`: 6 cases pass, including the new game page
- `gmake ct-rabbit_mgmt_http t="all_tests_without_prefix:bunny_hop_test"`: passes
- `gmake ct-rabbit_mgmt_http t="all_tests_with_prefix:bunny_hop_test"`: passes, including the CSP compatibility script
- JavaScript syntax checks and `bash -n selenium/suites/bunny-hop.sh`: pass
- PixiJS engine and compatibility extension SHA-256 digests match the pinned package metadata
- Chrome renders the logo and course at 360 by 640, 768 by 1024, and 1920 by 1080 without horizontal scrolling
- The default management CSP remains unchanged; the official shader compatibility extension and main-thread logo decoding are required for it
- An isolated current-source broker is used for direct Chrome lifecycle and refresh checks
- The existing local EJS output was stale; `gmake compile-ejs-templates` regenerated it and all 62 template sanity checks pass
- The new Selenium suite has not run: dependency installation cannot reach either the configured corporate npm registry or the public registry; direct Chrome checks supplement the model and HTTP tests

The browser suite can be run once npm connectivity is restored. No existing user broker data or refresh preference is modified by the game.

Direct Chrome checks against the source broker confirm that ready, paused, finished, and active screens remain intact across more than two five-second refresh intervals without management API polling. A 30-second pause preserves counters. Active play clears five obstacles in 12 seconds, and `q` returns to the overview with five-second polling restored after each screen.

The direct Chrome run also passes ten replay cycles, preserves the disabled refresh preference on return, and exits from a forced logo-load failure after more than two refresh intervals. Browser request blocking was used to simulate the failure; console loader errors in that scenario are expected.

Final Chrome checks pass for visible keyboard focus, explicit resume, and `q` during delayed engine loading without a late canvas. The isolated headless browser renders 57 frames in a one-second sample against the 60 FPS cap, and the paused render loop produces no additional frames. This is a local sample, not a performance guarantee across devices.

Three consecutive final reviews found no further meaningful improvements. They covered source and lifecycle behaviour, requirements and validation gaps, and repository conventions. The only outstanding validation is running the checked-in Selenium suite once its dependencies can be installed.
