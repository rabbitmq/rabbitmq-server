# Bunny Hop data model

All values are local to one page; there is no persistence or API data model.

## Game session

Fields: `state`, `run`, `result`, `disposed`, and owned renderer, asset, listener, and resize-observer handles. States are loading, ready, active, paused, finished, error, and exited.

Transitions:

| From | Trigger | To |
| --- | --- | --- |
| loading | Successful initialization | ready |
| loading | Initialization failure | error |
| ready | Start | active |
| active | Hidden tab or lost focus | paused |
| paused | Explicit Resume while visible and focused | active |
| active | Contact | finished |
| finished | Play again | active with fresh run |
| Any state except exited | Exit or `q` | exited |
| Any live renderer state | Unrecoverable render failure | error |

Exit is idempotent. Async initialization must check disposal after each await. Ready, paused, finished, and error states do not advance simulation. Action inputs do not propagate into jump input handling.

## Run

Fields: cumulative `distance` as a finite nonnegative number, integer `hops`, simulation time, fixed-step remainder, speed, character, obstacle list, next spawn position, and deterministic random source.

Display distance is the floor of cumulative course travel. Only active simulation changes counters. Collision ends advancement at first contact. Replay creates a new run and clears pending jump input, held-key state, timing remainder, and obstacles.

## Character

Fields: fixed horizontal position, vertical position, vertical velocity, grounded state, dimensions, and occupied-pixel mask transformed into logical coordinates. The displayed logo and mask use identical scale and origin.

A jump is accepted only for a fresh input while grounded. Empty padding and holes do not collide. Landing restores the ground position exactly.

## Obstacle

Fields: unique run-local ID, message or queue motif, current and previous position, dimensions, filled collision shape, and `counted` flag.

The obstacle list belongs to a run. Minimum spawn spacing guarantees a landing and next jump opportunity with at least 500 milliseconds of initial reaction time. For each simulation step, resolve contact first; only an uncontacted obstacle whose trailing edge has passed the character can increment hops. Each obstacle contributes at most one hop and is removed after leaving the course.

## Result

Fields: final displayed distance and integer hops copied at contact. Values are immutable until replay or exit. The colliding obstacle contributes no hop.
