# Feature Specification: Bunny Hop

**Feature Branch**: `spec-rabbit`

**Created**: 2026-10-29

**Status**: Draft

**Input**: Create a RabbitMQ-themed endless runner in the management UI at `/bunny-hop`, inspired by Google's Dinosaur Game. The RabbitMQ logo jumps over obstacles. Show distance and hops at the top, end on any obstacle contact, and offer final stats, replay, and exit. Explicit delivery constraints are recorded in [delivery-constraints.md](delivery-constraints.md).

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Play a run (Priority: P1)

A management UI user opens Bunny Hop, reads the controls, and starts a run. The RabbitMQ logo moves through a scrolling course and jumps over RabbitMQ-themed obstacles while the user watches distance and hops increase.

**Why this priority**: Jumping over obstacles with visible progress is the core game experience.

**Independent Test**: Open the game, start a run, jump over an obstacle, and observe both counters.

**Acceptance Scenarios**:

1. **Given** the management UI is available, **When** a user opens `/bunny-hop`, **Then** a ready screen shows the RabbitMQ logo, controls, and a Start action, with both counters at zero
2. **Given** the ready screen, **When** the user starts, **Then** the course scrolls automatically and the logo runs toward approaching obstacles
3. **Given** a grounded character, **When** the user presses Space or Arrow Up, or taps the play area, **Then** the character jumps and returns to the ground
4. **Given** an active run, **When** an obstacle passes completely behind the character without contact, **Then** hops increases by exactly one
5. **Given** an active run, **When** the character jumps without clearing an obstacle, **Then** hops does not increase
6. **Given** an active run, **When** the course advances, **Then** distance increases and both labelled counters remain visible at the top

---

### User Story 2 - Finish and replay (Priority: P1)

A user who touches an obstacle sees the final distance and hops and can immediately play again.

**Why this priority**: A definite end and a fresh attempt complete the game loop.

**Independent Test**: Cause a collision, check the final values, then select Play again.

**Acceptance Scenarios**:

1. **Given** an active run, **When** any part of the character's visible silhouette touches an obstacle's visible shape, **Then** the run ends immediately and a results screen shows the final distance and hops
2. **Given** a finished run, **When** time passes or jump controls are used, **Then** the course and final counters remain unchanged
3. **Given** the results screen, **When** Play again is selected, **Then** a new run starts with zero distance, zero hops, and the character grounded, without obstacles carried over from the previous run
4. **Given** an obstacle causes a collision, **When** results appear, **Then** that obstacle is not counted as a hop

---

### User Story 3 - Leave and return safely (Priority: P2)

A user can exit the game and return to normal management work without the run continuing in the background.

**Why this priority**: The game must fit within the management UI without disrupting its primary purpose.

**Independent Test**: Exit from the ready screen, an active run, and the results screen, then open the game again.

**Acceptance Scenarios**:

1. **Given** any game screen, **When** Exit is selected, **Then** the management overview appears and game movement and game input handling stop
2. **Given** the user exited, **When** the game is opened again, **Then** it shows a fresh ready screen with zero counters
3. **Given** an active run, **When** the tab becomes hidden or loses focus, **Then** the run pauses with counters unchanged until the user explicitly resumes
4. **Given** a paused run, **When** Resume is selected, **Then** the run continues from the same distance and obstacle positions without counting time spent paused

5. **Given** any game screen including a load error, **When** the user presses `q`, **Then** the game exits to the management overview
6. **Given** any game screen and an enabled management refresh preference, **When** two refresh intervals elapse, **Then** the game screen and run remain intact with no management refresh, and exiting restores normal management refresh behaviour

### Edge Cases

- Holding a jump key or tapping repeatedly while airborne must not create additional jumps before landing; a new press or tap is required after landing
- One jump clearing two obstacles counts two hops, each only after it is fully passed
- Contact at an obstacle edge ends the run; empty image padding does not count as contact
- A collision takes precedence over scoring for the colliding obstacle
- Resizing the window preserves the run state and keeps counters and actions visible at supported sizes
- A hidden tab or delayed frame must not cause an obstacle to pass through the character without collision detection
- If the game cannot load or run, show an explanatory message and an Exit action without starting a run
- Start, Play again, Resume, and Exit activation must not also trigger a jump

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The management UI MUST serve Bunny Hop at `/bunny-hop`, respecting any configured management path prefix
- **FR-002**: The game MUST show a ready screen with Start, Exit, and concise control instructions before movement begins
- **FR-003**: The playable character MUST use the supplied orange RabbitMQ logo with its proportions preserved, and the course MUST use RabbitMQ messaging motifs such as messages and queues for obstacles
- **FR-004**: After Start, the character MUST run automatically through a continuous scrolling course until collision or exit, with jumps controlled by Space, Arrow Up, and a tap or click on the play area
- **FR-005**: The character MUST only initiate a jump from the ground in response to a new jump input and MUST return to the ground after each jump
- **FR-006**: The course MUST generate obstacles with enough approach time and spacing to allow every obstacle to be cleared using the available jump; no unavoidable sequence is permitted
- **FR-007**: The top of the game MUST display labelled nonnegative integer Distance and Hops counters throughout a run
- **FR-008**: Distance MUST represent cumulative course travel in game distance units, rounded down for display, start at zero, and advance only during active play; equal course travel MUST give equal distance regardless of display refresh rate
- **FR-009**: Hops MUST start at zero and increase exactly once per obstacle fully cleared without contact, regardless of the number of jump inputs
- **FR-010**: Any contact between visible character and obstacle shapes MUST immediately end the run, freeze movement and scoring, and show final Distance and Hops values matching the run counters
- **FR-011**: The results screen MUST offer Play again and Exit; Play again MUST start a fresh run with zero counters and no previous obstacles or pending inputs
- **FR-012**: Exit MUST be available on every game screen through a visible action and the `q` key, return to the management overview, and stop all game activity and game-specific input handling
- **FR-013**: The game MUST pause when hidden or unfocused and require an explicit Resume action before movement and scoring continue
- **FR-014**: Controls and counters MUST remain usable at viewport sizes from 360 by 640 through 1920 by 1080; resizing MUST preserve run progress
- **FR-015**: Start, Play again, Resume, and Exit MUST be keyboard reachable with visible focus, and jumping MUST not scroll the page during active play
- **FR-016**: Failure to load required game resources or start the game MUST show a readable failure message and allow exit
- **FR-017**: Playing MUST leave broker configuration, queues, messages, permissions, existing management routes, and public interfaces unchanged

- **FR-018**: The game screen MUST have no management page auto-refresh in ready, active, paused, finished, or error states; leaving the game MUST preserve the management UI refresh preference

### Key Entities

- **Run**: One attempt, with ready, active, paused, finished, and exited states, cumulative distance, and obstacles cleared
- **Character**: The RabbitMQ logo, with grounded or airborne state and a visible shape used for contact
- **Obstacle**: An approaching course object with a position, visible shape, and whether it has already been counted
- **Run result**: The frozen distance and hops for the completed attempt, displayed until replay or exit

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A user can open the game, read the instructions, and start a run within 10 seconds after the ready screen appears
- **SC-002**: Across 20 controlled obstacle passes, Hops equals the exact number cleared without contact, including jumps that clear zero or multiple obstacles
- **SC-003**: Across 20 controlled contacts, every contact ends the run and displays matching final counters within 100 milliseconds of contact
- **SC-004**: Ten consecutive replay cycles each start with zero counters and no obstacles from an earlier run
- **SC-005**: Exit from each game state returns to the management overview within one second and leaves normal management keyboard and pointer controls usable
- **SC-006**: At 360 by 640, 768 by 1024, and 1920 by 1080, users can read both counters and operate every available action without horizontal scrolling
- **SC-007**: Pausing a run for 30 seconds adds zero distance and zero hops, and resuming preserves the obstacle positions from the pause

## Assumptions

- This is a single-player endless runner with one jump action; fixed speed is sufficient for the initial version
- Distance uses abstract game units rather than real-world metres; Hops counts obstacles cleared rather than jump inputs
- Scores last only for the current visit; persistent records, leaderboards, multiplayer, audio, and broker-driven gameplay are outside this feature
- Exit means returning to the management overview because browsers may not allow a page to close its own tab
- Existing management access rules apply; the feature introduces no separate accounts or permissions
- Game resources are delivered with the management plugin so play does not depend on an external asset service
- The referenced logo is available for reuse; delivery constraints and its repository location are recorded separately for planning
