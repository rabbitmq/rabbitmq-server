# Bunny Hop UI and delivery contract

## HTTP resources

| Resource | Behaviour |
| --- | --- |
| `/bunny-hop` | GET returns the dedicated game HTML document |
| `<prefix>/bunny-hop` | Same document when a management prefix is configured |
| `<prefix>/js/bunny-hop/*` | Locally packaged game modules and pinned engine |
| `<prefix>/css/bunny-hop.css` | Game styling |
| `<prefix>/img/bunny-hop/rabbitmq-logo.png` | Management-owned copy of the supplied logo |

Use existing static content types, caching and access rules. No game HTTP API, new credentials, or broker mutations. The canonical entry path has no trailing slash; if an alias with a trailing slash is provided, redirect to the canonical path so relative asset resolution is unchanged.

## Screens and actions

| State | Visible information | Actions |
| --- | --- | --- |
| Loading | Loading status | Exit, `q` |
| Ready | Logo, instructions, Distance 0, Hops 0 | Start, Exit, `q` |
| Active | Course and top Distance and Hops counters | Space, Arrow Up, play-area pointer input, Exit, `q` |
| Paused | Counters and paused status | Resume, Exit, `q` |
| Finished | Frozen final Distance and Hops | Play again, Exit, `q` |
| Error | Readable error message | Exit, `q` |

Use native buttons and a real Exit anchor with visible focus. Accept unmodified `q` independent of letter case, ignore key repeats and browser shortcuts using modifier keys. Jump inputs must not scroll the page or activate twice through bubbling. Exit removes game handlers before navigating to `<prefix>/#/`.

## Refresh and lifecycle invariants

The document loads a small classic `bootstrap.js` before importing game modules, so `q` and a native Exit anchor remain available even when the engine import fails. The document loads no management shell scripts, creates no management refresh timer, polls no management API, and does not change the stored refresh preference. This applies to loading, ready, active, paused, finished, and error screens. Browser reload initiated by the user remains ordinary navigation.

On hidden-tab or window-blur events pause active play, clear held inputs, and stop the ticker. Resume resets the frame baseline. Leaving by Exit, `q`, or browser navigation releases all owned resources. Reopening starts at ready with zero counters.

## Visual behaviour

Preserve the supplied logo silhouette and orange colour. Counters remain at the top. Fit a fixed logical course uniformly into available width without distorting the logo or changing jump physics. Keep controls usable from 360 by 640 upwards. No score or state is sent to the broker.
