# Message Deferral for Quorum Queues

## Overview

Message deferral lets an AMQP 1.0 consumer return a message to a quorum queue in a
*parked* state under a client-chosen token, then later pull that specific message back
on demand — without waiting for a delayed-retry timer or competing with other consumers.

This is useful for workload scheduling patterns where a consumer wants to decide at
receive time that a message should not be processed immediately, assign it a token
for later retrieval, and then fetch it explicitly when ready.

## Constraints

- **Quorum queues only.** Classic queues and streams do not support deferral tokens.
  Clients can detect support by checking for `rabbitmq:deferral-tokens` in the
  `offered-capabilities` field of the ATTACH response.
- **`x-opt-delivery-time` is required.** A deferral token alone does not park a
  message. Both `x-opt-deferral-token` and `x-opt-delivery-time` must be present in
  the MODIFIED outcome's `message-annotations`. A message returned via the
  `delayed-retry` queue configuration never creates a deferred entry, even if a token
  is present.
- **Tokens must be of AMQP type `utf8`.** This applies both to `x-opt-deferral-token`
  in the MODIFIED outcome's `message-annotations` and to each element of the
  `rabbitmq:deferral-tokens` array in a FLOW frame's `properties`. Any other AMQP
  type (e.g. `binary`, `symbol`) is rejected with `amqp:invalid-field`.
- **Credit must cover the matched messages.** When requesting deferred messages via a
  FLOW frame, the link credit granted in that same FLOW must be at least as large as
  the number of messages the submitted tokens resolve to (a single token may resolve
  to more than one message; see below). If credit is exhausted by normal message
  delivery before the deferred assignment runs, no deferred messages are delivered
  for that FLOW.

## Protocol Usage

### 1. Attach a consuming link and verify capability

Attach a link to a quorum queue source. Inspect the `offered-capabilities` array in
the ATTACH response for the symbol `rabbitmq:deferral-tokens`. If absent, the queue
does not support deferral.

### 2. Receive a message

The broker delivers a message to the link via a TRANSFER frame. Note the
`delivery-tag` for the settlement step.

### 3. Park the message with a deferral token

Send a DISPOSITION frame settling the delivery with a MODIFIED outcome. Include both
annotations in the `message-annotations` map of the MODIFIED outcome:

- `x-opt-deferral-token` (symbol key, utf8 value) — a client-chosen opaque
  identifier for this parked message. The same token may be assigned to more than
  one message, e.g. by settling a range of deliveries (`first =/= last`) with a
  single MODIFIED outcome. Retrieving such a token returns all messages parked
  under it, oldest first.
- `x-opt-delivery-time` (symbol key, timestamp value in milliseconds since the Unix
  epoch) — the earliest time at which the message becomes eligible for normal
  timer-based redelivery. The message is held until this time unless explicitly
  retrieved earlier via `assign_deferred`.

Example (pseudocode):

```
DISPOSITION {
  role = receiver,
  first = <delivery-id>,
  settled = true,
  state = MODIFIED {
    delivery-failed = false,
    undeliverable-here = false,
    message-annotations = {
      x-opt-deferral-token: "job-42-retry-1",
      x-opt-delivery-time: 1780000000000
    }
  }
}
```

### 4. Retrieve the message by token

When ready to process the parked message, send a FLOW frame on the same consuming
link. Grant enough link credit to receive the deferred messages and include the tokens
in the `properties` map of the FLOW frame under the key `rabbitmq:deferral-tokens` as
an array of utf8 values.

Example (pseudocode):

```
FLOW {
  handle = <link-handle>,
  delivery-count = <current-delivery-count>,
  link-credit = 1,
  properties = {
    rabbitmq:deferral-tokens: ["job-42-retry-1"]
  }
}
```

The broker looks up each token in the parked message set and delivers any matches
directly to this link as normal TRANSFER frames. Tokens that are not found (because
the message was already expired by its delivery time and requeued normally, or the
token was never issued) produce no delivery — the client is responsible for tracking
which tokens it expects.

## Relationship to `delayed-retry`

Deferral tokens and the queue-level `x-delayed-retry-*` configuration are
complementary but independent. A message returned with `x-opt-delivery-time` follows
the deferral path. A message returned without `x-opt-delivery-time` follows the
delayed-retry path if the queue is configured for it. The two paths do not interact:
delayed-retry messages cannot be retrieved by token.
