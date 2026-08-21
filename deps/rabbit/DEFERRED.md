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
- **At most 256 tokens per FLOW.** A `rabbitmq:deferral-tokens` array longer than
  that is rejected with `amqp:invalid-field`.
- **Credit does not have to cover the matched messages.** Submitting tokens claims
  the messages parked under them for the link; the queue then delivers them as the
  link's credit allows, over as many credit top-ups as it takes. A client that
  grants less credit than the tokens resolve to receives the rest on subsequent
  credit grants, without submitting the tokens again.
- **A token is claimed by one link at a time.** Submitting a token removes it from
  the queue's set of parked tokens, so a second link submitting the same token
  receives nothing. The claim is released, and the token becomes available again,
  when the claiming link detaches with messages still parked under it.

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
  single MODIFIED outcome. Retrieving such a token returns every message parked
  under it, oldest first, spread over as many credit grants as it takes. Reusing
  a token that still has messages parked under it adds to that set rather than
  replacing it.
- `x-opt-delivery-time` (symbol key, timestamp value in milliseconds since the Unix
  epoch) — the earliest time at which the message becomes eligible for normal
  timer-based redelivery. The message is held until this time unless the client
  retrieves it earlier by its token.

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
link, granting link credit as usual and including the tokens in the `properties` map
of the FLOW frame under the key `rabbitmq:deferral-tokens` as an array of utf8 values.

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

The broker claims each token's parked messages for this link and delivers them as
normal TRANSFER frames, ahead of any messages already ready in the queue: the credit
granted in this FLOW reaches the messages the client asked for rather than being
spent on the backlog first.

Tokens that are not found produce no delivery. That happens when the token was never
issued, when its messages were already requeued normally after their delivery time
elapsed, or when another link holds the claim. The client is responsible for tracking
which tokens it expects; each delivered message carries its own `x-opt-deferral-token`
in `message-annotations`, so a client can tell which of its tokens have been served.

A claim does not pin a message. If the client never grants the credit to collect a
claimed message, the message is still requeued normally once its
`x-opt-delivery-time` elapses, and the claim is dropped.

## Relationship to `delayed-retry`

Deferral tokens and the queue-level `x-delayed-retry-*` configuration are
complementary but independent. A message returned with `x-opt-delivery-time` follows
the deferral path. A message returned without `x-opt-delivery-time` follows the
delayed-retry path if the queue is configured for it. The two paths do not interact:
delayed-retry messages cannot be retrieved by token.
