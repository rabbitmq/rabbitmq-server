# STOMP Adapter Enhancements

This document outlines proposed changes to the way in which the STOMP adapter handles destinations and routing.

## Destinations 

In the current STOMP adapter, much of the destination configuration is handled by custom headers, including `routing_key` and `exchange`. 

This design proposes to remove the custom headers, and use only the `destination` header with compound values that define the exchange, routing key and queue names.

### Standard AMQP Destinations

Any exchange/queue or exchange/routing key combination can be accessed using destinations prefixed with `/exchange`.

For `SUBSCRIBE` frames, a destination of the form `/exchange/<name>[/<pattern>]` can be used. This destination will:

1. Create an anonymous, auto-delete queue on `<name>` exchange.
2. If `<pattern>` is supplied, bind the queue to `<name>` exchange using `<pattern>`
3. Register a subscription against the queue, for the current STOMP session

For `SEND` frames, a destination of the form `/exchange/<name>[/<routing-key>]` can be used. This destination will:

1. Send to exchange `<name>`with the routing key `<routing-key>`

### Queue Destinations

For simple queue destinations with round-robin delivery semantics, destinations of the form `/queue/<name>` can be used.

For both `SEND` and `SUBSCRIBE` frames, these destinations will create the queue `<name>` and bind it to the `amq.direct` exchange with routing key `<name>`.

For `SEND` frames, the message will be sent to the `amq.direct` exchange with the routing key `<name>`. For `SUBSCRIBE` frames, a subscription against the queue `<name>` will be created for the current STOMP session.

Given that both `SEND` and `SUBSCRIBE` frames cause the queue to be created, the ordering constraint, client codes doesn't need to worry about creating a subscription before sending messages. Furthermore, the created queues are set to `auto-delete=false` so they will last until they are deleted by the user. 

### Topic Destinations

For simple topic destinations which deliver a copy of each message to all active subscribers, destinations of the form `/topic/<name>` can be used.

For `SEND` frames, the message will be sent to the `amq.topic` exchange with the routing key `<name>`. 

For `SUBSCRIBE` frames, an exclusive queue will be created and bound to the `amq.topic` exchange with routing key `<name>`. A subscription will be created against the exclusive queue.
