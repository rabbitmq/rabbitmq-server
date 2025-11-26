# Consumer Timeouts

Consumer timeouts (also known as "message locks") limit how long a consumer can hold unacknowledged messages before RabbitMQ takes action.

**Note:** Consumer timeouts are **only supported for quorum queues**. Classic queues and stream queues do not support consumer timeouts.

## Overview

When a consumer receives a message in manual acknowledgement mode, it has a limited time to acknowledge (or reject) the message. If the timeout expires before the message is settled, RabbitMQ:

1. Returns the message to the queue (making it available for redelivery)
2. Marks the consumer as "timed out"
3. Notifies the client via a protocol-specific mechanism

## Configuration

Consumer timeouts can be configured at four levels:

### 1. Consumer Argument (Highest Priority)

Set via the `x-consumer-timeout` argument when creating a consumer (in milliseconds):

```erlang
#'basic.consume'{arguments = [{<<"x-consumer-timeout">>, long, 60000}]}
```

This takes absolute precedence over all other settings.

### 2. Queue Argument

Set via the `x-consumer-timeout` argument when declaring a queue (in milliseconds):

```erlang
#'queue.declare'{arguments = [{<<"x-consumer-timeout">>, long, 60000}]}
```

### 3. Queue Policy

Set via a policy with the `consumer-timeout` key (in milliseconds):

```bash
rabbitmqctl set_policy consumer-timeout-policy ".*" \
  '{"consumer-timeout": 60000}' --apply-to quorum_queues
```

**Note:** When both a queue argument and a policy are set, the **minimum** of the two values is used.

### 4. Global Configuration (Lowest Priority)

Set in `rabbitmq.conf`:

```ini
consumer_timeout = 1800000
```

**Default:** 1,800,000 milliseconds (30 minutes)

### Precedence Summary

1. Consumer argument (`x-consumer-timeout` on `basic.consume`) - if set, used directly
2. Otherwise: min(queue argument, queue policy) - if either is set
3. Otherwise: global `consumer_timeout` configuration
4. Otherwise: default of 30 minutes

## Queue Type Support

| Queue Type | Consumer Timeout Support |
|------------|-------------------------|
| Quorum queues | Full support |
| Classic queues | Not supported |
| Stream queues | Not supported |

## Changes from v4.2.x

In RabbitMQ 4.2.x and earlier, consumer timeouts **always closed the channel** with a `precondition_failed` error, regardless of client capabilities. This was disruptive as it terminated all consumers on the channel, not just the one that timed out.

Starting with RabbitMQ 4.3, the behaviour is more graceful for clients that support the `consumer_cancel_notify` capability (which most modern clients do). Instead of closing the channel, the server sends a `basic.cancel` notification to cancel only the timed-out consumer, leaving the channel and other consumers intact.

Clients that do not advertise the `consumer_cancel_notify` capability will still experience channel closure on timeout, maintaining backwards compatibility.

## Timeout Behaviour

When a consumer timeout occurs on a quorum queue:

1. **Message return:** Timed-out messages are returned to the queue with "ready" status, available for redelivery to any consumer
2. **Consumer state:** The consumer enters "timeout" status and will not receive new messages
3. **Partial timeout:** Only messages that have exceeded their timeout are returned; other checked-out messages remain with the consumer until they also time out
4. **Recovery:** The consumer can resume receiving messages only after all timed-out messages are settled (acknowledged, rejected, or the consumer is cancelled)

### Protocol-Specific Actions

When a timeout occurs, the server notifies the client using protocol-specific mechanisms:

#### AMQP 0.9.1

- **With `consumer_cancel_notify` capability:** Server sends `basic.cancel` to the consumer, channel remains open
- **Without `consumer_cancel_notify` capability:** Server closes the channel

#### AMQP 1.0 (Planned)

Options under consideration (in order of preference):

1. Spontaneously release the message using the `released` outcome (§3.4.4), if client supports it
2. Detach the link with an error (and cancel the consumer with the queue)
3. Terminate the session

#### MQTT / STOMP (Planned)

- Terminate the connection process

## Design Rationale

### Long vs Short Message Locks

RabbitMQ uses relatively long message locks (default 30 minutes) without lock renewal, unlike some other message brokers. For comparison, Azure Service Bus defaults to 1-minute locks with a 5-minute maximum, requiring explicit lock renewal.

This design choice reflects that:

1. AMQP 0.9.1 has no protocol mechanism for lock renewal
2. Many RabbitMQ workloads involve long-running message processing
3. Short locks with mandatory renewal would be a significant change in programming model

### Future Considerations: Lock Renewal

If lock renewal were to be supported in the future, one option for AMQP 1.0 would be to use the `received` outcome as a heartbeat mechanism to renew message locks. The `received` outcome is non-terminal and can be sent multiple times. However, this would need careful design to avoid conflicts with its primary use case for resumable large message transfers.
