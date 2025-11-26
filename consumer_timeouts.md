## Consumer timeouts (message locks)

### Long vs short message locks

RabbitMQ currently emulates very long message locks (without lock renewal) in
it's current consumer timeout implementation (AMQP legacy) only. E.g. 30 minutes.

Azure Service Bus (for example) have much shorter locks of 1 minute by default
with a maximum of 5 minutes. In addition to this they implement a lock renewal
function which requires a specific AMQP endpoint.

I am doubtful that many RabbitMQ users would cope very well with short message
locks that required continuous renewal. AMQP legacy does not have a protocol
gesture that can be used for lock renewal anyway.

If we wanted to support renewal we _could_ consider using the `received` outcome
as a heartbeat type of thing that has the effect of renewing the message lock(s)
for the referenced messages. `received` is a non-terminal outcome and thus can
be sent multiple times (I think). We'd have to make sure we don't paint ourselves
into a corner with using `received` as it also needs to be used for resumable
large message transfers.

### Configuration

Queues will default to the current `consumer_timeout` default. In addition
they should support queue specific overrides via policy as well as consumer
specific overrides (using a new message annotation: `x-opt-consumer-timeout`)

### Behaviour

Messages that time out will be returned to ready status and the consumer in
question will be marked as "timed out" and will not be assigned any further
messages until it settles the stuck message. Messages that are not timed out
will not be returned to the ready status until they also time out.

The consumer can be "revived" (to begin receiving messages again) only _if_ all
timed out messages are settled somehow.

This is the more complicated option but also the one that gives each protocol
more flexibility in how timeouts are handled.



### What will different protocols do

Options in order of most to least subtle.

#### AMQP

    * Spontaneously release the message. (§3.4.4) (if client supports the released outcome)
    * Detach the link with an error (and cancel the consumer with the queue)
    * Terminate Session

#### AMQP legacy

    * Issue a server side basic.cancel (not all client may handle this well)
    * Close channel - (current behaviour)

#### MQTT / STOMP

    * Terminate connection process
