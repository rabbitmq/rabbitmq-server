# AMQP 1.0 support for RabbitMQ

This plugin adds AMQP 1.0 support to RabbitMQ.  It can be swapped in
for RabbitMQ's standard TCP socket server; in other words, you can
configure it to listen on port 5672 and continue to use 0-8 or 0-9-1
clients as before, as well as 1.0 clients.

# Status

This is a prototype.  You can send and receive messages between 0-9-1
or 0-8 clients and 1.0 clients (all those 1.0 clients that there
are), with broadly the same semantics as you would get with 0-9-1.

# Building and configuring

The plugin uses the standard RabbitMQ plugin build environment; see <http://www.rabbitmq.com/plugin-development.html>.

Currently you need bug23749 of rabbitmq-server and rabbitmq-codegen.

By default, it will listen on port 5673.  However, you may wish to
listen on the standard AMQP port, 5672.  To do this, give RabbitMQ a
configuration that looks like this:

    [{rabbit, [{tcp_listeners, []}]},
     {rabbitmq_amqp1_0, [{tcp_listeners, [{"0.0.0.0", 5672}]}]}].

It will then serve AMQP 0-8, 0-9-1, and 1.0 on the socket.

AMQP 1.0 conceptually allows connections that are not authenticated
with SASL (i.e. where no username and password is supplied). By
default these will connect as the "guest" user. To change this, set
'default_user' to a string with the name of the user to use, or the
atom 'none' to prevent unauthenticated connections.

# Interoperability with AMQP 0-9-1

## Message payloads

This implementation as a plugin aims for useful interoperability with
AMQP 0-9-1 clients. AMQP 1.0 messages can be far more structured than
AMQP 0-9-1 messages, which simply have a payload of bytes.

The way we deal with this is that an AMQP 1.0 message with a single
data section will be transcoded to an AMQP 0-9-1 message with just the
bytes from that section, and vice versa. An AMQP 1.0 with any other
payload will keep exactly that payload (i.e., encoded AMQP 1.0
sections, concatenated), and for AMQP 0-9-1 clients the `type` field
of the `basic.properties` will contain the value `"amqp-1.0"`.

Thus, AMQP 0-9-1 clients may receive messages that they cannot
understand (if they don't have an AMQP 1.0 codec handy, anyway);
however, these will at least be labelled. AMQP 1.0 clients shall
receive exactly what they expect.

## Message properties, annotations, headers, etc.

Currently we expect no message- or delivery-annotations, and discard
any footer. Otherwise, the various headers and properties map as
follows:

    AMQP 1.0                                 AMQP 0-9-1
    Header                                   Properties
      durable              <--------------->   delivery-mode   [1]
      priority             <--------------->   priority
      ttl                                                      [2]
      first-acquirer                                           [3]
      delivery-count                                           [4]
    Properties
      message-id           <--------------->   message-id      [5]
      user-id              <--------------->   user-id
      to                                                       [6]
      subject                                                  [6]
      reply-to             <--------------->   reply-to        [6]
      correlation-id       <--------------->   correlation-id
      content-type         <--------------->   content-type
      content-encoding     <--------------->   content-encoding
      absolute-expiry-time <--------------->   expiration      [7]
      creation-time        <--------------->   timestamp
    Application headers    <-------/------->   headers         [8]

[1] `durable` is `true` if and only if `delivery-mode` is `2`.

[2] `ttl` has no corresponding field in AMQP 0-9-1, and is not supported
per message in RabbitMQ in any case.

[3] `first-acquirer` is true if and only if the `basic.deliver` field
`redelivered` is false.

[4] `delivery-count` is left null.

[5] AMQP 0-9-1 expects this to be a shortstr.

[6] See Routing and Addressing below.

[7] `expiration` is a shortstr; since many clients will expect this to
be an encoded string, we translate an `absolute-expiry-time` to the
string representation of its integer value. An expiration that is not
a string representation of an integer is discarded (going via a string
also avoids some ambiguity)

[8] The application headers section and the `basic.properties` field
`headers` are natural analogues. However, rather than try to transcode
an AMQP 1.0 map to an AMQP 0-9-1 field-table, currently we discard
application headers (of AMQP 1.0 messages) and headers (of AMQP 0-9-1
messages sent through to AMQP 1.0). In other words, the (AMQP 1.0)
application headers section is only available to AMQP 1.0 clients, and
the (AMQP 0-9-1) headers field is only available to AMQP 0-9-1
clients.

Note that properties (in both AMQP 1.0 and AMQP 0-9-1) and application
properties (in AMQP 1.0) are immutable; however, this can only apply
when the sending and receiving clients are using the same protocol.

## Routing and Addressing

In AMQP 1.0 source and destination addresses are opaque values, and
each message may have a `subject` field value. In AMQP
0-9-1 each message is published to an exchange and accompanied by a
routing key.

For interoperability with AMQP 0-9-1, we adopt the following
addressing scheme:

    Link target    Subject    AMQP 0-9-1 equivalent

    /exchange/X    RK         Publish to exchange X with routing key RK
    /queue         Q          Publish to default exchange with routing key Q
    /queue/Q       ignore[9]  Publish to default exchange with routing key Q


    Link source               AMQP 0-9-1 equivalent

    /queue/Q                  Consume from queue Q
    /exchange/X               Declare a private queue, bind it to
                              exchange X, and consume from it.

[9] Properties are immutable, so a 1.0 client receiving this message
will get exactly the value given; however it is not used for routing,
and a 0-9-1 client will see the queue name (Q) as the routing key.

Note that addresses used in `reply-to` are assumed to refer to
queues. As such, they are translated between AMQP 0-9-1 and AMQP 1.0
thus:

    AMQP 1.0             AMQP 0-9-1
    /queue/ReplyTo <---> ReplyTo

# Limitations and unsupported features

At the minute, the RabbitMQ AMQP 1.0 adapter does not support:

 - "Exactly once" delivery [10]
 - Link recovery [10]
 - Full message fragmentation [11]
 - Resuming messages
 - "Modified" outcome
 - Filters [12]
 - Transactions
 - Mixed settlement mode
 - Source/target expiry-policy other than link-detach and timeout
   other than 0
 - Max message size for links

[10] We do not deduplicate as a target, though we may resend as a
source (messages that have no settled outcome when an outgoing link is
detached will be requeued).

[11] We do fragment messages over multiple frames; however, if this
would overflow the session window we may discard or requeue messages.

[12] In principle, filters for consuming from an exchange could
translate to AMQP 0-9-1 bindings. This is not implemented, so
effectively only consuming from fanout exchanges and queues is useful
currently.
