# AMQP 1.0 support for RabbitMQ

This plugin adds AMQP 1.0 support to RabbitMQ.

Despite the name,
AMQP 0-9-1 and 1.0 are very much different protocols and thus
1.0 is treated as a separate protocol supported by RabbitMQ,
not a revision of the original protocol that will eventually supersede it.

This plugin is several years old and is moderately mature. It may have certain
limitations with its current architecture but most major AMQP 1.0 features should be in place.

This plugin supports 0-9-1 and 1.0 client interoperability with certain limitations.

# Configuration

This plugin ships with modern versions of RabbitMQ.

It will listen on the standard AMQP port, 5672. To reconfigure this,
do so [as you would for 0-9-1](http://www.rabbitmq.com/configure.html). Clients connecting with 0-9-1
will continue to work on the same port.

The following two configuration options (which are specific to the AMQP 1.0 adapter)
are accepted in the `rabbitmq_amqp1_0` section of the configuration file.

AMQP 1.0 conceptually allows connections that are not authenticated
with SASL (i.e. where no username and password is supplied). By
default these will connect as the "guest" user. To change this, set
`default_user` to a string with the name of the user to use, or the
atom `none` to prevent unauthenticated connections.

    {default_user, "guest"}

The default virtual host can be specified using the `default_vhost` setting.
See the "Virtual Hosts" section below for a description.

    {default_vhost, <<"/">>}

The `protocol_strict_mode` setting controls how strictly peers must conform
to the specification. The default is not to enforce strictness, which allows
non-fatal byte-counts in frames and inaccuracies in flow-control from peers.

    {protocol_strict_mode, false}


Configuration example using [sysctl config format](https://next.rabbitmq.com/configure.html#config-file-formats)
(currently only available in RabbitMQ master):

    amqp1_0.default_user  = guest
    amqp1_0.default_vhost = /
    amqp1_0.protocol_strict_mode = false


## Clients we have tested

The current field of AMQP 1.0 clients is somewhat limited. Therefore
we have not achieved as much interoperability as we might like.

We have tested against:

 * SwiftMQ Java client [1]
   We have done most of our testing against this client and things seem
   to work.

 * QPid / Proton C client [2]
   We have successfully tested against the "proton" command line tool
   this client ships with.

 * QPid / Proton Java client [2]
   We have not been able to get this client to get as far as opening a
   network connection (tested against 0.2 and 0.4).

 * Windows Azure Service Bus [3]
   It seems that the URI scheme used by this client assumes that it is
   connecting to Azure; it does not seem to be possible to get it to
   connect to another server.

[1] http://www.swiftmq.com/products/router/swiftlets/sys_amqp/client/index.html

[2] http://qpid.apache.org/proton/

[3] http://www.windowsazure.com/en-us/develop/net/how-to-guides/service-bus-amqp/

As new clients appear we will of course work on interoperability with them.

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

The headers and properties map as follows:

    AMQP 1.0                                 AMQP 0-9-1
    Header                                   Properties
      durable              <--------------->   delivery-mode   [1]
      priority             <--------------->   priority
      ttl                  <--------------->   expiration      [2]
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
      absolute-expiry-time                                     [7]
      creation-time        <--------------->   timestamp
    Application headers    <-------/------->   headers         [8]

[1] `durable` is `true` if and only if `delivery-mode` is `2`.

[2] `expiration` is a shortstr; since RabbitMQ will expect this to be
an encoded string, we translate a `ttl` to the string representation
of its integer value.

[3] `first-acquirer` is true if and only if the `basic.deliver` field
`redelivered` is false.

[4] `delivery-count` is left null.

[5] AMQP 0-9-1 expects this to be a shortstr.

[6] See Routing and Addressing below.

[7] `absolute-expiry-time` has no corresponding field in AMQP 0-9-1,
and is not supported in RabbitMQ in any case.

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
each message may have a `subject` field value.

For targets, addresses are:

    = "/exchange/"  X "/" RK  Publish to exchange X with routing key RK
    | "/exchange/"  X         Publish to exchange X with message subject as routing key
    | "/topic/"     RK        Publish to amq.topic with routing key RK
    | "/amq/queue/" Q         Publish to default exchange with routing key Q
    | "/queue/"     Q         Publish to default exchange with routing key Q
    | Q (no leading slash)    Publish to default exchange with routing key Q
    | "/queue"                Publish to default exchange with message subj as routing key

For sources, addresses are:

    = "/exchange/"  X "/" RK  Consume from temp queue bound to X with routing key RK
    | "/topic/"     RK        Consume from temp queue bound to amq.topic with routing key RK
    | "/amq/queue/" Q         Consume from Q
    | "/queue/"     Q         Consume from Q
    | Q (no leading slash)    Consume from Q

The intent is that the source and destination address formats should be
mostly the same as those supported by the STOMP plugin, to the extent
permitted by AMQP 1.0 semantics.

## Virtual Hosts

AMQP 1.0 has no equivalent of AMQP 0-9-1 virtual hosts. A virtual host
on the broker may be addressed when opening an AMQP 1.0 connection by setting
the `hostname` field, prefixing with "vhost:". Setting the `hostname` field
to "vhost:/" addresses the default virtual host. If the `hostname` field
does not start with "vhost:" then the `default_vhost` configuration
setting will be consulted.

# Limitations and unsupported features

At the minute, the RabbitMQ AMQP 1.0 adapter does not support:

 - "Exactly once" delivery [9]
 - Link recovery [9]
 - Full message fragmentation [10]
 - Resuming messages
 - "Modified" outcome
 - Filters [11]
 - Transactions
 - Source/target expiry-policy other than link-detach and timeout
   other than 0
 - Max message size for links
 - Aborted transfers
 - TLS negotiation via the AMQP2100 handshake (although SSL is supported)

[9] We do not deduplicate as a target, though we may resend as a
source (messages that have no settled outcome when an outgoing link is
detached will be requeued).

[10] We do fragment messages over multiple frames; however, if this
would overflow the session window we may discard or requeue messages.

[11] In principle, filters for consuming from an exchange could
translate to AMQP 0-9-1 bindings. This is not implemented, so
effectively only consuming from fanout exchanges and queues is useful
currently.
