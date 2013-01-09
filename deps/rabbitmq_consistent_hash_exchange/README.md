# RabbitMQ Consistent Hash Exchange Type

This plugin adds a consistent-hash exchange type to RabbitMQ.

In various scenarios, you may wish to ensure that messages sent to an
exchange are consistently and equally distributed across a number of
different queues based on the routing key of the message (or a
nominated header, see "Routing on a header" below). You could arrange
for this to occur yourself by using a direct or topic exchange,
binding queues to that exchange and then publishing messages to that
exchange that match the various binding keys.

However, arranging things this way can be problematic:

1. It is difficult to ensure that all queues bound to the exchange
will receive a (roughly) equal number of messages without baking in to
the publishers quite a lot of knowledge about the number of queues and
their bindings.

2. If the number of queues changes, it is not easy to ensure that the
new topology still distributes messages between the different queues
evenly.

[Consistent Hashing](http://en.wikipedia.org/wiki/Consistent_hashing)
is a hashing technique whereby each bucket appears at multiple points
throughout the hash space, and the bucket selected is the nearest
higher (or lower, it doesn't matter, provided it's consistent) bucket
to the computed hash (and the hash space wraps around). The effect of
this is that when a new bucket is added or an existing bucket removed,
only a very few hashes change which bucket they are routed to.

In the case of Consistent Hashing as an exchange type, the hash is
calculated from the hash of the routing key of each message
received. Thus messages that have the same routing key will have the
same hash computed, and thus will be routed to the same queue,
assuming no bindings have changed.

When you bind a queue to a consistent-hash exchange, the binding key
is a number-as-a-string which indicates the number of points in the
hash space at which you wish the queue to appear. The actual points
are generated randomly.

So, if you wish for queue A to receive twice as many messages as queue
B, then you bind the queue A with a binding key of twice the number
(as a string -- binding keys are always strings) of the binding key of
the binding to queue B.

Each message gets delivered to at most one queue. Normally, each
message gets delivered to exactly one queue, but there is a race
between the determination of which queue to send a message to, and the
deletion/death of that queue that does permit the possibility of the
message being sent to a queue which then disappears before the message
is processed. Hence in general, at most one queue.

The exchange type is "x-consistent-hash".

Here is an example using the Erlang client:

    -include_lib("amqp_client/include/amqp_client.hrl").
    
    test() ->
        {ok, Conn} = amqp_connection:start(#amqp_params_network{}),
        {ok, Chan} = amqp_connection:open_channel(Conn),
        Queues = [<<"q0">>, <<"q1">>, <<"q2">>, <<"q3">>],
        amqp_channel:call(Chan,
                          #'exchange.declare' {
                            exchange = <<"e">>, type = <<"x-consistent-hash">>
                          }),
        [amqp_channel:call(Chan, #'queue.declare' { queue = Q }) || Q <- Queues],
        [amqp_channel:call(Chan, #'queue.bind' { queue = Q,
                                                 exchange = <<"e">>,
                                                 routing_key = <<"10">> })
         || Q <- [<<"q0">>, <<"q1">>]],
        [amqp_channel:call(Chan, #'queue.bind' { queue = Q,
                                                 exchange = <<"e">>,
                                                 routing_key = <<"20">> })
         || Q <- [<<"q2">>, <<"q3">>]],
        Msg = #amqp_msg { props = #'P_basic'{}, payload = <<>> },
        [amqp_channel:call(Chan,
                           #'basic.publish'{
                             exchange = <<"e">>,
                             routing_key = list_to_binary(
                                             integer_to_list(
                                               random:uniform(1000000)))
                           }, Msg) || _ <- lists:seq(1,100000)],
        amqp_connection:close(Conn),
        ok.

As you can see, the queues `q0` and `q1` get bound each with 10 points
in the hash space to the exchange `e` which means they'll each get
roughly the same number of messages. The queues `q2` and `q3` however,
get 20 points each which means they'll each get roughly the same
number of messages too, but that will be approximately twice as many
as `q0` and `q1`. We then publish 100,000 messages to our exchange
with random routing keys. After this has completed, running
`rabbitmqctl list_queues` should show that the messages have been
distributed approximately as desired.

Note the `routing_key`s in the bindings are numbers-as-strings. This
is because AMQP specifies the routing_key must be a string.

The more points in the hash space each binding has, the closer the
actual distribution will be to the desired distribution (as indicated
by the ratio of points by binding). However, large numbers of points
(many thousands) will substantially decrease performance of the
exchange type.

Equally, it is important to ensure that the messages being published
to the exchange have a range of different `routing_key`s: if a very
small set of routing keys are being used then there's a possibility of
messages not being evenly distributed between the various queues. If
the routing key is a pseudo-random session ID or such, then good
results should follow.

## Routing on a header

Under most circumstances the routing key is a good choice for something to
hash. However, in some cases you need to use the routing key for some other
purpose (for example with more complex routing involving exchange to
exchange bindings). In this case you can configure the consistent hash
exchange to route based on a named header instead. To do this, declare the
exchange with a string argument called "hash-header" naming the header to
be used. For example using the Erlang client as above:

    amqp_channel:call(
      Chan, #'exchange.declare' {
              exchange  = <<"e">>,
              type      = <<"x-consistent-hash">>,
              arguments = [{<<"hash-header">>, longstr, <<"hash-me">>}]
            }).

If you specify "hash-header" and then publish messages without the named
header, they will all get routed to the same (arbitrarily-chosen) queue.

Any comments or feedback welcome, to the
[rabbitmq-discuss mailing list](https://lists.rabbitmq.com/cgi-bin/mailman/listinfo/rabbitmq-discuss)
or info@rabbitmq.com.
