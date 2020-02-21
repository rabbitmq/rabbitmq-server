# Erlang AMQP 1.0 client

This is an [Erlang client for the AMQP 1.0](https://www.amqp.org/resources/specifications) protocol.

It's primary purpose is to be used in RabbitMQ related projects but it is a
generic client that was tested with at least 4 implementations of AMQP 1.0.

If you are looking for an Erlang client for [AMQP 0-9-1](https://www.rabbitmq.com/tutorials/amqp-concepts.html) — a completely different
protocol despite the name — [consider this one](https://github.com/rabbitmq/rabbitmq-erlang-client).

## Project Maturity and Status

This client is used in the cross-protocol version of the RabbitMQ Shovel plugin. It is not 100%
feature complete but moderately mature and was tested against at least three AMQP 1.0 servers:
RabbitMQ, Azure ServiceBus, ActiveMQ.

This client library is not officially supported by VMware at this time.

## Usage

### Connection Settings

The `connection_config` map contains various configuration properties.

```                       
-type address :: inet:socket_address() | inet:hostname().

-type connection_config() ::
    #{container_id => binary(), % mandatory
      %% must provide a list of addresses or a single address
      addresses => [address()],
      address => address(), 
      %% defaults to 5672, mandatory for TLS
      port => inet:port_number(),
      % the dns name of the target host
      % required by some vendors such as Azure ServiceBus
      hostname => binary(),
      tls_opts => {secure_port, [ssl:ssl_option()]}, % optional
      notify => pid(), % Pid to receive protocol notifications. Set to self() if not provided
      max_frame_size => non_neg_integer(), % incoming max frame size
      idle_time_out => non_neg_integer(), % heartbeat
      sasl => none | anon | {plain, User :: binary(), Password :: binary(),
      % set this to a negative value to allow a sender to "overshoot" the flow
      % control by this margin
      transfer_limit_margin => 0 | neg_integer()}
  }.

```

### TLS

TLS is enabled by setting the `tls_opts` connection configuration property.
Currently the only valid value is `{secure_port, [ssl_option]}` where the port
specified only accepts TLS. It is possible that tls negotiation as described
in the amqp 1.0 protocol will be supported in the future. If no value is provided
for `tls_opt` then a plain socket will be used.


### Basic Example

```
%% this will connect to a localhost node
{ok, Hostname} = inet:gethostname(),
User = <<"guest">>,
Password = <<"guest">>,
%% create a configuration map
OpnConf = #{address => Hostname,
            port => Port,
            container_id => <<"test-container">>,
            sasl => {plain, User, Password}},
{ok, Connection} = amqp10_client:open_connection(OpnConf),
{ok, Session} = amqp10_client:begin_session(Connection),
SenderLinkName = <<"test-sender">>,
{ok, Sender} = amqp10_client:attach_sender_link(Session, SenderLinkName, <<"a-queue-maybe">>),

%% wait for credit to be received
receive
    {amqp10_event, {link, Sender, credited}} -> ok
after 2000 ->
      exit(credited_timeout)
end.

%% create a new message using a delivery-tag, body and indicate
%% it's settlement status (true meaning no disposition confirmation
%% will be sent by the receiver).
OutMsg = amqp10_msg:new(<<"my-tag">>, <<"my-body">>, true),
ok = amqp10_client:send_msg(Sender, OutMsg),
ok = amqp10_client:detach_link(Sender),

%% create a receiver link
{ok, Receiver} = amqp10_client:attach_receiver_link(Session, <<"test-receiver">>, <<"a-queue-maybe">>),

%% grant some credit to the remote sender but don't auto-renew it
ok = amqp10_client:flow_link_credit(Receiver, 5, never),

%% wait for a delivery
receive
    {amqp10_msg, Receiver, InMsg} -> ok
after 2000 ->
      exit(delivery_timeout)
end.

ok = amqp10_client:close_connection(Connection),
```


### Events

The `ampq10_client` API is mostly asynchronous with respect to the AMQP 1.0
protocol. Functions such as `amqp10_client:open_connection` typically return
after the `Open` frame has been successfully written to the socket rather than
waiting until the remote end returns with their `Open` frame. The client will
notify the caller of various internal/async events using `amqp10_event`
messages. In the example above when the remote replies with their `Open` frame
a message is sent of the following forma:

```
{amqp10_event, {connection, ConnectionPid, opened}}
```

When the connection is closed an event is issued as such:

```
{amqp10_event, {connection, ConnectionPid, {closed, Why}}}
```

`Why` could be `normal` or contain a description of an error that occured
and resulted in the closure of the connection.

Likewise sessions and links have similar events using a similar format.

```
%% success events
{amqp10_event, {connection, ConnectionPid, opened}}
{amqp10_event, {session, SessionPid, begun}}
{amqp10_event, {link, LinkRef, attached}}
```

```
%% error events
{amqp10_event, {connection, ConnectionPid, {closed, Why}}}
{amqp10_event, {session, SessionPid, {ended, Why}}}
{amqp10_event, {link, LinkRef, {detached, Why}}}
```

In addition the client may notify the initiator of certain protocol
events such as a receiver running out of credit or credit being available
to a sender.

```
%% no more credit available to sender
{amqp10_event, {link, Sender, credit_exhausted}}
%% sender credit received
{amqp10_event, {link, Sender, credited}}
```

Other events may be declared as necessary, Hence it makes sense for a user
of the client to handle all `{amqp10_event, _}` events to ensure unexpected
messages aren't kept around in the mailbox.
